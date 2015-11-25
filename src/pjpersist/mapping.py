##############################################################################
#
# Copyright (c) 2011 Zope Foundation and Contributors.
# Copyright (c) 2014 Shoobx Inc.
# Copyright (c) 2015 Schur Anton
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE.
#
##############################################################################
"""PostGreSQL/JSONB Mapping Implementations"""
from __future__ import absolute_import

from UserDict import DictMixin, IterableUserDict
from persistent.mapping import PersistentMapping

from . import serialize, interfaces, datamanager
from . smartsql import compile, PJResult, T, Q, Expr, JsonArray


class PMetaData(object):
    __slots__ = ('_mapping', '_mt', '_st', '_q')

    def __init__(self, mapping):
        self._mapping = mapping
        self._mt = None
        self._st = None
        self._q = None

    @property
    def mt(self):
        if self._mt is None:
            self._mt = getattr(T, self._mapping.table)
        return self._mt

    @property
    def st(self):
        if self._st is None:
            self._st = getattr(T, '%s_state' % self._mapping.table)
        return self._st

    @property
    def q(self):
        mt = self.mt
        st = self.st
        if self._q is None:
            self._q = Q(result=PJResult(self._mapping, compile=compile)).tables(mt & st).on((mt.id == st.pid)).where(mt.tid == st.tid)
        return self._q


class PJTableMapping(DictMixin, object):
    table = None
    mapping_key = 'key'

    def __init__(self, jar):
        self._p_jar = jar
        self._p_meta = PMetaData(self)

    def __pj_filter__(self):
        return Expr('true')

    def get_tables_objects(self):
        return self._p_meta.mt, self._p_meta.st

    def get_fields(self):
        mt, st = self.get_tables_objects()
        return mt.id, mt.package, mt.class_name, st.data

    def query(self):
        return self._p_meta.q.clone()

    def __getitem__(self, key):
        q = self.query()
        q = q.where(self.__pj_filter__())
        mt, st = self.get_tables_objects()
        q = q.where(st.data.jsonb_superset(datamanager.Json({self.mapping_key: key}))).fields(*list(self.get_fields()))
        objects = q.result(q).__iter__()
        obj = objects.next()
        if not obj:
            raise KeyError(key)
        return obj

    def __setitem__(self, key, value):
        # Even though setting the attribute should register the object with
        # the data manager, the value might not be in the DB at all at this
        # point, so registering it manually ensures that new objects get added.
        self._p_jar.register(value)
        setattr(value, interfaces.TABLE_ATTR_NAME, self.table)
        setattr(value, self.mapping_key, key)

    def __delitem__(self, key):
        # Deleting the object from the database is not our job. We simply
        # remove it from the dictionary.
        value = self[key]
        setattr(value, self.mapping_key, None)

    def keys(self):
        q = self.query()
        q = q.where(self.__pj_filter__())
        _, st = self.get_tables_objects()
        q = q.where(~(st.data.jsonb_superset(datamanager.Json({self.mapping_key: None})) | ~st.data.jsonb_contains_all(JsonArray([self.mapping_key]))))
        q = q.fields(st.data)
        with self._p_jar.getCursor() as cur:
            cur.execute(
                *compile(q)
            )
            return [
                res['data'][self.mapping_key]
                for res in cur]


# TODO: deleting of items from PJMapping
class PJMapping(PersistentMapping):
    """A persistent wrapper for mapping objects.

    This class stores name of table with a mapped
    objects.
    """

    table = None
    mapping_key = 'key'

    __super_delitem = IterableUserDict.__delitem__
    __super_setitem = IterableUserDict.__setitem__
    __super_clear = IterableUserDict.clear
    __super_update = IterableUserDict.update
    __super_setdefault = IterableUserDict.setdefault
    __super_pop = IterableUserDict.pop
    __super_popitem = IterableUserDict.popitem
    __super_has_key = IterableUserDict.has_key

    # TODO: use separate table to store removed objects (? key, tid only ?)
    def __delitem__(self, key):
        # self.__super_delitem(key)
        # self._p_changed = 1
        raise NotImplementedError

    def __pj_filter__(self):
        return Expr('true')

    def by_raw_id(self, _id):
        return self._p_jar.load(serialize.DBRef(self.table, _id, self._p_jar.database))

    def get_tables_objects(self):
        # ? or implement __init__ instead ?
        if not hasattr(self, '_p_meta'):
            setattr(self, '_p_meta', PMetaData(self))
        return self._p_meta.mt, self._p_meta.st

    def get_fields(self):
        mt, st = self.get_tables_objects()
        return mt.id, mt.package, mt.class_name, st.data

    def query(self):
        if not hasattr(self, '_p_meta'):
            setattr(self, '_p_meta', PMetaData(self))
        return self._p_meta.q.clone()

    def keys(self):
        raise NotImplementedError

    def __getitem__(self, key):
        if key not in self.data:
            q = self.query()
            q = q.where(self.__pj_filter__())
            mt, st = self.get_tables_objects()
            q = q.where(st.data.jsonb_superset(datamanager.Json({self.mapping_key: key}))).fields(*list(self.get_fields()))
            objects = q.result(q).__iter__()
            obj = objects.next()
            if not obj:
                raise KeyError(key)
        else:
            obj = self.data[key]
        return obj

    def __setitem__(self, key, value):
        super(PJMapping, self).__setitem__(key, value)
        setattr(value, interfaces.TABLE_ATTR_NAME, self.table)
        setattr(value, self.mapping_key, key)

    def __getstate__(self):
        """
        Register items in jar and do not store the 'data' attribute
        """
        data = getattr(self, 'data', dict())
        for k, v in data.items():
            if v._p_jar is None or v._p_changed:
                self._p_jar.register(v)

        d = super(PJMapping, self).__getstate__()
        if 'data' in d:
            del d['data']
        return d

    def __setstate__(self, state):
        """
        Create data attribute if not exists
        :param state:
        :return:
        """
        if 'data' not in state:
            state['data'] = dict()
        super(PJMapping, self).__setstate__(state)

    def clear(self):
        # self.__super_clear()
        # self._p_changed = 1
        raise NotImplementedError

    def __contains__(self, item):
        k = self.__super_has_key(item)
        if k:
            return k

        q = self.query()
        q = q.where(self.__pj_filter__())
        mt, st = self.get_tables_objects()
        q = q.where(st.data.jsonb_superset(datamanager.Json({self.mapping_key: item}))).fields(mt.id)

        with self._p_jar.getCursor() as cur:
            cur.execute(
                *compile(q)
            )
            if cur.rowcount:
                return True

        return False

    def has_key(self, key):
        return self.__contains__(key)

    def update(self, _dict=None, **kwargs):
        self.__super_update(_dict, **kwargs)
        self._p_changed = 1

    def setdefault(self, key, failobj=None):
        # We could inline all of UserDict's implementation into the
        # method here, but I'd rather not depend at all on the
        # implementation in UserDict (simple as it is).
        if key not in self.data:
            self._p_changed = 1
        return self.__super_setdefault(key, failobj)

    def pop(self, key, *args):
        # self._p_changed = 1
        # return self.__super_pop(key, *args)
        raise NotImplementedError

    def popitem(self):
        # self._p_changed = 1
        # return self.__super_popitem()
        raise NotImplementedError
