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
from . smartsql import compile, PJResult, T, Q, Expr, JsonArray, PJMappedVirtualTable


class PMetaData(object):
    __slots__ = ('_mapping', '_mt', '_st', '_vt', '_q')

    def __init__(self, mapping):
        self._mapping = mapping
        self._mt = None  # main table
        self._st = None  # state table
        self._vt = None  # virtual table (main table joined with state table)
        self._q = None  # SmartSql's query object

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
    def vt(self):
        if self._vt is None:
            self._vt = PJMappedVirtualTable(self._mapping)
        return self._vt

    @property
    def q(self):
        if self._q is None:
            self._q = Q(result=PJResult(self._mapping, compile=compile)).tables(self.vt)
        return self._q


class PJTableMapping(DictMixin, object):
    table = None
    mapping_key = 'key'

    def __init__(self, jar):
        self._p_jar = jar
        self._p_meta = PMetaData(self)

    def __pj_filter__(self):
        return Expr('true')

    def get_table_object(self, ttype='vt'):
        return getattr(self._p_meta, ttype, self._p_meta.vt)

    def get_fields(self):
        mt = self.get_table_object(ttype='mt')
        st = self.get_table_object(ttype='st')
        return mt.id, mt.tid, mt.package, mt.class_name, st.data

    def query(self):
        return self._p_meta.q.clone()

    def __getitem__(self, key):
        q = self.query()
        q = q.where(self.__pj_filter__())
        vt = self.get_table_object()
        q = q.where(vt.f.jsonb_superset(datamanager.Json({self.mapping_key: key}))).fields('*')
        objects = q.select().__iter__()
        try:
            obj = next(objects)
        except StopIteration:
            raise KeyError(key)
        if not obj:
            raise KeyError(key)
        return obj

    def __setitem__(self, key, value):
        # Even though setting the attribute should register the object with
        # the data manager, the value might not be in the DB at all at this
        # point, so registering it manually ensures that new objects get added.
        self._p_jar.register(value)
        setattr(value, interfaces.ATTR_NAME_TABLE, self.table)
        setattr(value, self.mapping_key, key)

    def __delitem__(self, key):
        # Deleting the object from the database is not our job. We simply
        # remove it from the dictionary.
        value = self[key]
        setattr(value, self.mapping_key, None)

    def keys(self):
        q = self.query()
        q = q.where(self.__pj_filter__())
        st = self.get_table_object(ttype='st')
        q = q.where(~(st.data.jsonb_superset(datamanager.Json({self.mapping_key: None})) | ~st.data.jsonb_contains_all(JsonArray([self.mapping_key]))))
        q = q.fields(st.data)
        with self._p_jar.getCursor() as cur:
            cur.execute(
                *compile(q)
            )
            return [
                res['data'][self.mapping_key]
                for res in cur]


class PJMappingKeysProxy(list):

    def __init__(self, iterable):
        self._iterable = iterable
        super(PJMappingKeysProxy, self).__init__()

    def __iter__(self):
        mapping = getattr(self._iterable, '_mapping', None)
        if mapping is None:
            return
        for i in self._iterable:
            try:
                yield i['data'][mapping.mapping_key]
            except:
                return


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

    def get_table_object(self, ttype='vt'):
        if not hasattr(self, '_p_meta'):
            setattr(self, '_p_meta', PMetaData(self))
        return getattr(self._p_meta, ttype, self._p_meta.vt)

    def get_fields(self):
        mt = self.get_table_object(ttype='mt')
        st = self.get_table_object(ttype='st')
        return mt.id, mt.tid, mt.package, mt.class_name, st.data

    def query(self):
        if not hasattr(self, '_p_meta'):
            setattr(self, '_p_meta', PMetaData(self))
        return self._p_meta.q.clone()

    def iterkeys(self):
        if self._p_jar is not None:
            q = self.query()
            q = q.where(self.__pj_filter__())
            q = q.fields('data')
            for row in q.select():
                    yield row['data'][self.mapping_key]

    def keys(self):
        if self._p_jar is not None:
            q = self.query()
            q = q.where(self.__pj_filter__())
            q = q.fields('data')
            return PJMappingKeysProxy(q.select())
        return list()

    def itervalues(self):
        if self._p_jar is not None:
            q = self.query()
            q = q.where(self.__pj_filter__())
            q = q.fields('*')
            for obj in q.select():
                yield obj

    def values(self):
        if self._p_jar is not None:
            q = self.query()
            q = q.where(self.__pj_filter__())
            q = q.fields('*')
            return q.select()
        return list()

    def __getitem__(self, key):
        if key not in self.data and self._p_jar is not None:
            q = self.query()
            q = q.where(self.__pj_filter__())
            vt = self.get_table_object()
            q = q.where(vt.f.jsonb_superset(datamanager.Json({self.mapping_key: key}))).fields('*')
            objects = q.select().__iter__()
            try:
                obj = next(objects)
            except StopIteration:
                raise KeyError(key)
            if not obj:
                raise KeyError(key)
        else:
            obj = self.data[key]
        return obj

    def __setitem__(self, key, value):
        super(PJMapping, self).__setitem__(key, value)
        serialize.prepare_class(value.__class__)
        setattr(value, interfaces.ATTR_NAME_TABLE, self.table)
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

        if self._p_jar is None:
            return False

        q = self.query()
        q = q.where(self.__pj_filter__())
        st = self.get_table_object(ttype='st')
        q = q.where(st.data.jsonb_superset(datamanager.Json({self.mapping_key: item}))).fields(st.sid)

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
