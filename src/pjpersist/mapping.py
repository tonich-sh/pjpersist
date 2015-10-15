##############################################################################
#
# Copyright (c) 2011 Zope Foundation and Contributors.
# Copyright (c) 2014 Shoobx Inc.
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
import json

from UserDict import DictMixin, IterableUserDict

from sqlbuilder.smartsql import Q, T, compile, Expr, NamedCondition, PLACEHOLDER, Name

from persistent.mapping import PersistentMapping

from pjpersist import serialize, interfaces, datamanager


class JsonbSuperset(NamedCondition):
    _sql = '@>'


class JsonbContainsAll(NamedCondition):
    _sql = '?&'


def jsonb_superset(inst, other):
    return JsonbSuperset(inst, other)


def jsonb_contains_all(inst, other):
    return JsonbContainsAll(inst, other)

setattr(Expr, 'jsonb_superset', jsonb_superset)
setattr(Expr, 'jsonb_contains_all', jsonb_contains_all)


class JsonArray(object):

    __slots__ = ('_value', )

    def __init__(self, value):
        self._value = value


@compile.when(JsonArray)
def compile_json_array(compile, expr, state):
    state.sql.append("array[")
    for item in expr._value:
        state.sql.append(PLACEHOLDER)
        state.params.append(item)
    state.sql.append("]")


@compile.when(Name)
def compile_name(compile, expr, state):
    # state.sql.append('"')
    state.sql.append(expr._name)
    # state.sql.append('"')


class PJTableMapping(DictMixin, object):
    __pj_table__ = None
    __pj_mapping_key__ = 'key'

    def __init__(self, jar):
        self._pj_jar = jar

    def __pj_filter__(self):
        return Expr('true')

    def get_tables_objects(self):
        if not hasattr(self, '_p_meta'):
            setattr(self, '_p_meta', {})
        if 'mt' not in self._p_meta or 'st' not in self._p_meta:
            self._p_meta['mt'] = getattr(T, self.__pj_table__)
            self._p_meta['st'] = getattr(T, '%s_state' % self.__pj_table__)
        return self._p_meta['mt'], self._p_meta['st']

    def query(self):
        if not hasattr(self, '_p_meta'):
            setattr(self, '_p_meta', {})
        mt, st = self.get_tables_objects()
        if 'q' not in self._p_meta:
            self._p_meta['q'] = Q().tables(mt & st).on((mt.id == st.pid)).where(mt.tid == st.tid)
        return self._p_meta['q'].clone()

    def __getitem__(self, key):
        q = self.query()
        q = q.where(self.__pj_filter__())
        mt, st = self.get_tables_objects()
        q = q.where(st.data.jsonb_superset(datamanager.Json({self.__pj_mapping_key__: key}))).fields(mt.id)
        with self._pj_jar.getCursor() as cur:
            cur.execute(
                *compile(q)
            )
            if not cur.rowcount:
                raise KeyError(key)
            _id = cur.fetchone()['id']
            dbref = serialize.DBRef(self.__pj_table__, _id, self._pj_jar.database)
        return self._pj_jar.load(dbref)

    def __setitem__(self, key, value):
        # Even though setting the attribute should register the object with
        # the data manager, the value might not be in the DB at all at this
        # point, so registering it manually ensures that new objects get added.
        self._pj_jar.register(value)
        setattr(value, interfaces.TABLE_ATTR_NAME, self.__pj_table__)
        setattr(value, self.__pj_mapping_key__, key)

    def __delitem__(self, key):
        # Deleting the object from the database is not our job. We simply
        # remove it from the dictionary.
        value = self[key]
        setattr(value, self.__pj_mapping_key__, None)

    def keys(self):
        q = self.query()
        q = q.where(self.__pj_filter__())
        _, st = self.get_tables_objects()
        q = q.where(~(st.data.jsonb_superset(datamanager.Json({self.__pj_mapping_key__: None})) | ~st.data.jsonb_contains_all(JsonArray([self.__pj_mapping_key__]))))
        q = q.fields(st.data)
        with self._pj_jar.getCursor() as cur:
            cur.execute(
                *compile(q)
            )
            return [
                res['data'][self.__pj_mapping_key__]
                for res in cur.fetchall()]


# TODO: tests for PJMapping
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
        return 'true'

    def by_raw_id(self, _id):
        return self._p_jar.load(serialize.DBRef(self.table, _id, self._p_jar.database))

    def query(self):
        return None

    def __getitem__(self, key):
        if key not in self.data:
            _filter = self.__pj_filter__()
            if not isinstance(key, basestring):
                key_string = key.__str__()
            else:
                key_string = key
            _filter += ''' AND s.data @> '%s' ''' % json.dumps({self.mapping_key: key_string})
            if self._p_jar is None:
                raise KeyError(key)
            obj = None
            with self._p_jar.getCursor() as cur:
                cur.execute(
                    '''
SELECT
    m.id
FROM
    %s m
    JOIN %s_state s ON m.id = s.pid and m.tid = s.tid
WHERE
    %s''' % (self.table, self.table, _filter)
                )
                if not cur.rowcount:
                    raise KeyError(key)
                id = cur.fetchone()['id']
                dbref = serialize.DBRef(self.table, id, self._p_jar.database)
                obj = self._p_jar.load(dbref)
            assert obj is not None
        else:
            obj = self.data[key]
        setattr(obj, interfaces.TABLE_ATTR_NAME, self.table)
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
        _filter = self.__pj_filter__()
        if not isinstance(item, basestring):
            key_string = item.__str__()
        else:
            key_string = item
        _filter += ''' AND data @> '%s' ''' % json.dumps({self.mapping_key: key_string})
        with self._p_jar.getCursor() as cur:
            cur.execute(
                '''
SELECT
    m.id
FROM
    %s m
    JOIN %s_state s ON m.id = s.pid and m.tid = s.tid
WHERE
    %s''' % (self.table, self.table, _filter)
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

