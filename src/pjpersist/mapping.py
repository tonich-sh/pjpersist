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

from persistent.mapping import PersistentMapping

from pjpersist import serialize, interfaces


class PJTableMapping(DictMixin, object):
    __pj_table__ = None
    __pj_mapping_key__ = 'key'

    def __init__(self, jar):
        self._pj_jar = jar

    def __pj_filter__(self):
        return 'true'

    def __getitem__(self, key):
        filter = self.__pj_filter__()
        filter += ''' AND data @> '%s' ''' % json.dumps({self.__pj_mapping_key__: key})
        with self._pj_jar.getCursor() as cur:
            cur.execute(
                '''
SELECT
    m.id
FROM
    %s m
    JOIN %s_state s ON m.id = s.pid and m.tid = s.tid
WHERE
    %s''' % (self.__pj_table__, self.__pj_table__, filter)
            )
            if not cur.rowcount:
                raise KeyError(key)
            id = cur.fetchone()['id']
            dbref = serialize.DBRef(self.__pj_table__, id, self._pj_jar.database)
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
        filter = self.__pj_filter__()
        filter += """ AND NOT (data @> '{"%s": null}' OR""" % \
            self.__pj_mapping_key__
        filter += "      NOT data ?& array['%s'] )" % self.__pj_mapping_key__
        with self._pj_jar.getCursor() as cur:
            cur.execute(
                '''
SELECT
    m.*, s.data
FROM
    %s m
    JOIN %s_state s ON m.id = s.pid and m.tid = s.tid
WHERE
    %s''' % (self.__pj_table__, self.__pj_table__, filter)
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

    def __getitem__(self, key):
        if key not in self.data:
            _filter = self.__pj_filter__()
            if not isinstance(key, basestring):
                key_string = key.__str__()
            else:
                key_string = key
            _filter += ''' AND data @> '%s' ''' % json.dumps({self.mapping_key: key_string})
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

