##############################################################################
#
# Copyright (c) 2011 Zope Foundation and Contributors.
# Copyright (c) 2014 Shoobx, Inc.
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
"""Object Cache"""
from __future__ import absolute_import
import threading
import zope.interface

from pjpersist import interfaces, serialize

_CACHE = threading.local()
AGGRESSIVE_MODE = False

class NoOpObjectCache(object):
    #zope.interface.implements(interfaces.IObjectCache)

    def __init__(self, datamanager):
        self._datamanager = datamanager

    def reset(self):
        pass

    def get_object(self, dbref):
        return None

    def put_object(self, obj):
        pass

    def purge_object(self, obj_or_dbref):
        pass

    def before_store(self, oid, doc):
        pass


class TransactionalObjectCache(object):
    #zope.interface.implements(interfaces.IObjectCache)

    txn_serial_attr_name = '_pj_txn_serial'

    def __init__(self, datamanager):
        self._datamanager = datamanager
        self.txn_serial = self._get_txn_serial()
        if not hasattr(_CACHE, 'objects'):
            _CACHE.objects = {}

    def _get_txn_serial(self):
        with self._datamanager._conn.cursor() as cur:
            cur.execute('''
              DO $$ BEGIN
                IF NOT EXISTS (
                  SELECT 1 FROM pg_class where relname = 'seq_pj_txb_serial')
                THEN
                  CREATE SEQUENCE seq_pj_txb_serial START WITH 0 MINVALUE 0 NO MAXVALUE;
                END IF;
              END $$;
              SELECT nextval('seq_pj_txb_serial')
              ''')
            return cur.fetchone()[0]

    def reset(self):
        self._cache.__init__()

    def get_object(self, dbref):
        # If the object is not in the cache, then we are done.
        if dbref not in _CACHE.objects:
            return None
        doc = self._datamanager._latest_states.get(dbref)
        # If there is no doc, then we cannot get the latest txn_serial for the
        # object.
        if doc is None:
            if not AGGRESSIVE_MODE:
                return None
            # In aggressive mode, we actually want to go out and ask the
            # datamanager to fetch the object.
            doc = self._jar._get_doc_by_dbref(obj._p_oid)
            self._datamanager._latest_states[dbref] = doc
        # Look up the cache value
        cache_serial, obj = _CACHE.objects[dbref]
        current_serial = doc.get(txn_serial_attr_name)
        # Make sure the document contains a transaction serial
        if current_serial is None:
            return None
        # Make sure the database does not have a newer version.
        if current_serial > cache_serial:
            self.purge_object(dbref)
            return None
        # The object in the cache is valid, so we can return it after
        # assigning the latest data manager to it.
        obj._p_jar = self._datamanager
        return obj

    def put_object(self, obj):
        if not hasattr(obj, '_p_oid'):
            raise AttributeError('Non-persistent object')
        if obj._p_oid is None:
            raise ValueError('Object not yet added to database')
        _CACHE.objects[obj._p_oid] = (self.txn_serial, obj)

    def purge_object(self, obj_or_dbref):
        dbref = obj_or_dbref
        if not isinstance(dbref, serialize.DBRef):
            dbref = dbref._p_oid
        del _CACHE.objects[dbref]

    def before_store(self, oid, doc):
        doc[self.txn_serial_attr_name] = self.txn_serial
