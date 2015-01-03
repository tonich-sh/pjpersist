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
import logging
import threading
import zope.interface

import pjpersist.datamanager
from pjpersist import interfaces, serialize

LOG = logging.getLogger(__name__)

# XXX: for now go with a thread level cache
#      use later a cache pool as ZODB does with Connectinons,
#      assigned a cache on transaction start  based evtl. on cache size
#      preferring a big cache first
_CACHE = threading.local()


class ObjectCache(object):
    #zope.interface.implements(interfaces.IObjectCache)

    table = 'persistence_invalidations'

    def __init__(self, datamanager):
        self._datamanager = datamanager
        #if pjpersist.datamanager.PJ_AUTO_CREATE_TABLES:
        #    self._ensure_db_objects()
        self._ensure_db_objects()
        if not hasattr(_CACHE, 'objects'):
            # XXX: for now go with a simple dict
            #      later use persistent.picklecache.PickleCache
            #      because we want to limit the cache size
            _CACHE.objects = {}
            with self._datamanager._conn.cursor() as cur:
                cur.execute("SELECT max(txn) FROM %s" % self.table)
                if cur.rowcount:
                    txn = cur.fetchone()[0]
                else:
                    txn = 0

            _CACHE.last_seen_txn = txn
        else:
            self._read_invalidations()
        self.invalidations = set()

    def _ensure_db_objects(self):
        with self._datamanager._conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM pg_class where relname = 'seq_pj_txn_serial'")
            if not cur.rowcount:
                cur.execute(
                    "CREATE SEQUENCE seq_pj_txn_serial START WITH 0 MINVALUE 0 NO MAXVALUE;")
            cur.execute(
                "SELECT * FROM information_schema.tables where table_name=%s",
                (self.table,))
            if not cur.rowcount:
                LOG.info("Creating table %s" % self.table)
                cur.execute('''
                    CREATE TABLE %s (
                        txn BIGINT PRIMARY KEY,
                        dbrefs TEXT[][])
                    ''' % self.table)

    def _get_txn_serial(self):
        with self._datamanager._conn.cursor() as cur:
            cur.execute(''' SELECT nextval('seq_pj_txn_serial') ''')
            return cur.fetchone()[0]

    def _read_invalidations(self):
        with self._datamanager._conn.cursor() as cur:
            cur.execute("SELECT txn, dbrefs FROM %s WHERE txn > %%s" % self.table,
                        (_CACHE.last_seen_txn,))
            for row in cur:
                txn, dbrefs = row
                _CACHE.last_seen_txn = max(_CACHE.last_seen_txn, txn)
                for dbref in dbrefs:
                    oref = serialize.DBRef.from_tuple(dbref)
                    try:
                        del _CACHE.objects[oref]
                    except KeyError:
                        pass

    def invalidate(self, dbref):
        self.invalidations.add(dbref)

    def commit(self):
        if self.invalidations:
            # do not insert if no changes
            ser = self._get_txn_serial()
            # XXX: might need to batch later if a shitload of objs get changed
            dbrefs = [dbref.as_tuple() for dbref in self.invalidations]

            with self._datamanager._conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO %s (txn, dbrefs) values (%%s, %%s)" % self.table,
                    (ser, dbrefs))

            _CACHE.last_seen_txn = ser

    def abort(self):
        # do some magic with the changed objects to revert changes
        self.invalidations.clear()

    def get_object(self, dbref):
        return _CACHE.objects[dbref]

    def del_object(self, dbref):
        try:
            del _CACHE.objects[dbref]
        except KeyError:
            pass

    def put_object(self, obj):
        if not hasattr(obj, '_p_oid'):
            raise AttributeError('Non-persistent object')
        if obj._p_oid is None:
            raise ValueError('Object not yet added to database')
        _CACHE.objects[obj._p_oid] = obj
