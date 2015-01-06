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
#_CACHE = threading.local()


def get_cache(datamanager):
    return TransactionalObjectCache(datamanager)
    #return DatamanagerObjectCache(datamanager)


def dbref_key(dbref):
    return hash(dbref)


class TransactionalObjectCache(object):
    def __init__(self, datamanager):
        self._datamanager = datamanager
        self.objects = {}

    def _ensure_db_objects(self):
        pass

    def commit(self):
        self.objects = {}

    def abort(self):
        self.objects = {}

    def clear_cache(self):
        self.objects = {}

    def invalidate(self, obj):
        pass

    def get_object(self, dbref):
        try:
            rv = self.objects[dbref_key(dbref)]
            #print "CACHE hit", dbref
            return rv
        except KeyError:
            #print "CACHE miss", dbref
            raise

    def del_object(self, obj):
        try:
            del self.objects[dbref_key(obj._p_oid)]
        except KeyError:
            pass

    def put_object(self, obj):
        if not hasattr(obj, '_p_oid'):
            raise AttributeError('Non-persistent object')
        if obj._p_oid is None:
            raise ValueError('Object not yet added to database')
        self.objects[dbref_key(obj._p_oid)] = obj


class DatamanagerObjectCache(TransactionalObjectCache):
    #zope.interface.implements(interfaces.IObjectCache)

    # a very important basic rule:
    # objects get their _p_jar set to the PJDataManager which loads them
    # then these objects get into the cache, with their _p_jar
    # the result is that these objects must be pulled from the cache
    # by the VERY SAME PJDataManager, therefore the cache must be kept
    # on the PJDataManager object
    # you want to keep the PJDataManager instance around by using a pool

    table = 'persistence_invalidations'

    def __init__(self, datamanager):
        self._datamanager = datamanager
        #if pjpersist.datamanager.PJ_AUTO_CREATE_TABLES:
        #    self._ensure_db_objects()
        self._ensure_db_objects()
        if not hasattr(self._datamanager, '_DatamanagerObjectCache_objects'):
            # XXX: for now go with a simple dict
            #      later use persistent.picklecache.PickleCache
            #      because we want to limit the cache size
            self._datamanager._DatamanagerObjectCache_objects = {}
            with self._datamanager._conn.cursor() as cur:
                cur.execute("SELECT max(txn) FROM %s" % self.table)
                if cur.rowcount:
                    txn = cur.fetchone()[0]
                else:
                    txn = 0

            self.last_seen_txn = txn
        else:
            self._read_invalidations()
        self.invalidations = set()

    def _ensure_db_objects(self):
        with self._datamanager._conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM pg_class where relname = 'seq_pj_txn_serial'")
            if not cur.rowcount:
                cur.execute("""
                            CREATE SEQUENCE seq_pj_txn_serial
                            START WITH 0 MINVALUE 0 NO MAXVALUE;""")
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

    @property
    def objects(self):
        return self._datamanager._DatamanagerObjectCache_objects

    @property
    def last_seen_txn(self):
        return self._datamanager._DatamanagerObjectCache_last_seen_txn

    @last_seen_txn.setter
    def last_seen_txn(self, value):
        self._datamanager._DatamanagerObjectCache_last_seen_txn = value

    def _get_txn_serial(self):
        with self._datamanager._conn.cursor() as cur:
            cur.execute(''' SELECT nextval('seq_pj_txn_serial') ''')
            return cur.fetchone()[0]

    def _read_invalidations(self):
        with self._datamanager._conn.cursor() as cur:
            cur.execute("SELECT txn, dbrefs FROM %s WHERE txn > %%s" % self.table,
                        (self.last_seen_txn,))
            objs = self.objects
            for row in cur:
                txn, dbrefs = row
                self.last_seen_txn = max(self.last_seen_txn, txn)
                for dbref in dbrefs:
                    oref = serialize.DBRef.from_tuple(dbref)
                    try:
                        del objs[dbref_key(oref)]
                    except KeyError:
                        pass

    def invalidate(self, obj):
        self.invalidations.add(obj._p_oid)

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

            self.last_seen_txn = ser

    def abort(self):
        # do some magic with the changed objects to revert changes
        # the simplest way seems to be to just nuke the object from the cache
        for dbref in self.invalidations:
            try:
                del self.objects[dbref_key(dbref)]
            except KeyError:
                pass

        self.invalidations.clear()

    def clear_cache(self):
        del self._datamanager._DatamanagerObjectCache_objects
        del self._datamanager._DatamanagerObjectCache_last_seen_txn


class ConnectionObjectCache(DatamanagerObjectCache):
    def __init__(self, datamanager):
        self._datamanager = datamanager
        #if pjpersist.datamanager.PJ_AUTO_CREATE_TABLES:
        #    self._ensure_db_objects()
        self._ensure_db_objects()
        if not hasattr(self._datamanager._conn, '_pj_object_cache'):
            # XXX: for now go with a simple dict
            #      later use persistent.picklecache.PickleCache
            #      because we want to limit the cache size
            self._datamanager._conn._pj_object_cache = {}
            with self._datamanager._conn.cursor() as cur:
                cur.execute("SELECT max(txn) FROM %s" % self.table)
                if cur.rowcount:
                    txn = cur.fetchone()[0]
                else:
                    txn = 0

            self._datamanager._conn._pj_last_seen_txn = txn
        else:
            self._read_invalidations()
        self.invalidations = set()

    @property
    def objects(self):
        return self._datamanager._conn._pj_object_cache

    @property
    def last_seen_txn(self):
        return self._datamanager._conn.last_seen_txn

    @last_seen_txn.setter
    def last_seen_txn(self, value):
        self._datamanager._conn.last_seen_txn = value

    def clear_cache(self):
        self.objects = {}
