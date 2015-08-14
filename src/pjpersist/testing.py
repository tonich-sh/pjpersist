##############################################################################
#
# Copyright (c) 2011 Zope Foundation and Contributors.
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
"""Mongo Persistence Testing Support"""
from __future__ import absolute_import
import atexit
import doctest
import logging
import psycopg2
import psycopg2.extras
import os
import re
import sys
import threading
import transaction
import unittest
from pprint import pprint
from StringIO import StringIO

import zope.component
from zope.testing import module, renormalizing

from pjpersist import datamanager, serialize, serializers, interfaces

checker = renormalizing.RENormalizing([
    # Date/Time objects
    (re.compile(r'datetime.datetime\(.*\)'),
     'datetime.datetime(2011, 10, 1, 9, 45)'),
    # IDs
    (re.compile(r"'[0-9a-f]{24}'"),
     "'0001020304050607080a0b0c0'"),
    # Object repr output.
    (re.compile(r"object at 0x[0-9a-f]*>"),
     "object at 0x001122>"),
    ])

OPTIONFLAGS = (
    doctest.NORMALIZE_WHITESPACE|
    doctest.ELLIPSIS|
    doctest.REPORT_ONLY_FIRST_FAILURE
    #|doctest.REPORT_NDIFF
    )

DBNAME = 'pjpersist_test'
DBNAME_OTHER = 'pjpersist_test_other'


@zope.interface.implementer(interfaces.IPJDataManagerProvider)
class SimpleDataManagerProvider(object):
    def __init__(self, dms, default=None):
        self.idx = {dm.database: dm for dm in dms}
        self.idx[None] = default

    def get(self, database):
        return self.idx[database]


def getConnection(database=None):
    conn = psycopg2.connect(
        database=database or 'template1',
        host=os.getenv('PJ_DB_HOST', 'localhost'), port=5432,
        user='pjpersist', password='pjpersist')
    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_SERIALIZABLE)
    return conn


def createDB():
    dropDB()
    conn = getConnection()
    with conn.cursor() as cur:
        cur.execute('END')
        cur.execute('CREATE DATABASE %s' % DBNAME)
        cur.execute('CREATE DATABASE %s' % DBNAME_OTHER)
    conn.commit()
    conn.close()


def dropDB():
    conn = getConnection()
    with conn.cursor() as cur:
        cur.execute('END')
        try:
            cur.execute('DROP DATABASE %s' % DBNAME_OTHER)
            cur.execute('DROP DATABASE %s' % DBNAME)
        except psycopg2.ProgrammingError:
            pass
    conn.commit()
    conn.close()


def cleanDB(conn=None):
    if conn is None:
        conn = getConnection(DBNAME)
    conn.rollback()
    with conn.cursor() as cur:
        cur.execute("""SELECT tablename FROM pg_tables""")
        for res in cur.fetchall():
            if not res[0].startswith('pg_') and not res[0].startswith('sql_'):
                cur.execute('DROP TABLE ' + res[0])
    conn.commit()


def setUpSerializers(test):
    serialize.SERIALIZERS = [serializers.DateTimeSerializer(),
                             serializers.DateSerializer(),
                             serializers.TimeSerializer()]


def tearDownSerializers(test):
    del serialize.SERIALIZERS[:]


def setUp(test):
    module.setUp(test)
    setUpSerializers(test)
    # createDB()
    g = test.globs
    g['conn'] = getConnection(DBNAME)
    g['conn_other'] = getConnection(DBNAME_OTHER)
    cleanDB(g['conn'])
    cleanDB(g['conn_other'])
    g['commit'] = transaction.commit
    g['dm'] = datamanager.PJDataManager(g['conn'])
    g['dm_other'] = datamanager.PJDataManager(g['conn_other'])

    def dumpTable(table, flush=True, isolate=False):
        if isolate:
            conn = getConnection(database=DBNAME)
        else:
            conn = g['dm']._conn
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            try:
                cur.execute('SELECT * FROM %s m join %s_state s on m.id = s.pid and m.tid = s.tid' % (table, table))
            except psycopg2.ProgrammingError, err:
                print err
            else:
                pprint([dict(e) for e in cur.fetchall()])
        if isolate:
            conn.close()
    g['dumpTable'] = dumpTable

    dmp = SimpleDataManagerProvider([g['dm'], g['dm_other']], g['dm'])
    zope.component.provideUtility(dmp)


def tearDown(test):
    module.tearDown(test)
    tearDownSerializers(test)
    transaction.abort()
    cleanDB(test.globs['conn'])
    cleanDB(test.globs['conn_other'])
    test.globs['conn'].close()
    test.globs['conn_other'].close()
    # dropDB()
    resetCaches()


class DatabaseLayer(object):
    __bases__ = ()

    def __init__(self, name):
        self.__name__ = name

    def setUp(self):
        createDB()
        self.setUpSqlLogging()

    def tearDown(self):
        self.tearDownSqlLogging()
        dropDB()

    def setUpSqlLogging(self):
        if "SHOW_SQL" not in os.environ:
            return

        self.save_PJ_ACCESS_LOGGING = datamanager.PJ_ACCESS_LOGGING
        datamanager.PJ_ACCESS_LOGGING = True

        setUpLogging(datamanager.TABLE_LOG, copy_to_stdout=True)
        setUpLogging(datamanager.LOG, copy_to_stdout=True)

    def tearDownSqlLogging(self):
        if "SHOW_SQL" not in os.environ:
            return

        tearDownLogging(datamanager.LOG)
        tearDownLogging(datamanager.TABLE_LOG)

        datamanager.PJ_ACCESS_LOGGING = self.save_PJ_ACCESS_LOGGING


db_layer = DatabaseLayer("db_layer")


class PJTestCase(unittest.TestCase):
    layer = db_layer

    def setUp(self):
        #module.setUp(self)
        setUpSerializers(self)
        self.conn = getConnection(DBNAME)
        cleanDB(self.conn)
        self.dm = datamanager.PJDataManager(self.conn)

    def tearDown(self):
        #module.tearDown(self)
        tearDownSerializers(self)
        transaction.abort()
        cleanDB(self.conn)
        self.conn.close()
        resetCaches()


def resetCaches():
    serialize.OID_CLASS_LRU.__init__(20000)
    serialize.AVAILABLE_NAME_MAPPINGS.__init__()
    serialize.PATH_RESOLVE_CACHE = {}
    serialize.TABLE_KLASS_MAP = {}


def log_sql_to_file(fname, add_tb=True, tb_limit=15):
    import logging

    datamanager.PJ_ENABLE_QUERY_STATS = True
    datamanager.PJ_ACCESS_LOGGING = True
    datamanager.TABLE_LOG.setLevel(logging.DEBUG)
    datamanager.PJPersistCursor.TB_LIMIT = tb_limit

    fh = logging.FileHandler(fname)
    fh.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        '%(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    datamanager.TABLE_LOG.addHandler(fh)


class StdoutHandler(logging.StreamHandler):
    """Logging handler that follows the current binding of sys.stdout."""

    def __init__(self):
        # skip logging.StreamHandler.__init__()
        logging.Handler.__init__(self)

    @property
    def stream(self):
        return sys.stdout


def setUpLogging(logger, level=logging.DEBUG, format='%(message)s',
                 copy_to_stdout=False):
    if isinstance(logger, str):
        logger = logging.getLogger(logger)
    buf = StringIO()
    handler = logging.StreamHandler(buf)
    handler._added_by_tests_ = True
    handler._old_propagate_ = logger.propagate
    handler._old_level_ = logger.level
    handler.setFormatter(logging.Formatter(format))
    logger.addHandler(handler)
    if copy_to_stdout:
        # can't use logging.StreamHandler(sys.stdout) because sys.stdout might
        # be changed latter to a StringIO, and we want messages to be seen
        # by doctests.
        handler = StdoutHandler()
        handler._added_by_tests_ = True
        handler._old_propagate_ = logger.propagate
        handler._old_level_ = logger.level
        handler.setFormatter(logging.Formatter(format))
        logger.addHandler(handler)
    logger.propagate = False
    logger.setLevel(level)
    return buf


def tearDownLogging(logger):
    if isinstance(logger, str):
        logger = logging.getLogger(logger)
    for handler in list(logger.handlers):
        if hasattr(handler, '_added_by_tests_'):
            logger.removeHandler(handler)
            logger.propagate = handler._old_propagate_
            logger.setLevel(handler._old_level_)


#TO_JOIN = []
def run_in_thread(func):
    t = threading.Thread(target=func)
    t.setDaemon(True)
    t.start()
    #TO_JOIN.append(t)
