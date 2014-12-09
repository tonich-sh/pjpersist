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
import psycopg2
import psycopg2.extras
import re
import transaction
from pprint import pprint
from zope.testing import cleanup, module, renormalizing

from pjpersist import datamanager, serialize, serializers

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


def getConnection(database=None):
    conn = psycopg2.connect(
        database=database or 'template1',
        host='localhost', port=5432,
        user='shoobx', password='shoobx')
    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_SERIALIZABLE)
    return conn


def createDB():
    dropDB()
    conn = getConnection()
    with conn.cursor() as cur:
        cur.execute('END')
        cur.execute('CREATE DATABASE %s' %DBNAME)
    conn.commit()
    conn.close()

def dropDB():
    conn = getConnection()
    with conn.cursor() as cur:
        cur.execute('END')
        try:
            cur.execute('DROP DATABASE %s' %DBNAME)
        except psycopg2.ProgrammingError:
            pass
    conn.commit()
    conn.close()


def cleanDB(conn=None):
    if conn is None:
        conn = getConnection(DBNAME)
    with conn.cursor() as cur:
        cur.execute("""SELECT tablename FROM pg_tables""")
        for res in cur.fetchall():
            if not res[0].startswith('pg_') and not res[0].startswith('sql_'):
                cur.execute('DROP TABLE ' + res[0])
    conn.commit()


def setUp(test):
    module.setUp(test)
    serialize.SERIALIZERS = [serializers.DateTimeSerializer(),
                             serializers.DateSerializer(),
                             serializers.TimeSerializer()]
    #createDB()
    test.globs['conn'] = getConnection(DBNAME)
    cleanDB(test.globs['conn'])
    test.globs['commit'] = transaction.commit
    test.globs['dm'] = datamanager.PJDataManager(test.globs['conn'])

    def dumpTable(table, flush=True, isolate=False):
        if isolate:
            conn = getConnection(database=DBNAME)
        else:
            conn = test.globs['dm']._conn
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            try:
                cur.execute('SELECT * FROM ' + table)
            except psycopg2.ProgrammingError, err:
                print err
            else:
                pprint([dict(e) for e in cur.fetchall()])
        if isolate:
            conn.close()
    test.globs['dumpTable'] = dumpTable


def tearDown(test):
    module.tearDown(test)
    transaction.abort()
    cleanDB(test.globs['conn'])
    test.globs['conn'].close()
    #dropDB()
    resetCaches()
    serialize.SERIALIZERS = []


class DatabaseLayer(object):
    __bases__ = ()

    def __init__(self, name):
        self.__name__ = name

    def setUp(self):
        createDB()

    def tearDown(self):
        dropDB()


db_layer = DatabaseLayer("db_layer")


def resetCaches():
    serialize.SERIALIZERS.__init__()
    serialize.OID_CLASS_LRU.__init__(20000)
    serialize.TABLES_WITH_TYPE.__init__()
    serialize.AVAILABLE_NAME_MAPPINGS.__init__()
    serialize.PATH_RESOLVE_CACHE = {}


def log_sql_to_file(fname, add_tb=True, tb_limit=15):
    import logging

    datamanager.PJ_ACCESS_LOGGING = True
    datamanager.LOG.setLevel(logging.DEBUG)
    datamanager.PJPersistCursor.ADD_TB = add_tb
    datamanager.PJPersistCursor.TB_LIMIT = tb_limit

    fh = logging.FileHandler(fname)
    fh.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        '%(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    datamanager.LOG.addHandler(fh)


cleanup.addCleanUp(resetCaches)
atexit.register(dropDB)
