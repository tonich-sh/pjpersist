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
import re
import transaction
from zope.testing import cleanup, module, renormalizing

from pjpersist import datamanager, serialize, serializers

checker = renormalizing.RENormalizing([
    # Date/Time objects
    (re.compile(r'datetime.datetime\(.*\)'),
     'datetime.datetime(2011, 10, 1, 9, 45)'),
    # UUIDs
    (re.compile(r"'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}'"),
     "'00000000-0000-0000-0000-000000000000'"),
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
    return psycopg2.connect(
        database=database or 'template1',
        host='localhost', port=5433,
        user='shoobx', password='shoobx')


def createDB():
    dropDB()
    conn = getConnection()
    with conn.cursor() as cur:
        cur.execute('END')
        cur.execute('CREATE DATABASE %s' %DBNAME)
    conn.close()

def dropDB():
    conn = getConnection()
    with conn.cursor() as cur:
        cur.execute('END')
        try:
            cur.execute('DROP DATABASE %s' %DBNAME)
        except psycopg2.ProgrammingError:
            pass
    conn.close()

def setUp(test):
    module.setUp(test)
    serialize.SERIALIZERS = [serializers.DateTimeSerializer(),
                             serializers.DateSerializer(),
                             serializers.TimeSerializer()]
    createDB()
    test.globs['conn'] = getConnection(DBNAME)
    test.globs['commit'] = transaction.commit
    test.globs['dm'] = datamanager.PJDataManager(test.globs['conn'])


def tearDown(test):
    module.tearDown(test)
    transaction.abort()
    test.globs['conn'].close()
    dropDB()
    resetCaches()
    serialize.SERIALIZERS = []


def resetCaches():
    serialize.SERIALIZERS.__init__()
    serialize.OID_CLASS_LRU.__init__(20000)
    serialize.TABLES_WITH_TYPE.__init__()
    serialize.AVAILABLE_NAME_MAPPINGS.__init__()
    serialize.PATH_RESOLVE_CACHE = {}

cleanup.addCleanUp(resetCaches)
atexit.register(dropDB)
