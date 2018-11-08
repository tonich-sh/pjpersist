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
from __future__ import absolute_import
from __future__ import print_function
import datetime
import doctest
import json
from pprint import pprint

from pjpersist import datamanager
from pjpersist import testing
from pjpersist import serialize
from pjpersist import sqlbuilder as sb

DTIMES = [
    datetime.datetime(2013,1,22,18,59),
    datetime.datetime(1987,2,2,8,31),
    datetime.datetime(1949,6,25,23,50),
    datetime.datetime(1918,3,3,8,35),
    datetime.datetime(1980,7,23,8,6),
    datetime.datetime(2011,11,1,16,26),
    datetime.datetime(1911,1,17,4,24),
    datetime.datetime(1975,7,22,4,44),
    datetime.datetime(1914,5,17,9,37),
    None,
    datetime.datetime(1936,12,16,10,38),
    datetime.datetime(1930,12,12,3,58),
    datetime.datetime(1995,1,23,9,59),
    datetime.datetime(1923,6,2,15,5),
    datetime.datetime(1967,3,1,13,42),
    datetime.datetime(1966,7,22,17,30),
    datetime.datetime(2005,7,13,17,18),
    datetime.datetime(1912,1,9,15,4),
    datetime.datetime(1983,6,27,3,56),
    None,
]


def pjvalue(obj):
    return serialize.ObjectWriter(None).get_state(obj)


def jformat(obj):
    """Helper for inserting JSON"""
    return datamanager.Json(pjvalue(obj))


def setUp(test):
    testing.setUp(test)

    conn = test.globs['conn']
    with conn.cursor() as cur:
        cur.execute("CREATE TABLE ser (id SERIAL PRIMARY KEY, data JSONB)")
        for dt in DTIMES:
            obj = dict(dtime=dt)
            cur.execute("INSERT INTO ser (data) VALUES (%s)", (jformat(obj),))
    conn.commit()


def select(conn, query, print_sql=False, **kwargs):
    try:
        with conn.cursor() as cur:
            sql = sb.sqlrepr(
                sb.Select(sb.Field("ser", "data"), where=query, **kwargs),
                'postgres'
            )
            if print_sql:
                print('SQL> ', sql)
            cur.execute(sql)
            for e in cur.fetchall():
                pprint(e[0])
    finally:
        conn.rollback()


def doctest_datetime_range():
    """Test datetime serialization vs. SQL range query

    >> dumpTable('ser')

    >>> datafld = sb.Field('ser', 'data')
    >>> select(conn, sb.JGET(datafld, 'dtime') == pjvalue(DTIMES[10]), True)
    SQL>  SELECT ser.data FROM ser WHERE (((ser.data) -> ('dtime')) = ('{"_py_type": "datetime.datetime", "value": "1936-12-16T10:38:00"}'::jsonb))
    {u'dtime': {u'_py_type': u'datetime.datetime',
                u'value': u'1936-12-16T10:38:00'}}

    >>> select(conn, sb.JGET(datafld, 'dtime') > pjvalue(DTIMES[10]),
    ...     print_sql=True, orderBy="(data->'dtime')")
    SQL>  SELECT ser.data FROM ser WHERE (((ser.data) -> ('dtime')) > ('{"_py_type": "datetime.datetime", "value": "1936-12-16T10:38:00"}'::jsonb)) ORDER BY (data->'dtime')
    {u'dtime': {u'_py_type': u'datetime.datetime',
                u'value': u'1949-06-25T23:50:00'}}
    {u'dtime': {u'_py_type': u'datetime.datetime',
                u'value': u'1966-07-22T17:30:00'}}
    {u'dtime': {u'_py_type': u'datetime.datetime',
                u'value': u'1967-03-01T13:42:00'}}
    {u'dtime': {u'_py_type': u'datetime.datetime',
                u'value': u'1975-07-22T04:44:00'}}
    {u'dtime': {u'_py_type': u'datetime.datetime',
                u'value': u'1980-07-23T08:06:00'}}
    {u'dtime': {u'_py_type': u'datetime.datetime',
                u'value': u'1983-06-27T03:56:00'}}
    {u'dtime': {u'_py_type': u'datetime.datetime',
                u'value': u'1987-02-02T08:31:00'}}
    {u'dtime': {u'_py_type': u'datetime.datetime',
                u'value': u'1995-01-23T09:59:00'}}
    {u'dtime': {u'_py_type': u'datetime.datetime',
                u'value': u'2005-07-13T17:18:00'}}
    {u'dtime': {u'_py_type': u'datetime.datetime',
                u'value': u'2011-11-01T16:26:00'}}
    {u'dtime': {u'_py_type': u'datetime.datetime',
                u'value': u'2013-01-22T18:59:00'}}

    >>> sorted([dt for dt in DTIMES if dt > DTIMES[10]])
    Traceback (most recent call last):
    ...
    TypeError: can't compare datetime.datetime to NoneType

    >>> select(conn, sb.JGET(datafld, 'dtime') < pjvalue(DTIMES[10]),
    ...     print_sql=True, orderBy="(data->'dtime')")
    SQL>  SELECT ser.data FROM ser WHERE (((ser.data) -> ('dtime')) < ('{"_py_type": "datetime.datetime", "value": "1936-12-16T10:38:00"}'::jsonb)) ORDER BY (data->'dtime')
    {u'dtime': None}
    {u'dtime': None}
    {u'dtime': {u'_py_type': u'datetime.datetime',
                u'value': u'1911-01-17T04:24:00'}}
    {u'dtime': {u'_py_type': u'datetime.datetime',
                u'value': u'1912-01-09T15:04:00'}}
    {u'dtime': {u'_py_type': u'datetime.datetime',
                u'value': u'1914-05-17T09:37:00'}}
    {u'dtime': {u'_py_type': u'datetime.datetime',
                u'value': u'1918-03-03T08:35:00'}}
    {u'dtime': {u'_py_type': u'datetime.datetime',
                u'value': u'1923-06-02T15:05:00'}}
    {u'dtime': {u'_py_type': u'datetime.datetime',
                u'value': u'1930-12-12T03:58:00'}}

    >>> select(conn, sb.JGET(datafld, 'dtime') == None,
    ...     print_sql=True, orderBy="(data->'dtime')")
    SQL>  SELECT ser.data FROM ser WHERE (((ser.data) -> ('dtime')) = ('null'::jsonb)) ORDER BY (data->'dtime')
    {u'dtime': None}
    {u'dtime': None}

    >>> select(conn, sb.JGET(datafld, 'dtime') != None,
    ...     print_sql=True, orderBy="(data->'dtime')")
    SQL>  SELECT ser.data FROM ser WHERE (((ser.data) -> ('dtime')) <> ('null'::jsonb)) ORDER BY (data->'dtime')
    {u'dtime': {u'_py_type': u'datetime.datetime',
                u'value': u'1911-01-17T04:24:00'}}
    {u'dtime': {u'_py_type': u'datetime.datetime',
                u'value': u'1912-01-09T15:04:00'}}
    {u'dtime': {u'_py_type': u'datetime.datetime',
                u'value': u'1914-05-17T09:37:00'}}
    {u'dtime': {u'_py_type': u'datetime.datetime',
                u'value': u'1918-03-03T08:35:00'}}
    {u'dtime': {u'_py_type': u'datetime.datetime',
                u'value': u'1923-06-02T15:05:00'}}
    {u'dtime': {u'_py_type': u'datetime.datetime',
                u'value': u'1930-12-12T03:58:00'}}
    {u'dtime': {u'_py_type': u'datetime.datetime',
                u'value': u'1936-12-16T10:38:00'}}
    {u'dtime': {u'_py_type': u'datetime.datetime',
                u'value': u'1949-06-25T23:50:00'}}
    {u'dtime': {u'_py_type': u'datetime.datetime',
                u'value': u'1966-07-22T17:30:00'}}
    {u'dtime': {u'_py_type': u'datetime.datetime',
                u'value': u'1967-03-01T13:42:00'}}
    {u'dtime': {u'_py_type': u'datetime.datetime',
                u'value': u'1975-07-22T04:44:00'}}
    {u'dtime': {u'_py_type': u'datetime.datetime',
                u'value': u'1980-07-23T08:06:00'}}
    {u'dtime': {u'_py_type': u'datetime.datetime',
                u'value': u'1983-06-27T03:56:00'}}
    {u'dtime': {u'_py_type': u'datetime.datetime',
                u'value': u'1987-02-02T08:31:00'}}
    {u'dtime': {u'_py_type': u'datetime.datetime',
                u'value': u'1995-01-23T09:59:00'}}
    {u'dtime': {u'_py_type': u'datetime.datetime',
                u'value': u'2005-07-13T17:18:00'}}
    {u'dtime': {u'_py_type': u'datetime.datetime',
                u'value': u'2011-11-01T16:26:00'}}
    {u'dtime': {u'_py_type': u'datetime.datetime',
                u'value': u'2013-01-22T18:59:00'}}

    """


def test_suite():
    suite = doctest.DocTestSuite(
        setUp=setUp, tearDown=testing.tearDown,
        optionflags=testing.OPTIONFLAGS)
    suite.layer = testing.db_layer
    return suite
