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
import doctest
import json

from pjpersist import testing, mquery, sqlbuilder as sb

dataset = [
    {'foo': 'bar'},
    {'nr': 42},
    {'some': {'numbers': [42, 69, 105]}},
    {'plan': 'getdown', 'day': 'Friday', 'drink': 'whiskey', 'nr': 42},
]

def jformat(obj):
    """Helper for inserting JSON"""
    return "('%s')" % json.dumps(obj).replace("'", "''")


def setUp(test):
    testing.setUp(test)

    conn = test.globs['conn']
    with conn.cursor() as cur:
        cur.execute("CREATE TABLE mq (id SERIAL PRIMARY KEY, data JSONB)")
        cur.execute("INSERT INTO mq (data) VALUES " +
                    ", ".join(jformat(datum) for datum in dataset))
    conn.commit()


def select(conn, query, print_sql=False):
    try:
        with conn.cursor() as cur:
            converter = mquery.Converter("mq", "data")
            sql = sb.sqlrepr(
                sb.Select(sb.Field("mq", "data"), where=converter.convert(query)),
                'postgres'
            )
            if print_sql:
                print 'SQL> ', sql
            cur.execute(sql)
            for e in cur.fetchall():
                print e[0] 
    finally:
        conn.rollback()

def doctest_operators():
    """Test simple selectors and comparisons:


    We can do simple matches of strings:

       >>> select(conn, {'foo': 'bar'})
       {u'foo': u'bar'}

    And numbers:

       >>> select(conn, {'nr': 42})
       {u'nr': 42}
       {u'nr': 42, u'drink': u'whiskey', u'day': u'Friday', u'plan': u'getdown'}

    We can query for an element in the list:

       >>> select(conn, {'some.numbers': 69})
       {u'some': {u'numbers': [42, 69, 105]}}

       >>> select(conn, {'nr': {'$gt': 40}})
       {u'nr': 42}
       {u'nr': 42, u'drink': u'whiskey', u'day': u'Friday', u'plan': u'getdown'}

       >>> select(conn, {'nr': {'$in': [40, 41, 42]}})
       {u'nr': 42}
       {u'nr': 42, u'drink': u'whiskey', u'day': u'Friday', u'plan': u'getdown'}

       >>> select(conn, {'foo': {'$in': ['foo', 'bar', 'baz']}})
       {u'foo': u'bar'}

       >>> select(conn, {'foo': {'$nin': ['foo', 'baz']}})
       {u'foo': u'bar'}

       >>> select(conn, {'some.numbers': {'$exists': True}})
       {u'some': {u'numbers': [42, 69, 105]}}

       >>> select(conn, {'nr': {'$exists': False}})
       {u'foo': u'bar'}
       {u'some': {u'numbers': [42, 69, 105]}}

       >>> select(conn, {'nr': {'$not': {'$gt': 40}}})
       {u'foo': u'bar'}
       {u'some': {u'numbers': [42, 69, 105]}}

       >>> select(conn, {'nr': {'$not': {'$gt': 42}}})
       {u'foo': u'bar'}
       {u'nr': 42}
       {u'some': {u'numbers': [42, 69, 105]}}
       {u'nr': 42, u'drink': u'whiskey', u'day': u'Friday', u'plan': u'getdown'}

       >>> select(conn, {'some.numbers': {'$size': 3}})
       {u'some': {u'numbers': [42, 69, 105]}}

       >>> select(conn, {'some.numbers': {'$all': [69, 42]}})
       {u'some': {u'numbers': [42, 69, 105]}}

       >>> select(conn, {'some.numbers': {'$all': [69, 43]}})

    $elemMatch matches when one of the array elements matches all
    conditions:

       >>> select(conn, {'some.numbers': {'$elemMatch':
       ...                   [{'$gt': 68}, {'$lt': 70}]}})
       {u'some': {u'numbers': [42, 69, 105]}}

       >>> select(conn, {'some.numbers': {'$elemMatch':
       ...                   [{'$gt': 60}, {'$lt': 65}]}})

    _id is specialcased:

       >>> select(conn, {'_id': 1}, True)
       SQL>  SELECT mq.data FROM mq WHERE ((mq.id) = (1))
       {u'foo': u'bar'}

       >>> select(conn, {'_id': {'$lt': 2}}, True)
       SQL>  SELECT mq.data FROM mq WHERE ((mq.id) < (2))
       {u'foo': u'bar'}

    """


def test_suite():
    suite =  doctest.DocTestSuite(
        setUp=setUp, tearDown=testing.tearDown,
        optionflags=testing.OPTIONFLAGS)
    suite.layer = testing.db_layer
    return suite
