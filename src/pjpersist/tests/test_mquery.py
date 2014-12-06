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
from pjpersist import testing, mquery


def run(sqlx):
    print sqlx.__sqlrepr__('postgres')


def test_convert():
    """Test mquery.convert.

    We can parse MongoDB queries and convert them to SQLBuilder
    expressions.  First we inititalize the converter with our table
    and field names:

        >>> mq = mquery.Converter("Bar", "data")

    Let's enable a mode that simplifies some crud for nicer tests:

        >>> mq.simplified = True

    Now we can run a basic query:

        >>> mq.convert({'day': 'Friday', 'drink': 'whiskey', 'nr': 42})
        <SQLOp ...>

        >>> run(_)
        ((((Bar.data) ->> ('day')) = ('Friday')) AND
         ((((Bar.data) ->> ('drink')) = ('whiskey')) AND
          (((Bar.data) ->> ('nr')) = (42))))

    Actually, this is wrong, because in mongo the test could as well
    mean checking for an member in an array:

        >>> mq.simplified = False
        >>> run(mq.convert({'nr': 42}))
        ((((Bar.data) ->> ('nr')) = (42)) OR
         ((('[]'::jsonb) <@ ((Bar.data) ->> ('nr'))) AND
          (((Bar.data) ->> ('nr')) ? (42))))

    Enough craziness:

        >>> mq.simplified = True

    Keys can be dotted paths:

        >>> run(mq.convert({'contract.week.day': 'Friday'}))
        (((Bar.data) #>> (array['contract', 'week', 'day'])) = ('Friday'))

    Values can be objects with comparison directives:

        >>> run(mq.convert({'quantity': {'$gt': 20}}))
        (((Bar.data) ->> ('quantity')) > (20))

        >>> run(mq.convert({'quantity': {'$lte': 20}}))
        (((Bar.data) ->> ('quantity')) <= (20))

        >>> run(mq.convert({'quantity': {'$in': [20, 30, 40]}}))
        (((Bar.data) ->> ('quantity')) IN (20, 30, 40))

        >>> run(mq.convert({'quantity': {'$nin': [20, 30, 40]}}))
        NOT (((Bar.data) ->> ('quantity')) IN (20, 30, 40))

    There can be just one element in this object:

        >>> run(mq.convert({'quantity': {'$eq': 1, 'foo': 'bar'}}))
        Traceback (most recent call last):
          ...
        ValueError: Too many elements: {'$eq': 1, 'foo': 'bar'}

    The $not operator:

        >>> run(mq.convert({'quantity': {'$not': {'$gt': 20}}}))
        NOT (((Bar.data) ->> ('quantity')) > (20))

    """

def test_convert_logical():
    """Test mquery.convert logical operators.

    Setup:

        >>> mq = mquery.Converter("Bar", "data")
        >>> mq.simplified = True

    There are three operators that aggregate  lists of queries:

        >>> run(mq.convert({
        ...     '$and': [{'mode': 1}, {'foo': 'bar'}]}))
        ((((Bar.data) ->> ('mode')) = (1)) AND (((Bar.data) ->> ('foo')) = ('bar')))

        >>> run(mq.convert({
        ...     '$or': [{'mode': 1}, {'foo': 'bar'}]}))
        ((((Bar.data) ->> ('mode')) = (1)) OR (((Bar.data) ->> ('foo')) = ('bar')))

        >>> run(mq.convert({
        ...     '$nor': [{'mode': 1}, {'foo': 'bar'}]}))
        NOT ((((Bar.data) ->> ('mode')) = (1)) OR (((Bar.data) ->> ('foo')) = ('bar')))

    """

def test_suite():
    return doctest.DocTestSuite(
        optionflags=testing.OPTIONFLAGS)

