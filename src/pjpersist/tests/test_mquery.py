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

    The $not operator.  It matches if the field does not exist or
    does not match the condition:

        >>> run(mq.convert({'quantity': {'$not': {'$gt': 20}}}))
        ((((Bar.data) ->> ('quantity')) IS NULL) OR
         (NOT (((Bar.data) ->> ('quantity')) > (20))))

    The $size operator:

        >>> run(mq.convert({'quantities': {'$size': 3}}))
        ((json_array_length(((Bar.data) -> ('quantities')))) = (3))

        >>> run(mq.convert({'some.quantities': {'$size': 3}}))
        ((json_array_length(((Bar.data) #> (array['some', 'quantities'])))) = (3))

    The $exists operator

        >>> run(mq.convert({'quantities': {'$exists': True}}))
        (((Bar.data) -> ('quantities')) IS NOT NULL)

        >>> run(mq.convert({'quantities': {'$exists': False}}))
        (((Bar.data) -> ('quantities')) IS NULL)

    With a dotted path the $exists is more complex:

        >>> run(mq.convert({'foo.bar.baz': {'$exists': True}}))
        (((Bar.data) #> (array['foo', 'bar', 'baz'])) IS NOT NULL)

    There can be several operators in one dict:

        >>> run(mq.convert({'qty': {'$exists': True, '$nin': [1, 2]}}))
        ((((Bar.data) -> ('qty')) IS NOT NULL) AND (NOT (((Bar.data) ->> ('qty')) IN (1, 2))))

    The $all operator

        >>> run(mq.convert({'some.quantities': {'$all': [1, 3]}}))
        (((Bar.data) #> (array['some', 'quantities'])) @> ('[1, 3]'))

    $elemMatch is tricky:

        >>> run(mq.convert({'nrs': {'$elemMatch': [{'$gt': 2}, {'$lte': 3}]}}))
        EXISTS (SELECT values FROM jsonb_array_elements(((Bar.data) -> ('nrs')))
                WHERE ((((Bar.data) ->> ('nrs')) > (2)) AND
                       (((Bar.data) ->> ('nrs')) <= (3))))


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


def test_convert_id():
    """Test mquery.convert _id handling.

    Setup:

        >>> mq = mquery.Converter("Bar", "data")
        >>> mq.simplified = True

    Fields named _id are translated to query the id column directly on
    the table:

        >>> run(mq.convert({'_id': '3334444555', "lastname": "Getinthechopper"}))
        (((Bar.id) = ('3334444555')) AND
         (((Bar.data) ->> ('lastname')) = ('Getinthechopper')))

        >>> run(mq.convert({'_id': {'$exists': True}}))
        ((Bar.id) IS NOT NULL)

    """

def test_suite():
    return doctest.DocTestSuite(
        optionflags=testing.OPTIONFLAGS)
