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

    Now we can run a basic query:

        >>> mq.convert({'day': 'Friday', 'drink': 'whiskey', 'nr': 42})
        <SQLOp ...>

        >>> run(_)
        ((((Bar.data) ->> ('day')) = ('Friday')) AND
         ((((Bar.data) ->> ('drink')) = ('whiskey')) AND
          (((Bar.data) ->> ('nr')) = (42))))

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

    """


def test_suite():
    return doctest.DocTestSuite(
        optionflags=testing.OPTIONFLAGS)

