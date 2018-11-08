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
"""Mongo Persistence Doc Tests"""
from __future__ import absolute_import
import datetime
import doctest
import unittest
from pprint import pprint

from zope.exceptions import exceptionformatter

from pjpersist import testing


class ReprMixin(object):
    def __repr__(self):
        return '<%s %s>' % (self.__class__.__name__, self)


def setUp(test):
    testing.setUp(test)
    # silence this, otherwise half-baked objects raise exceptions
    # on trying to __repr__ missing attributes
    test.orig_DEBUG_EXCEPTION_FORMATTER = \
        exceptionformatter.DEBUG_EXCEPTION_FORMATTER
    exceptionformatter.DEBUG_EXCEPTION_FORMATTER = 0

    def fetchone(table, clause=None):
        qry = 'SELECT * FROM ' + table
        if clause:
            qry += ' WHERE ' + clause
        qry += ' ORDER BY tid desc limit 1'
        with test.globs['dm'].getCursor(False) as cur:
            cur.execute(qry)
            return cur.fetchone()
    test.globs['fetchone'] = fetchone


def tearDown(test):
    testing.tearDown(test)
    exceptionformatter.DEBUG_EXCEPTION_FORMATTER = \
        test.orig_DEBUG_EXCEPTION_FORMATTER


def setUpRST(test):
    # add more stuff to globals to have less cruft around in README.rst
    setUp(test)
    test.globs['datetime'] = datetime
    test.globs['pprint'] = pprint
    test.globs['ReprMixin'] = ReprMixin


def test_suite():
    suite = unittest.TestSuite((
        doctest.DocFileSuite(
            '../README.txt',
            setUp=setUp, tearDown=tearDown,
            checker=testing.checker,
            optionflags=testing.OPTIONFLAGS),
        doctest.DocFileSuite(
            '../../../README.rst',
            setUp=setUpRST, tearDown=tearDown,
            checker=testing.checker,
            optionflags=testing.OPTIONFLAGS),
        ))
    suite.layer = testing.db_layer
    return suite
