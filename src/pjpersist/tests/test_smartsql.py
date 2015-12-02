##############################################################################
#
# Copyright (c) 2011 Zope Foundation and Contributors.
# Copyright (c) 2015 Anton Schur
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
import unittest

from pjpersist import testing, smartsql
from pjpersist.smartsql import T, compile

def doctest_smartsql():
    r"""

        jsonb "->>" operator

        >>> table = T('tbl')
        >>> compile(table.field.jsonb_item_text('test'))
        ("tbl.field->>'test'", [])

        >>> compile(table.field.jsonb_item_text(5))
        ('tbl.field->>5', [])

        >>> compile(smartsql.JsonArray(['x', 'y', 'z']))
        ('array[%s, %s, %s]', ['x', 'y', 'z'])


    """


def test_suite():
    return unittest.TestSuite([
        doctest.DocTestSuite(optionflags=testing.OPTIONFLAGS),
        doctest.DocTestSuite(
            module=smartsql,
            optionflags=testing.OPTIONFLAGS),
    ])
