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

from pjpersist.mapping import PJTableMapping
from pjpersist import testing, smartsql
from pjpersist.smartsql import T, compile


class TestMapping(PJTableMapping):
    table = 'mapping'


def doctest_smartsql():
    r"""

        jsonb "->>" operator

        >>> table = T('tbl')
        >>> compile(table.field.jsonb_item_text('test'))
        ("tbl.field->>'test'", [])

        >>> compile(table.field.jsonb_item_text(5))
        ('tbl.field->>5', [])

        >>> compile(table.field.jsonb_path_text('a__b__2'))
        ("tbl.field#>>'{a, b, 2}'", [])

        >>> compile(smartsql.JsonArray(['x', 'y', 5]))
        ('array[%s, %s, %s]', ['x', 'y', 5])

        >>> vt = smartsql.PJMappedVirtualTable(TestMapping(None))

        >>> compile(smartsql.JsonbDataField('test', vt))
        ("mapping_state.data->>'test'", [])

        >>> compile(vt)
        ('mapping INNER JOIN mapping_state ON (mapping.id = mapping_state.pid AND mapping.tid = mapping_state.tid)', [])

        >>> compile(vt.test == 5)
        ("mapping_state.data->>'test' = %s", [5])

        Field name will be ignored in this case

        >>> compile(smartsql.JsonbSuperset(vt.test, None))
        ('mapping_state.data @> NULL', [])

        >>> compile(smartsql.JsonbContainsAll(vt.test, None))
        ('mapping_state.data ?& NULL', [])

    """


def test_suite():
    return unittest.TestSuite([
        doctest.DocTestSuite(optionflags=testing.OPTIONFLAGS),
        doctest.DocTestSuite(
            module=smartsql,
            optionflags=testing.OPTIONFLAGS),
    ])
