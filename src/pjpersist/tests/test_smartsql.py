# -*- coding: utf-8 -*-
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

from __future__ import absolute_import
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

    to use index scan on datetime field we must define IMMUTABLE cast function:

    CREATE FUNCTION to_timestamp_cast(TEXT) RETURNS TIMESTAMP
        AS 'select cast($1 as TIMESTAMP)'
        LANGUAGE SQL
        IMMUTABLE
        RETURNS NULL ON NULL INPUT;

    and create functional index:

    CREATE INDEX mapping_state_test_idx
        ON mapping_state
        USING btree
        (to_timestamp_cast(cast(data#>>'{test,value}' as text)));

        >>> compile(vt.test.as_datetime() == 5)
        ("to_timestamp_cast(mapping_state.data#>>'{test, value}') = %s", [5])

    .as_bool() treats null as false (e.g. if json field not exists)

    CREATE FUNCTION to_bool_cast(TEXT) RETURNS BOOLEAN
        AS 'select case when ($1) is null then false else cast($1 as BOOLEAN) end'
        LANGUAGE SQL
        IMMUTABLE
    ;

        >>> compile(vt.test.as_bool() == False)
        ("to_bool_cast(mapping_state.data->>'test') = %s", [False])

        >>> compile(vt.test.like(u'Test русский'))
        ("mapping_state.data->>'test' LIKE %s", [u'Test \xd1\x80\xd1\x83\xd1\x81\xd1\x81\xd0\xba\xd0\xb8\xd0\xb9'])

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
