##############################################################################
#
# Copyright (c) 2015 Schur Anton
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
"""Postgresql's jsonb support for sqlbuilder"""
from __future__ import absolute_import

from sqlbuilder.smartsql import Q, T, compile as parent_comile, Expr, NamedCondition, PLACEHOLDER, Name, Result

compile = parent_comile.create_child()


class JsonbSuperset(NamedCondition):
    _sql = '@>'


class JsonbContainsAll(NamedCondition):
    _sql = '?&'


class JsonOperator(Expr):
    __slots__ = ('_left', '_right')

    def __init__(self, left, right):
        self._left = left
        self._right = right


class JsonItemText(JsonOperator):
    _sql = '->>'


def jsonb_superset(inst, other):
    return JsonbSuperset(inst, other)


def jsonb_contains_all(inst, other):
    return JsonbContainsAll(inst, other)


def jsonb_item_text(inst, other):
    return JsonItemText(inst, other)


setattr(Expr, 'jsonb_superset', jsonb_superset)
setattr(Expr, 'jsonb_contains_all', jsonb_contains_all)
setattr(Expr, 'jsonb_item_text', jsonb_item_text)


class JsonArray(object):

    __slots__ = ('_value', )

    def __init__(self, value):
        self._value = value


@compile.when(JsonOperator)
def compile_json_array(compile, expr, state):
    compile(expr._left, state)
    state.sql.append(expr._sql)
    if isinstance(expr._right, basestring):
        state.sql.append("'")
    if isinstance(expr._right, basestring):
        state.sql.append(expr._right)
    else:
        state.sql.append(str(expr._right))
    if isinstance(expr._right, basestring):
        state.sql.append("'")


@compile.when(JsonArray)
def compile_json_array(compile, expr, state):
    state.sql.append("array[")
    placeholders = []
    for item in expr._value:
        placeholders.append(PLACEHOLDER)
        state.params.append(item)
    state.sql.append(', '.join(placeholders))
    state.sql.append("]")


@compile.when(Name)
def compile_name(compile, expr, state):
    state.sql.append(expr._name)


class PJResult(Result):
    compile = compile

    def __init__(self, mapping, compile=None):
        super(PJResult, self).__init__(compile=compile)
        self._mapping = mapping
        self._cur = None
        self._query = None

    def execute(self):
        query, params = super(PJResult, self).execute()
        cur = self._mapping._p_jar.getCursor()
        cur.execute(
            query, params
        )
        self._cur = cur
        return self._cur

    select = count = insert = update = delete = execute

    def unserialize(self, data):
        if data and (('pid' in data) or ('id' in data)) and 'data' in data and \
           'package' in data and 'class_name' in data:
            id = data.get('pid', data.get('id'))
            return self._mapping._p_jar._reader.load(data, self._mapping.table, id)
        return data

    def __iter__(self):
        if self._cur is None:
            self.execute()
        data = self.unserialize(self._cur.fetchone())
        yield data

    def __len__(self):
        if self._cur is None:
            return 0
        else:
            return self._cur.rowcount
