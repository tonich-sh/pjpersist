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

from sqlbuilder.smartsql import Q, T, compile as parent_comile, Expr, NamedCondition, \
    PLACEHOLDER, Name, Result, MetaTable, MetaField, FieldProxy, cr, same, \
    LOOKUP_SEP, Field

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


class JsonbDataField(MetaField("NewBase", (Expr,), {})):

    __slots__ = ('_name', '_prefix', '__cached__')

    def __init__(self, name, prefix=None):
        self._name = name
        self._prefix = prefix
        self.__cached__ = {}


@cr
class PJMappedVirtualTable(MetaTable("NewBase", (object, ), {})):

    __slots__ = ('_mapping', 'fields', '__cached__')

    def __init__(self, mapping):
        self._mapping = mapping
        self.fields = FieldProxy(self)
        self.__cached__ = {}

    def __getattr__(self, key):
        if key[0] == '_':
            raise AttributeError

        if key in self.fields.__dict__:
            return self.fields.__dict__[key]

        parts = key.split(LOOKUP_SEP, 1)
        name, alias = parts + [None] * (2 - len(parts))

        if name in self.fields.__dict__:
            f = self.fields.__dict__[name]
        else:
            f = JsonbDataField(name, self)
            setattr(self.fields, name, f)
        if alias:
            f = f.as_(alias)
        setattr(self.fields, key, f)
        return f

    # TODO: ? join ...
    get_field = same('__getattr__')


@compile.when(PJMappedVirtualTable)
def compile_mapped_table(compile, expr, state):
    mt, st = expr._mapping.get_tables_objects()
    compile(expr._cr.TableJoin(mt).inner_join(st).on((mt.id == st.pid) & (mt.tid == st.tid)), state)


@compile.when(JsonbDataField)
def compile_jsonb_datafield(compile, expr, state):
    # import sqlbuilder.smartsql as ss
    # if len(state._stack) > 1:
    #     op = state._stack[1]
    # else:
    #     op = None
    # if isinstance(op, ss.Eq):
    #     pass
    # default
    if expr._prefix is not None:
        _, st = expr._prefix._mapping.get_tables_objects()
        compile(st.data.jsonb_item_text(expr._name), state)
    else:
        compile(Field('data').jsonb_item_text(expr._name), state)
