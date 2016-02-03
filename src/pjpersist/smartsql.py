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

from sqlbuilder.smartsql import Q, T, compile as parent_comile, Expr, NamedCondition, NamedCallable, \
    PLACEHOLDER, Name, Result, MetaTable, MetaField, FieldProxy, cr, same, \
    LOOKUP_SEP, Field, string_types, Comparable

compile = parent_comile.create_child()


class JsonbOp(NamedCondition):
    __slots__ = ()


class JsonbSuperset(JsonbOp):
    __slots__ = ()
    _sql = '@>'


class JsonbContainsAll(JsonbOp):
    __slots__ = ()
    _sql = '?&'


class JsonOperator(Expr):
    __slots__ = ('_left', '_right')

    def __init__(self, left, right):
        self._left = left
        self._right = right


class JsonItemText(JsonOperator):
    __slots__ = ()
    _sql = '->>'


class JsonPathText(JsonOperator):
    __slots__ = ()
    _sql = '#>>'


class PGRegexp(NamedCondition):
    __slots__ = ()
    _sql = '~'


class PGRegexpCI(NamedCondition):
    __slots__ = ()
    _sql = '~*'


class PGRegexpNot(NamedCondition):
    __slots__ = ()
    _sql = '!~'


class PGRegexpNotCI(NamedCondition):
    __slots__ = ()
    _sql = '!~*'


class PGCast(Expr):
    __slots__ = ('_left', '_right')

    def __init__(self, left, right):
        self._left = left
        self._right = right


class Random(NamedCallable):
    __slots__ = ()
    _sql = 'random'


def jsonb_superset(inst, other):
    return JsonbSuperset(inst, other)


def jsonb_contains_all(inst, other):
    return JsonbContainsAll(inst, other)


def jsonb_item_text(inst, other):
    return JsonItemText(inst, other)


def jsonb_path_text(inst, other):
    return JsonPathText(inst, other)


def pg_cast(inst, cast_to):
    return PGCast(inst, cast_to)


def pg_regexp(inst, other):
    return PGRegexp(inst, other)


def pg_regexp_ci(inst, other):
    return PGRegexpCI(inst, other)


def pg_not_regexp(inst, other):
    return PGRegexpNot(inst, other)


def pg_not_regexp_ci(inst, other):
    return PGRegexpNotCI(inst, other)


@compile.when(PGCast)
def compile_pg_cast(compile, expr, state):
    state.sql.append("cast(")
    compile(expr._left, state)
    state.sql.append(" as %s)" % expr._right)


setattr(Expr, 'jsonb_superset', jsonb_superset)
setattr(Expr, 'jsonb_contains_all', jsonb_contains_all)
setattr(Expr, 'jsonb_item_text', jsonb_item_text)
setattr(Expr, 'jsonb_path_text', jsonb_path_text)
setattr(Expr, 'cast', pg_cast)
setattr(Expr, 'regexp', pg_regexp)
setattr(Expr, 'not_regexp', pg_not_regexp)
setattr(Expr, 'regexp_ci', pg_regexp_ci)
setattr(Expr, 'not_regexp_ci', pg_not_regexp_ci)


class JsonArray(Comparable):

    __slots__ = ('_value', )

    def __init__(self, value):
        self._value = value


@compile.when(JsonOperator)
def compile_json_array(compile, expr, state):
    compile(expr._left, state)
    state.sql.append(expr._sql)
    if isinstance(expr._right, string_types):
        state.sql.append("'")
    if isinstance(expr._right, string_types):
        state.sql.append(expr._right)
    else:
        state.sql.append(str(expr._right))
    if isinstance(expr._right, string_types):
        state.sql.append("'")


@compile.when(JsonPathText)
def compile_json_path_text(compile, expr, state):
    compile(expr._left, state)
    state.sql.append(expr._sql)
    state.sql.append("'{")
    state.sql.append(', '.join(expr._right.split(LOOKUP_SEP)))
    state.sql.append("}'")


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
        for row in self._cur:
            yield self.unserialize(row)

    def __len__(self):
        if self._cur is None:
            return 0
        else:
            return self._cur.rowcount


class PJDateTime(Comparable):
    __slots__ = ('_value', )

    def __init__(self, value):
        self._value = value


@compile.when(PJDateTime)
def compile_pj_datetime(compile, expr, state):
    # TODO: possibility of change function name (constant, configuration)
    state.sql.append('to_timestamp_cast(')
    compile(JsonPathText(expr._value, expr._value._name + '__value'), state)
    state.sql.append(')')


class PJBool(Comparable):
    __slots__ = ('_value', )

    def __init__(self, value):
        self._value = value


@compile.when(PJBool)
def compile_pj_datetime(compile, expr, state):
    # TODO: possibility of change function name (constant, configuration)
    state.sql.append('to_bool_cast(')
    compile(expr._value, state)
    state.sql.append(')')


class JsonbDataField(MetaField("NewBase", (Expr,), {})):

    __slots__ = ('_name', '_prefix', '__cached__')

    def __init__(self, name, prefix):
        self._name = name
        self._prefix = prefix
        self.__cached__ = {}

    def as_datetime(self):
        return PJDateTime(self)

    def as_bool(self):
        return PJBool(self)


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
    mt = expr._mapping.get_table_object(ttype='mt')
    st = expr._mapping.get_table_object(ttype='st')
    compile(expr._cr.TableJoin(mt).inner_join(st).on((mt.id == st.pid) & (mt.tid == st.tid)), state)


@compile.when(JsonbDataField)
def compile_jsonb_datafield(compile, expr, state):
    # import sqlbuilder.smartsql as ss
    if len(state.callers) > 1:
        op = state.callers[1]
    else:
        op = None

    st = expr._prefix._mapping.get_table_object(ttype='st')

    if op and issubclass(op, JsonPathText):
        compile(st.data, state)
        return

    if op and issubclass(op, JsonbOp):
        compile(st.data, state)
        return

    # default
    compile(st.data.jsonb_item_text(expr._name), state)
