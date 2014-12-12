##############################################################################
#
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
"""SQLBuilder extensions"""
import json

from sqlobject.sqlbuilder import *


########################################
## Postgres JSON operators
########################################

# Let's have them here and then see if they're worth contributing
# upstream when they settle down.

class PGArray(SQLExpression):
    """PostgreSQL array expression.

    The downside of current implementation is that the items must be
    the same type.  Literal arrays like '{1, 1.5, "two"}' can be
    non-homogenous, but need different quoting of strings.
    """

    def __init__(self, iterable):
        self.iterable = iterable

    def __sqlrepr__(self, db):
        assert db == 'postgres', "Postgres-specific feature, sorry."

        items = (sqlrepr(item, db) for item in self.iterable)
        return 'array[%s]' % ", ".join(items)


class TYPECAST(SQLExpression):
    """Cast to a type"""

    cast = None

    def __init__(self, arg, typ=None):
        self.arg = arg
        if typ is not None:
            self.cast = '::' + typ

    def __sqlrepr__(self, db):
        return sqlrepr(self.arg, db) + self.cast


class JSONB(TYPECAST):
    """Cast to JSONB"""
    cast = "::jsonb"


class JSON(TYPECAST):
    """Cast to JSON"""
    cast = '::json'


class TEXT(TYPECAST):
    """Cast to text"""
    cast = '::text'


def JSON_GETITEM(json, key):
    return SQLOp("->", json, key)

def JSON_GETITEM_TEXT(json, key):
    return SQLOp("->>", json, key)

def JSON_PATH(json, keys):
    """keys is an SQL array"""
    return SQLOp("#>", json, PGArray(keys))

def JSON_PATH_TEXT(json, keys):
    """keys is an SQL array"""
    return SQLOp("#>>", json, PGArray(keys))

def JSONB_SUPERSET(superset, subset):
    return SQLOp("@>", superset, subset)

def JSONB_SUBSET(subset, superset):
    return SQLOp("<@", subset, superset)

def JSONB_CONTAINS(jsonb, key):
    return SQLOp("?", jsonb, key)

def JSONB_CONTAINS_ANY(jsonb, keys):
    """keys is an SQL array"""
    return SQLOp("?|", jsonb, PGArray(keys))

def JSONB_CONTAINS_ALL(jsonb, keys):
    """keys is an SQL array"""
    return SQLOp("?&", jsonb, PGArray(keys))


class JGET(object):
    """JSON field getter that JSONifies the second argument of comparisons.

    Normally it just gets a JSON key of a table field:

       >>> print JGET("data", "key", table="Person").__sqlrepr__('postgres')
       ((Person.data) -> ('key'))

    We can also pass a field object and omit the table:

       >>> print JGET(Field("Person", "data"), "key").__sqlrepr__('postgres')
       ((Person.data) -> ('key'))

    The right operand for comparison operators gets converted to JSON:

       >>> print (JGET("data", "key", table="Person") == {'foo': 'bar'}
       ...     ).__sqlrepr__('postgres')
       (((Person.data) -> ('key')) = ('{"foo": "bar"}'::jsonb))

       >>> print (JGET("data", "key", table="Person") >= [True, False, None]
       ...     ).__sqlrepr__('postgres')
       (((Person.data) -> ('key')) >= ('[true, false, null]'::jsonb))

    But not always (is this a good idea?):
    (adamG: no it's not a good idea, because -> returns jsonb which is never NULL
    see doctest_datetime_range)

       >>> print (JGET("data", "key", table="Person") == None
       ...     ).__sqlrepr__('postgres')
       (((Person.data) -> ('key')) = ('null'::jsonb))

       >>> print (JGET("data", "key", table="Person") != None
       ...     ).__sqlrepr__('postgres')
       (((Person.data) -> ('key')) <> ('null'::jsonb))
    """

    def __init__(self, field, selector, table=None):
        if table is not None:
            self.field = Field(table, field)
        else:
            self.field = field
        self.selector = selector

    def __lt__(self, other):
        return SQLOp("<", self, JSONB(json.dumps(other)))
    def __le__(self, other):
        return SQLOp("<=", self, JSONB(json.dumps(other)))
    def __gt__(self, other):
        return SQLOp(">", self, JSONB(json.dumps(other)))
    def __ge__(self, other):
        return SQLOp(">=", self, JSONB(json.dumps(other)))
    def __eq__(self, other):
        return SQLOp("=", self, JSONB(json.dumps(other)))
    def __ne__(self, other):
        return SQLOp("<>", self, JSONB(json.dumps(other)))
    def __and__(self, other):
        return SQLOp("AND", self, JSONB(json.dumps(other)))
    def __rand__(self, other):
        return SQLOp("AND", JSONB(json.dumps(other), self))
    def __or__(self, other):
        return SQLOp("OR", self, JSONB(json.dumps(other)))
    def __ror__(self, other):
        return SQLOp("OR", JSONB(json.dumps(other), self))
    def __invert__(self):
        return SQLPrefix("NOT", self)

    def __sqlrepr__(self, db):
        expr = JSON_GETITEM(self.field, self.selector)
        return sqlrepr(expr, db)


class NoTables(SQLExpression):
    """A dirty hack that fools the tablesUsedSet detection"""
    def __init__(self, expr):
        self.expr = expr

    def __sqlrepr__(self, db):
        return sqlrepr(self.expr, db)

    def tablesUsedImmediate(self, db):
        return []

    def tablesUsedSet(self, db):
        return set()

    def tablesUsed(self, db):
        return {}
