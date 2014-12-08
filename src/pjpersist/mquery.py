##############################################################################
#
# Copyright (c) 2014 Zope Foundation and Contributors.
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
"""Mongo-like queries for PJ"""

import json
from pjpersist import sqlbuilder as sb


class Converter(object):
    """Translator for MongoDB queries -> sqlbuilder expressions.

    The resultant expressions can be used as WHERE clauses in queries.

    This implements only a subset of the query language.
    """

    simplified = False

    def __init__(self, table, field):
        self.table = table
        self.field = field

    def convert(self, query):
        clauses = []
        doc = sb.Field(self.table, self.field)
        for key, value in sorted(query.items()):
            accessor = self.getField(doc, key)

            if key in ('$and', '$or', '$nor'):
                if not isinstance(value, (list, tuple)):
                    raise ValueError("Argument must be a list: %r" % value)
                oper = {
                    '$and': sb.AND,
                    '$or': sb.OR,
                    '$nor': lambda *items: sb.NOT(sb.OR(*items))
                }[key]
                clauses.append(oper(*(self.convert(expr) for expr in value)))
            elif isinstance(value, dict):
                for operator, operand in value.items():
                    clauses.append(
                        self.operator_expr(
                            operator, doc, key, operand))
            else:
                # Scalar -- equality or array membership
                if self.simplified:
                    # Let's ignore the membership case for test clarity
                    clauses.append(accessor == value)
                else:
                    clauses.append(sb.OR(
                        accessor == value,
                        sb.AND(
                            sb.JSONB_SUBSET(sb.JSONB('[]'), accessor),
                            sb.JSONB_CONTAINS(accessor, value)
                        )
                    ))
        return sb.AND(*clauses)

    def getField(self, field, key, json=False):
        if json:
            if '.' not in key:
                accessor = sb.JSON_GETITEM(field, key)
            else:
                accessor = sb.JSON_PATH(field, key.split("."))
        else:
            if '.' not in key:
                accessor = sb.JSON_GETITEM_TEXT(field, key)
            else:
                accessor = sb.JSON_PATH_TEXT(field, key.split("."))

        return accessor

    def operator_expr(self, operator, field, key, op2):
        op1 = self.getField(field, key)
        if operator == '$gt':
            return op1 > op2
        if operator == '$lt':
            return op1 < op2
        if operator == '$gte':
            return op1 >= op2
        if operator == '$lte':
            return op1 <= op2
        if operator == '$ne':
            return op1 != op2
        if operator == '$in':
            return sb.IN(op1, op2)
        if operator == '$nin':
            return sb.NOT(sb.IN(op1, op2))
        if operator == '$not':
            # MongoDB's rationalization for this operator:
            # it matches when op1 does not pass the condition
            # or when op1 is not set at all.
            operator2, op3 = op2.items()[0]
            return sb.OR(
                sb.ISNULL(op1),
                sb.NOT(sb.AND(
                    *(self.operator_expr(operator2, field, key, op3)
                      for operator2, op3 in op2.items())
                )))
        if operator == '$size':
            op1 = self.getField(field, key, json=True)
            return sb.func.json_array_length(op1) == op2
        if operator == '$exists':
            op1 = self.getField(field, key, json=True)
            return sb.ISNOTNULL(op1) if op2 else sb.ISNULL(op1)
        if operator == '$all':
            op1 = self.getField(field, key, json=True)
            return sb.JSONB_SUPERSET(op1, json.dumps(op2))
        if operator == '$elemMatch':
            op1 = sb.NoTables(self.getField(field, key, json=True))
            # SELECT data FROM tbl WHERE EXISTS (
            #          SELECT value
            #          FROM jsonb_array_elements(data -> 'arr')
            #          WHERE value < '3' AND value >= '2'
            # );
            return sb.EXISTS(
                sb.Select(
                    ['values'],
                    staticTables=[sb.func.jsonb_array_elements(op1)],
                    where=sb.NoTables(sb.AND(*(
                        sb.AND(*(
                            self.operator_expr(operator2, field, key, op3)
                            for operator2, op3 in query.items()
                        ))
                        for query in op2
                    )))
                )
            )
        else:
            raise ValueError("Unrecognized operator %s" % operator)
