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

from pjpersist import sqlbuilder as sb


class Converter(object):

    simplified = False

    def __init__(self, table, field):
        self.table = table
        self.field = field

    def convert(self, query):
        clauses = []
        doc = sb.Field(self.table, self.field)
        for key, value in sorted(query.items()):
            if '.' not in key:
                accessor = sb.JSON_GETITEM_TEXT(doc, key)
            else:
                accessor = sb.JSON_PATH_TEXT(doc, key.split("."))

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
                if len(value) != 1:
                    raise ValueError("Too many elements: %r" % value)
                operator, operand = value.items()[0]
                clauses.append(self.operator_expr(operator, accessor, operand))
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

    def operator_expr(self, operator, op1, op2):
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
            return sb.NOT(self.operator_expr(operator2, op1, op3))
        else:
            raise ValueError("Unrecognized operator %s" % operator)
