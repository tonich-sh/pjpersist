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

            if isinstance(value, dict):
                comparison, val = value.items()[0]
                if comparison == '$gt':
                    clauses.append(accessor > val)
                if comparison == '$lt':
                    clauses.append(accessor < val)
                if comparison == '$gte':
                    clauses.append(accessor >= val)
                if comparison == '$lte':
                    clauses.append(accessor <= val)
                if comparison == '$ne':
                    clauses.append(accessor != val)
                if comparison == '$in':
                    clauses.append(sb.IN(accessor, val))
                if comparison == '$nin':
                    clauses.append(sb.NOT(sb.IN(accessor, val)))
            else:
                # Scalar -- equality
                clauses.append(accessor == value)
        return sb.AND(*clauses)
