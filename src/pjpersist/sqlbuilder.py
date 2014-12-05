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
from sqlobject.sqlbuilder import *


########################################
## Postgres JSON operators
########################################

# Let's have them here and then see if they're worth contributing
# upstream when they settle down.

def JSON_GETITEM(json, key):
    return SQLOp("->", json, key)

def JSON_GETITEM_TEXT(json, key):
    return SQLOp("->>", json, key)

def JSON_PATH(json, keys):
    """keys is an SQL array"""
    return SQLOp("#>", json, key)

def JSON_PATH_TEXT(json, keys):
    """keys is an SQL array"""
    return SQLOp("#>>", json, key)

def JSONB_SUPERSET(superset, subset):
    return SQLOp("@>", superset, subset)

def JSONB_SUBSET(subset, superset):
    return SQLOp("<@", subset, superset)

def JSONB_CONTAINS(jsonb, key):
    return SQLOp("?", jsonb, key)

def JSONB_CONTAINS_ANY(jsonb, keys):
    """keys is an SQL array"""
    return SQLOp("?|", jsonb, keys)

def JSONB_CONTAINS_ALL(jsonb, keys):
    """keys is an SQL array"""
    return SQLOp("?&", jsonb, keys)



