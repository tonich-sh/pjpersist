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
"""Persistent Object Support"""
from __future__ import absolute_import
import persistent
import zope.interface

from pjpersist import interfaces

class PersistentSerializationHooks(persistent.Persistent):
    zope.interface.implements(interfaces.IPersistentSerializationHooks)

    def _pj_after_store_hook(self, conn):
        return None

    def _pj_after_load_hook(self, conn):
        return None


class SimpleColumnSerialization(object):
    zope.interface.implements(interfaces.IColumnSerialization)

    _pj_column_fields = ()

    def _pj_get_column_fields(self):
        return {
            field.__name__: getattr(self, field.__name__)
            for field in self._pj_column_fields}


def select_fields(schema, *fieldnames):
    return tuple(schema[fname] for fname in fieldnames)
