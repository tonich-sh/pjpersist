##############################################################################
#
# Copyright (c) 2011 Zope Foundation and Contributors.
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
"""PostGreSQL/JSONB Persistence Schema Fields"""
import zope.interface
import zope.schema._field
import zope.schema.interfaces
import pjpersist.serialize

class PJSequence(zope.schema._field.AbstractCollection):
    zope.interface.implements(zope.schema.interfaces.IList)
    _type = (tuple, list, pjpersist.serialize.PersistentList)

class PJMapping(zope.schema._field.Dict):
    zope.interface.implements(zope.schema.interfaces.IDict)
    _type = (dict, pjpersist.serialize.PersistentDict)
