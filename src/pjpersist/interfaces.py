##############################################################################
#
# Copyright (c) 2011 Zope Foundation and Contributors.
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
"""PG/JSONB Persistence Interfaces"""
from __future__ import absolute_import
import datetime
import decimal
import persistent.interfaces
import transaction.interfaces
import types
import zope.interface
import zope.schema

PJ_NATIVE_TYPES = (
    bool, int, long, float, unicode, types.NoneType)
REFERENCE_SAFE_TYPES = (
    datetime.datetime, datetime.date, datetime.time, decimal.Decimal)

DATABASE_ATTR_NAME = '_p_pj_database'
TABLE_ATTR_NAME = '_p_pj_table'
STORE_TYPE_ATTR_NAME = '_p_pj_store_type'
SUB_OBJECT_ATTR_NAME = '_p_pj_sub_object'
DOC_OBJECT_ATTR_NAME = '_p_pj_doc_object'
STATE_ATTR_NAME = '_p_pj_state'
PY_TYPE_ATTR_NAME = '_py_persistent_type'

class ConflictError(transaction.interfaces.TransientError):
    """An error raised when a write conflict is detected."""

    def __init__(self, message=None, object=None,
                 orig_state=None, cur_state=None, new_state=None):
        self.message = message or "database conflict error"
        self.object = object
        self.orig_state = orig_state
        self.cur_state = cur_state
        self.new_state = new_state

    def __str__(self):
        extras = [
            'oid %s' % self.object._p_oid if self.object else '',
            'class %s' % self.object.__class__.__name__ if self.object
            else '']
        return "%s (%s)" % (self.message, ", ".join(extras))

    def __unicode__(self):
        return unicode(self.__str__())

    def __repr__(self):
        return '%s: %s' % (self.__class__.__name__, self)


class CircularReferenceError(Exception):
    pass


class IObjectSerializer(zope.interface.Interface):
    """An object serializer allows for custom serialization output for
    objects."""

    def can_read(state):
        """Returns a boolean indicating whether this serializer can deserialize
        this state."""

    def get_object(state):
        """Convert the state to an object."""

    def can_write(obj):
        """Returns a boolean indicating whether this serializer can serialize
        this object."""

    def get_state(obj):
        """Convert the object to a state/document."""


class IObjectWriter(zope.interface.Interface):
    """The object writer stores an object in the database."""

    def get_non_persistent_state(obj, seen):
        """Convert a non-persistent object to a JSONB state/document."""

    def get_persistent_state(obj, seen):
        """Convert a persistent object to a JSONB state/document."""

    def get_state(obj, seen=None):
        """Convert an arbitrary object to a JSONB state/document.

        A ``CircularReferenceError`` is raised, if a non-persistent loop is
        detected.
        """

    def store(obj, id=None):
        """Store an object in the database with given id

        If id is not specified, unique one will be generated
        """


class IObjectReader(zope.interface.Interface):
    """The object reader reads an object from the database."""

    def resolve(path):
        """Resolve a path to a class.

        The path can be any string. It is the responsibility of the resolver
        to maintain the mapping from path to class.
        """

    def get_object(state, obj):
        """Get an object from the given state.

        The ``obj`` is the JSONB document of which the created object is part
        of.
        """

    def set_ghost_state(obj):
        """Convert a ghosted object to an active object by loading its state.
        """

    def get_ghost(coll_name, oid):
        """Get the ghosted version of the object.
        """


class IPJDataManager(persistent.interfaces.IPersistentDataManager):
    """A persistent data manager that stores data in PostGreSQL/JSONB."""

    root = zope.interface.Attribute(
        """Get the root object, which is a mapping.""")

    def create_tables(tables):
        """Create passed tables and persistence_name_map, use this instead
        of PJ_AUTO_CREATE_TABLES"""

    def get_table_of_object(obj):
        """Return the table name for an object."""

    def reset():
        """Reset the datamanager for the next transaction."""

    def dump(obj):
        """Store the object to PostGreSQL/JSONB and return its DBRef."""

    def load(dbref):
        """Load the object from PostGreSQL/JSONB by using its DBRef.

        Note: The returned object is in the ghost state.
        """

    def flush():
        """Flush all changes to PostGreSQL."""

    def insert(obj, id=None):
        """Insert an object into PostGreSQL.

        The correct collection is determined by object type.

        If `id` is provided, object will be inserted under that id. Otherwise,
        new unique id will be generated.
        """

    def remove(obj):
        """Remove an object from PostGreSQL.

        The correct collection is determined by object type.
        """

class IPJDataManagerProvider(zope.interface.Interface):
    """Utility to get a PJ data manager.

    Implementations of this utility ususally maintain connection information
    and ensure that there is one consistent datamanager per thread.
    """

    def get(database):
        """Return a PJ data manager for the given database."""
