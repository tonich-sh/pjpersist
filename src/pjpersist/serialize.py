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
"""Object Serialization for PostGreSQL's JSONB"""
from __future__ import absolute_import
import uuid
import copy_reg
import datetime

import persistent.interfaces
import persistent.dict
import persistent.list
import types
import zope.interface
from repoze.lru import LRUCache

from zope.dottedname.resolve import resolve
from decimal import Decimal

from pjpersist import interfaces

ALWAYS_READ_FULL_DOC = True

SERIALIZERS = []
AVAILABLE_NAME_MAPPINGS = set()
PATH_RESOLVE_CACHE = {}
TABLE_KLASS_MAP = {}
DBREF_RESOLVE_CACHE = LRUCache(500)

# actually we should extract this somehow from psycopg2
PYTHON_TO_PG_TYPES = {
    unicode: "text",
    str: "text",
    bool: "bool",
    float: "double",
    int: "integer",
    long: "bigint",
    (int, long): "bigint",
    Decimal: "numeric",
    datetime.date: "date",
    datetime.time: "time",
    datetime.datetime: "timestamptz",
    datetime.timedelta: "interval",
    list: "array",
    uuid.UUID: "UUID",
}


def get_dotted_name(obj, escape=False, state=False):
    name = obj.__module__ + '.' + obj.__name__
    if not escape:
        return name
    # Make the name safe.
    name = name.replace('.', '_dot_')
    # start with _.
    name = 'u'+name if name.startswith('_') else name
    if state:
        name += '_state'
    return name


class PersistentDict(persistent.dict.PersistentDict):
    _p_pj_sub_object = True

    def __init__(self, data=None, **kwargs):
        # We optimize the case where data is not a dict. The original
        # implementation always created an empty dict, which it then
        # updated. This turned out to be expensive.
        if data is None:
            self.data = {}
        elif isinstance(data, dict):
            self.data = data.copy()
        else:
            self.data = dict(data)
        if len(kwargs):
            self.update(kwargs)

    def __getitem__(self, key):
        # The UserDict supports a __missing__() function, which I have never
        # seen or used before, but it makes the method significantly
        # slower. So let's not do that.
        return self.data[key]

    def __eq__(self, other):
        return self.data == other

    def __ne__(self, other):
        return not self.__eq__(other)


class PersistentList(persistent.list.PersistentList):
    _p_pj_sub_object = True


class DBRef(object):

    def __init__(self, table, id, database=None):
        self._table = table
        self._id = id
        self._database = database
        self.__calculate_hash()

    def __calculate_hash(self):
        self.hash = hash(str(self.database)+str(self.table)+str(self.id))

    @property
    def database(self):
        return self._database

    @database.setter
    def database(self, value):
        self._database = value
        self.__calculate_hash()

    @property
    def table(self):
        return self._table

    @table.setter
    def table(self, value):
        self._table = value
        self.__calculate_hash()

    @property
    def id(self):
        return self._id

    @id.setter
    def id(self, value):
        self._id = value
        self.__calculate_hash()

    def __setstate__(self, state):
        self.__init__(state['table'], state['id'], state['database'])

    def __getstate__(self):
        return {'database': self.database,
                'table': self.table,
                'id': self.id}

    def __hash__(self):
        return self.hash

    def __eq__(self, other):
        if other is None:
            return False
        return self.hash == other.hash

    def __neq__(self, other):
        return self.hash != other.hash

    def __repr__(self):
        return 'DBRef(%r, %r, %r)' % (self.table, self.id, self.database)

    def as_key(self):
        return '%r::%r::%r' % (self.database, self.table, self.id)

    def as_tuple(self):
        return self.database, self.table, self.id

    def as_json(self):
        return {'_py_type': 'DBREF',
                'database': self.database,
                'table': self.table,
                'id': self.id}


class Binary(str):
    pass


class ObjectSerializer(object):
    zope.interface.implements(interfaces.IObjectSerializer)

    def can_read(self, state):
        raise NotImplementedError

    def read(self, state):
        raise NotImplementedError

    def can_write(self, obj):
        raise NotImplementedError

    def write(self, obj):
        raise NotImplementedError


class ObjectWriter(object):
    zope.interface.implements(interfaces.IObjectWriter)

    def __init__(self, jar):
        self._jar = jar

    def get_table_name(self, obj):
        db_name = getattr(
            obj, interfaces.DATABASE_ATTR_NAME,
            self._jar.database if self._jar else None)
        try:
            table_name = getattr(obj, interfaces.TABLE_ATTR_NAME)
        except AttributeError:
            return db_name, get_dotted_name(obj.__class__, True)
        return db_name, table_name

    def get_non_persistent_state(self, obj, seen):
        # XXX: Look at the pickle library how to properly handle all types and
        # old-style classes with all of the possible pickle extensions.

        # Only non-persistent, custom objects can produce unresolvable
        # circular references.
        if id(obj) in seen:
            raise interfaces.CircularReferenceError(obj)
        # Add the current object to the list of seen objects.
        if not (type(obj) in interfaces.REFERENCE_SAFE_TYPES or
                getattr(obj, '_pj_reference_safe', False)):
            seen.append(id(obj))
        # Get the state of the object. Only pickable objects can be reduced.
        reduce_fn = copy_reg.dispatch_table.get(type(obj))
        if reduce_fn is not None:
            reduced = reduce_fn(obj)
        else:
            # XXX: __reduce_ex__
            reduced = obj.__reduce__()
        # The full object state (item 3) seems to be optional, so let's make
        # sure we handle that case gracefully.
        if isinstance(reduced, str):
            # When the reduced state is just a string it represents a name in
            # a module. The module will be extrated from __module__.
            return {'_py_constant': obj.__module__+'.'+reduced}
        if len(reduced) == 2:
            factory, args = reduced
            obj_state = {}
        else:
            factory, args, obj_state = reduced
            if obj_state is None:
                obj_state = {}
        # We are trying very hard to create a clean JSONB (sub-)document. But
        # we need a little bit of meta-data to help us out later.
        if factory == copy_reg._reconstructor and \
               args == (obj.__class__, object, None):
            # This is the simple case, which means we can produce a nicer
            # JSONB output.
            state = {'_py_type': get_dotted_name(args[0])}
        elif factory == copy_reg.__newobj__ and args == (obj.__class__,):
            # Another simple case for persistent objects that do not want
            # their own document.
            state = {interfaces.PY_TYPE_ATTR_NAME: get_dotted_name(args[0])}
        else:
            state = {'_py_factory': get_dotted_name(factory),
                     '_py_factory_args': self.get_state(args, obj, seen)}
        for name, value in obj_state.items():
            state[name] = self.get_state(value, obj, seen)
        return state

    def get_persistent_state(self, obj, seen):
        # Persistent sub-objects are stored by reference, the key being
        # (table name, oid).
        # Getting the table name is easy, but if we have an unsaved
        # persistent object, we do not yet have an OID. This must be solved by
        # storing the persistent object.
        if obj._p_oid is None:
            dbref = self.store(obj, ref_only=True)
        else:
            db_name, table_name = self.get_table_name(obj)
            dbref = obj._p_oid
        # Create the reference sub-document. The _p_type value helps with the
        # deserialization later.
        return dbref.as_json()

    def get_state(self, obj, pobj=None, seen=None):
        seen = seen or []
        if type(obj) in interfaces.PJ_NATIVE_TYPES:
            # If we have a native type, we'll just use it as the state.
            return obj
        if isinstance(obj, str):
            # In Python 2, strings can be ASCII, encoded unicode or binary
            # data. Unfortunately, BSON cannot handle that. So, if we have a
            # string that cannot be UTF-8 decoded (luckily ASCII is a valid
            # subset of UTF-8), then we use the BSON binary type.
            try:
                obj.decode('utf-8')
                return obj
            except UnicodeError:
                return {'_py_type': 'BINARY', 'data': obj.encode('base64')}

        # Some objects might not naturally serialize well and create a very
        # ugly JSONB entry. Thus, we allow custom serializers to be
        # registered, which can encode/decode different types of objects.
        for serializer in SERIALIZERS:
            if serializer.can_write(obj):
                return serializer.write(obj)

        if isinstance(obj, (type, types.ClassType)):
            # We frequently store class and function paths as meta-data, so we
            # need to be able to properly encode those.
            return {'_py_type': 'type',
                    'path': get_dotted_name(obj)}

        # We need to make sure that the object's jar and doc-object are
        # set. This is important for the case when a sub-object was just
        # added.
        if getattr(obj, interfaces.SUB_OBJECT_ATTR_NAME, False):
            if obj._p_jar is None:
                if pobj is not None and \
                        getattr(pobj, '_p_jar', None) is not None:
                    obj._p_jar = pobj._p_jar
                setattr(obj, interfaces.DOC_OBJECT_ATTR_NAME, pobj)

        if isinstance(obj, (tuple, list, PersistentList)):
            # Make sure that all values within a list are serialized
            # correctly. Also convert any sequence-type to a simple list.
            return [self.get_state(value, pobj, seen) for value in obj]
        if isinstance(obj, (dict, PersistentDict)):
            # Same as for sequences, make sure that the contained values are
            # properly serialized.
            # Note: A big constraint in JSONB is that keys must be strings!
            has_non_string_key = False
            data = []
            for key, value in obj.items():
                data.append((key, self.get_state(value, pobj, seen)))
                has_non_string_key |= not isinstance(key, basestring)
                if (not isinstance(key, basestring) or '\0' in key):
                    has_non_string_key = True
            if not has_non_string_key:
                # The easy case: all keys are strings:
                return dict(data)
            else:
                # We first need to reduce the keys and then produce a data
                # structure.
                data = [(self.get_state(key, pobj), value)
                        for key, value in data]
                return {'dict_data': data}

        if isinstance(obj, persistent.Persistent):
            # Only create a persistent reference, if the object does not want
            # to be a sub-document.
            if not getattr(obj, interfaces.SUB_OBJECT_ATTR_NAME, False):
                return self.get_persistent_state(obj, seen)
            # This persistent object is a sub-document, so it is treated like
            # a non-persistent object.

        return self.get_non_persistent_state(obj, seen)

    def get_full_state(self, obj):
        doc = self.get_state(obj.__getstate__(), obj)
        # Always add a persistent type info
        doc[interfaces.PY_TYPE_ATTR_NAME] = get_dotted_name(obj.__class__)
        # Return the full state document
        return doc

    def store(self, obj, ref_only=False, id=None):
        # If it is the first time that this type of object is stored, getting
        # the table name has the side affect of telling the class whether it
        # has to store its Python type as well. So, do not remove, even if the
        # data is not used right away,
        db_name, table_name = self.get_table_name(obj)

        if ref_only:
            # We only want to get OID quickly. Trying to reduce the full state
            # might cause infinite recursion loop. (Example: 2 new objects
            # reference each other.)
            doc = {}
            # Make sure that the object gets saved fully later.
            self._jar.register(obj)

            #doc_id = self._jar.createId()
            #oid = DBRef(table_name, doc_id, db_name)
            #return oid
        else:
            # XXX: Handle newargs; see ZODB.serialize.ObjectWriter.serialize
            # Go through each attribute and search for persistent references.
            doc = self.get_state(obj.__getstate__(), obj)

        # Always add a persistent type info
        py_type_attr_name = get_dotted_name(obj.__class__)
        doc[interfaces.PY_TYPE_ATTR_NAME] = py_type_attr_name

        stored = False
        if interfaces.IColumnSerialization.providedBy(obj):
            self._jar._ensure_sql_columns(obj, table_name)
            column_data = obj._pj_get_column_fields()
        else:
            column_data = None
        if obj._p_oid is None:
            doc_id = self._jar._insert_doc(
                db_name, table_name, doc, id, column_data)
            stored = True
            obj._p_jar = self._jar
            obj._p_oid = DBRef(table_name, doc_id, db_name)
        else:
            self._jar._update_doc(
                db_name, table_name, doc, obj._p_oid.id, column_data)
            stored = True
        # let's call the hook here, to always have _p_jar and _p_oid set
        if interfaces.IPersistentSerializationHooks.providedBy(obj):
            obj._pj_after_store_hook(self._jar._conn)

        if stored:
            doc[interfaces.PY_TYPE_ATTR_NAME] = py_type_attr_name
        DBREF_RESOLVE_CACHE.put(obj._p_oid.as_key(), obj.__class__)
        return obj._p_oid


class ObjectReader(object):
    zope.interface.implements(interfaces.IObjectReader)

    def __init__(self, jar):
        self._jar = jar
        self.preferPersistent = True

    def simple_resolve(self, path):
        path = path.replace('_dot_', '.')
        path = path[1:] if path.startswith('u_') else path
        # We try to look up the klass from a cache. The important part here is
        # that we also cache lookup failures as None, since they actually
        # happen more frequently than a hit due to an optimization in the
        # resolve() function.
        try:
            klass = PATH_RESOLVE_CACHE[path]
        except KeyError:
            try:
                klass = resolve(path)
            except ImportError:
                PATH_RESOLVE_CACHE[path] = klass = None
            else:
                PATH_RESOLVE_CACHE[path] = klass
        if klass is None:
            raise ImportError(path)
        return klass

    def resolve(self, dbref, no_cache=False):

        # 0. Use DBREF_RESOLVE_CACHE
        if not no_cache:
            klass = DBREF_RESOLVE_CACHE.get(dbref.as_key())
            if klass is not None:
                return klass

        # 1. Try to optimize on whether there's just one class stored in one
        #    table, that can save us one DB query
        if dbref.table in TABLE_KLASS_MAP:
            results = TABLE_KLASS_MAP[dbref.table]
            if len(results) == 1:
                # there must be just ONE, otherwise we need to check the JSONB
                klass = list(results)[0]
                DBREF_RESOLVE_CACHE.put(dbref.as_key(), klass)
                return klass

        # from this point on we need the dbref.id
        if dbref.id is None:
            raise ImportError(dbref)
        # 2. Get the class from the object state
        #    Multiple object types are stored in the table. We have to
        #    look at the object (JSONB) to find out the type.
        if ALWAYS_READ_FULL_DOC:
            # Optimization: Read the entire doc and stick it in the right
            # place so that unghostifying the object later will not cause
            # another database access.
            obj_doc = self._jar._get_doc_by_dbref(dbref)
        else:
            # Just read the type from the database, still requires one query
            pytype = self._jar._get_doc_py_type(
                dbref.database, dbref.table, dbref.id)
            obj_doc = {interfaces.PY_TYPE_ATTR_NAME: pytype}
        if obj_doc is None:
            # There is no document for this reference in the database.
            raise ImportError(dbref)
        if interfaces.PY_TYPE_ATTR_NAME in obj_doc:
            # We have always the path to the class in JSONB
            klass = self.simple_resolve(obj_doc[interfaces.PY_TYPE_ATTR_NAME])
        else:
            raise ImportError(dbref)
        DBREF_RESOLVE_CACHE.put(dbref.as_key(), klass)
        return klass

    def get_non_persistent_object(self, state, obj):
        if '_py_constant' in state:
            return self.simple_resolve(state['_py_constant'])

        # this method must NOT change the passed in state dict
        state = dict(state)
        if '_py_type' in state:
            # Handle the simplified case.
            klass = self.simple_resolve(state.pop('_py_type'))
            sub_obj = copy_reg._reconstructor(klass, object, None)
        elif interfaces.PY_TYPE_ATTR_NAME in state:
            # Another simple case for persistent objects that do not want
            # their own document.
            klass = self.simple_resolve(state.pop(interfaces.PY_TYPE_ATTR_NAME))
            sub_obj = copy_reg.__newobj__(klass)
        else:
            factory = self.simple_resolve(state.pop('_py_factory'))
            factory_args = self.get_object(state.pop('_py_factory_args'), obj)
            sub_obj = factory(*factory_args)
        if len(state):
            sub_obj_state = self.get_object(state, sub_obj)
            if hasattr(sub_obj, '__setstate__'):
                sub_obj.__setstate__(dict(sub_obj_state))
            else:
                sub_obj.__dict__.update(sub_obj_state)
            if isinstance(sub_obj, persistent.Persistent):
                # This is a persistent sub-object -- mark it as such. Otherwise
                # we risk to store this object in its own table next time.
                setattr(sub_obj, interfaces.SUB_OBJECT_ATTR_NAME, True)
        if getattr(sub_obj, interfaces.SUB_OBJECT_ATTR_NAME, False):
            setattr(sub_obj, interfaces.DOC_OBJECT_ATTR_NAME, obj)
            sub_obj._p_jar = self._jar
        return sub_obj

    def get_object(self, state, obj):
        if isinstance(state, dict) and state.get('_py_type') == 'BINARY':
            # Binary data in Python 2 is presented as a string. We will
            # convert back to binary when serializing again.
            return state['data'].decode('base64')
        if isinstance(state, dict) and state.get('_py_type') == 'DBREF':
            # Load a persistent object. Using the _jar.load() method to make
            # sure we're loading from right database and caching is properly
            # applied.
            dbref = DBRef(state['table'], state['id'], state['database'])
            return self._jar.load(dbref)
        if isinstance(state, dict) and state.get('_py_type') == 'type':
            # Convert a simple object reference, mostly classes.
            return self.simple_resolve(state['path'])

        # Give the custom serializers a chance to weigh in.
        for serializer in SERIALIZERS:
            if serializer.can_read(state):
                return serializer.read(state)

        if isinstance(state, dict) and (
            '_py_factory' in state
            or '_py_constant' in state
            or '_py_type' in state
            or interfaces.PY_TYPE_ATTR_NAME in state):
            # Load a non-persistent object.
            return self.get_non_persistent_object(state, obj)
        if isinstance(state, (tuple, list)):
            # All lists are converted to persistent lists, so that their state
            # changes are noticed. Also make sure that all value states are
            # converted to objects.
            sub_obj = [self.get_object(value, obj) for value in state]
            if self.preferPersistent:
                sub_obj = PersistentList(sub_obj)
                setattr(sub_obj, interfaces.DOC_OBJECT_ATTR_NAME, obj)
                sub_obj._p_jar = self._jar
            return sub_obj
        if isinstance(state, dict):
            # All dictionaries are converted to persistent dictionaries, so
            # that state changes are detected. Also convert all value states
            # to objects.
            # Handle non-string key dicts.
            if 'dict_data' in state:
                items = state['dict_data']
            else:
                items = state.items()
            # sub_obj_list = list()
            # for name, value in items:
            #     obj_name = self.get_object(name, obj)
            #     obj_value = self.get_object(value, obj)
            #     sub_obj_list.append((obj_name, obj_value))
            # sub_obj = dict(sub_obj_list)
            sub_obj = dict(
                [(self.get_object(name, obj), self.get_object(value, obj))
                 for name, value in items])
            if self.preferPersistent:
                sub_obj = PersistentDict(sub_obj)
                setattr(sub_obj, interfaces.DOC_OBJECT_ATTR_NAME, obj)
                sub_obj._p_jar = self._jar
            return sub_obj
        return state

    def set_ghost_state(self, obj, doc=None):
        # Check whether the object state was stored on the object itself.
        if doc is None:
            doc = getattr(obj, interfaces.STATE_ATTR_NAME, None)
        # Look up the object state by table_name and oid.
        if doc is None:
            doc = self._jar._get_doc_by_dbref(obj._p_oid)
        # Check that we really have a state doc now.
        if doc is None:
            raise ImportError(obj._p_oid)
        # Remove unwanted attributes.
        pytype = doc.pop(interfaces.PY_TYPE_ATTR_NAME)

        # Now convert the document to a proper Python state dict.
        state = dict(self.get_object(doc, obj))

        # Sometimes this method is called to update the object state
        # before storage.
        doc[interfaces.PY_TYPE_ATTR_NAME] = pytype

        # Set the state.
        obj.__setstate__(state)
        # Run the custom load functions.
        if interfaces.IPersistentSerializationHooks.providedBy(obj):
            obj._pj_after_load_hook(self._jar._conn)

    def get_ghost(self, dbref, klass=None):
        if klass is None:
            klass = self.resolve(dbref)
        obj = klass.__new__(klass)
        obj._p_jar = self._jar
        obj._p_oid = dbref
        del obj._p_changed
        # Assign the table after deleting _p_changed, since the attribute
        # is otherwise deleted.
        setattr(obj, interfaces.DATABASE_ATTR_NAME, dbref.database)
        setattr(obj, interfaces.TABLE_ATTR_NAME, dbref.table)
        return obj

    def load(self, data, table, _id, database=None):
        pytype = data['data'].get(interfaces.PY_TYPE_ATTR_NAME, None)
        if pytype is None:
            pytype = data['package'] + '.' + data['class_name']
            data['data'][interfaces.PY_TYPE_ATTR_NAME] = pytype
        klass = self.simple_resolve(pytype)
        obj = klass.__new__(klass)
        obj._p_jar = self._jar
        if database is None:
            database = self._jar.database
        dbref = DBRef(table, _id, database)
        obj._p_oid = dbref
        del obj._p_changed
        # Assign the table after deleting _p_changed, since the attribute
        # is otherwise deleted.
        setattr(obj, interfaces.DATABASE_ATTR_NAME, dbref.database)
        setattr(obj, interfaces.TABLE_ATTR_NAME, dbref.table)
        self.set_ghost_state(obj, data['data'])
        return obj


class table:
    """Declare the table used by the class.

    sets also the atrtibute interfaces.TABLE_ATTR_NAME
    but register the fact also in TABLE_KLASS_MAP, this will allow pjpersist
    to optimize class lookup when just one class is stored in one table
    otherwise class lookup always needs the JSONB data from PG
    """

    def __init__(self, table_name):
        self.table_name = table_name

    def __call__(self, ob):
        try:
            setattr(ob, interfaces.TABLE_ATTR_NAME, self.table_name)
            TABLE_KLASS_MAP.setdefault(self.table_name, set()).add(ob)
        except AttributeError:
            raise TypeError(
                "Can't declare %s" % interfaces.TABLE_ATTR_NAME, ob)
        return ob
