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

from . import interfaces
from . import broken

ALWAYS_READ_FULL_DOC = True

SERIALIZERS = []
AVAILABLE_NAME_MAPPINGS = set()
PATH_RESOLVE_CACHE = {}
TABLE_KLASS_MAP = {}
DBREF_RESOLVE_CACHE = LRUCache(500)

FMT_DATE = "%Y-%m-%d"
FMT_TIME = "%H:%M:%S"
FMT_DATETIME = "%Y-%m-%dT%H:%M:%S"

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
        """
        :type jar: pjpersist.datamanager.PJDatamanager
        :return:
        """
        self._jar = jar

    def get_table_name(self, obj):
        db_name = getattr(
            obj, interfaces.ATTR_NAME_DATABASE,
            None)

        if db_name is None:
            db_name = self._jar.database if self._jar else None
        try:
            table_name = getattr(obj, interfaces.ATTR_NAME_TABLE)
        except AttributeError:
            table_name = None

        if table_name is None:
            table_name = get_dotted_name(obj.__class__, True)

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
            state = {interfaces.ATTR_NAME_PY_TYPE: get_dotted_name(args[0])}
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
        objectType = type(obj)
        if objectType in interfaces.PJ_NATIVE_TYPES:
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

        if objectType == datetime.date:
            return {'_py_type': 'datetime.date',
                    'value': obj.strftime(FMT_DATE)}
        if objectType == datetime.time:
            return {'_py_type': 'datetime.time',
                    'value': obj.strftime(FMT_TIME)}
        if objectType == datetime.datetime:
            return {'_py_type': 'datetime.datetime',
                    'value': obj.strftime(FMT_DATETIME)}

        if isinstance(obj, (type, types.ClassType)):
            # We frequently store class and function paths as meta-data, so we
            # need to be able to properly encode those.
            return {'_py_type': 'type',
                    'path': get_dotted_name(obj)}

        # We need to make sure that the object's jar and doc-object are
        # set. This is important for the case when a sub-object was just
        # added.
        if getattr(obj, interfaces.ATTR_NAME_SUB_OBJECT, False):
            if obj._p_jar is None:
                if pobj is not None and \
                        getattr(pobj, '_p_jar', None) is not None:
                    obj._p_jar = pobj._p_jar
                setattr(obj, interfaces.ATTR_NAME_DOC_OBJECT, pobj)

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
            if not getattr(obj, interfaces.ATTR_NAME_SUB_OBJECT, False):
                return self.get_persistent_state(obj, seen)
            # This persistent object is a sub-document, so it is treated like
            # a non-persistent object.

        return self.get_non_persistent_state(obj, seen)

    def get_full_state(self, obj):
        doc = self.get_state(obj.__getstate__(), obj)
        # Always add a persistent type info
        doc[interfaces.ATTR_NAME_PY_TYPE] = get_dotted_name(obj.__class__)
        # Return the full state document
        return doc

    def store(self, obj, ref_only=False, _id=None):
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
        doc[interfaces.ATTR_NAME_PY_TYPE] = py_type_attr_name

        stored = False
        txn_id = self._jar.get_transaction_id()
        if interfaces.IColumnSerialization.providedBy(obj):
            self._jar._ensure_sql_columns(obj, table_name)
            column_data = obj._pj_get_column_fields()
        else:
            column_data = None
        if obj._p_oid is None:
            doc_id = self._jar._insert_doc(
                db_name, table_name, doc, _id, column_data)
            stored = True
            obj._p_jar = self._jar
            obj._p_oid = DBRef(table_name, doc_id, db_name)

            # Make sure that any other code accessing this object in this
            # session, gets the same instance.
            self._jar._object_cache[obj._p_oid.as_key()] = obj
        else:
            self._jar._update_doc(
                db_name, table_name, doc, obj._p_oid.id, column_data)
            stored = True
        # let's call the hook here, to always have _p_jar and _p_oid set
        if interfaces.IPersistentSerializationHooks.providedBy(obj):
            obj._pj_after_store_hook(self._jar._conn)

        self._jar._stored_objects[id(obj)] = obj

        doc[interfaces.ATTR_NAME_PY_TYPE] = py_type_attr_name
        if stored:
            setattr(obj, interfaces.ATTR_NAME_TX_ID, txn_id)
        DBREF_RESOLVE_CACHE.put(obj._p_oid.as_key(), obj.__class__)
        return obj._p_oid


class ObjectReader(object):
    zope.interface.implements(interfaces.IObjectReader)

    def __init__(self, jar):
        self._jar = jar
        self.preferPersistent = True

    def simple_resolve(self, path, use_broken=True):
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
            # raise ImportError(path)
            if use_broken:
                return broken.Broken
            else:
                raise ImportError(path)
        return klass

    def _prepare_class(self, klass):
        # add attributes to the class

        class TableNameDescriptor(object):
            def __init__(self):
                self.name = {}

            def __get__(self, instance, owner):
                path = get_dotted_name(instance.__class__)
                if path not in self.name:
                    return None
                return self.name[path]

            def __set__(self, instance, value):
                path = get_dotted_name(instance.__class__)
                self.name[path] = str(value)

        if not hasattr(klass, interfaces.ATTR_NAME_TABLE):
            setattr(klass, interfaces.ATTR_NAME_TABLE, TableNameDescriptor())
        return klass

    def resolve(self, dbref, no_cache=False, use_broken=True):

        # 0. Use DBREF_RESOLVE_CACHE
        if not no_cache:
            klass = DBREF_RESOLVE_CACHE.get(dbref.as_key())
            if klass is not None:
                return self._prepare_class(klass)

        # 1. Try to optimize on whether there's just one class stored in one
        #    table, that can save us one DB query
        if dbref.table in TABLE_KLASS_MAP:
            results = TABLE_KLASS_MAP[dbref.table]
            if len(results) == 1:
                # there must be just ONE, otherwise we need to check the JSONB
                klass = list(results)[0]
                DBREF_RESOLVE_CACHE.put(dbref.as_key(), klass)
                return self._prepare_class(klass)

        # from this point on we need the dbref.id
        if dbref.id is None:
            if use_broken:
                return broken.Broken
            else:
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
            obj_doc = {interfaces.ATTR_NAME_PY_TYPE: pytype}
        if obj_doc is None:
            # There is no document for this reference in the database.
            if use_broken:
                return broken.Broken
            else:
                raise ImportError(dbref)
        if interfaces.ATTR_NAME_PY_TYPE in obj_doc:
            # We have always the path to the class in JSONB
            klass = self.simple_resolve(obj_doc[interfaces.ATTR_NAME_PY_TYPE], use_broken=use_broken)
        else:
            if use_broken:
                return broken.Broken
            else:
                raise ImportError(dbref)
        DBREF_RESOLVE_CACHE.put(dbref.as_key(), klass)
        return self._prepare_class(klass)

    def get_non_persistent_object(self, state, obj):
        if '_py_constant' in state:
            return self.simple_resolve(state['_py_constant'])

        # this method must NOT change the passed in state dict
        state = dict(state)
        if '_py_type' in state:
            # Handle the simplified case.
            klass = self.simple_resolve(state.pop('_py_type'))
            sub_obj = copy_reg._reconstructor(klass, object, None)
        elif interfaces.ATTR_NAME_PY_TYPE in state:
            # Another simple case for persistent objects that do not want
            # their own document.
            klass = self.simple_resolve(state.pop(interfaces.ATTR_NAME_PY_TYPE))
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
                setattr(sub_obj, interfaces.ATTR_NAME_SUB_OBJECT, True)
        if getattr(sub_obj, interfaces.ATTR_NAME_SUB_OBJECT, False):
            setattr(sub_obj, interfaces.ATTR_NAME_DOC_OBJECT, obj)
            sub_obj._p_jar = self._jar
        return sub_obj

    def get_object(self, state, obj):
        # stateIsDict and state_py_type: optimization to avoid X lookups
        # the code was:
        # if isinstance(state, dict) and state.get('_py_type') == 'DBREF':
        # this methods gets called a gazillion times, so being fast is crucial
        stateIsDict = isinstance(state, dict)
        if stateIsDict:
            state_py_type = state.get('_py_type')
            if state_py_type == 'BINARY':
                # Binary data in Python 2 is presented as a string. We will
                # convert back to binary when serializing again.
                return state['data'].decode('base64')
            if state_py_type == 'DBREF':
                # Load a persistent object. Using the _jar.load() method to make
                # sure we're loading from right database and caching is properly
                # applied.
                dbref = DBRef(state['table'], state['id'], state['database'])
                return self._jar.load(dbref)
            if state_py_type == 'type':
                # Convert a simple object reference, mostly classes.
                return self.simple_resolve(state['path'])
            if state_py_type == 'datetime.date':
                return datetime.datetime.strptime(
                    state['value'], FMT_DATE).date()
            if state_py_type == 'datetime.time':
                return datetime.datetime.strptime(
                    state['value'], FMT_TIME).time()
            if state_py_type == 'datetime.datetime':
                return datetime.datetime.strptime(
                    state['value'], FMT_DATETIME)

        # Give the custom serializers a chance to weigh in.
        for serializer in SERIALIZERS:
            if serializer.can_read(state):
                return serializer.read(state)

        if stateIsDict and (
            '_py_factory' in state
            or '_py_constant' in state
            or '_py_type' in state
            or interfaces.ATTR_NAME_PY_TYPE in state):
            # Load a non-persistent object.
            return self.get_non_persistent_object(state, obj)
        if isinstance(state, (tuple, list)):
            # All lists are converted to persistent lists, so that their state
            # changes are noticed. Also make sure that all value states are
            # converted to objects.
            sub_obj = [self.get_object(value, obj) for value in state]
            if self.preferPersistent:
                sub_obj = PersistentList(sub_obj)
                setattr(sub_obj, interfaces.ATTR_NAME_DOC_OBJECT, obj)
                sub_obj._p_jar = self._jar
            return sub_obj
        if stateIsDict:
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
                setattr(sub_obj, interfaces.ATTR_NAME_DOC_OBJECT, obj)
                sub_obj._p_jar = self._jar
            return sub_obj
        return state

    def set_ghost_state(self, obj, doc=None):
        # Check whether the object state was stored on the object itself.
        if doc is None:
            doc = getattr(obj, interfaces.ATTR_NAME_STATE, None)
        # Look up the object state by table_name and oid.
        if doc is None:
            doc = self._jar._get_doc_by_dbref(obj._p_oid)
        # Check that we really have a state doc now.
        if doc is None:
            raise ImportError(obj._p_oid)
        # Remove unwanted attributes.
        pytype = doc.pop(interfaces.ATTR_NAME_PY_TYPE)

        # Now convert the document to a proper Python state dict.
        state = dict(self.get_object(doc, obj))

        # Sometimes this method is called to update the object state
        # before storage.
        doc[interfaces.ATTR_NAME_PY_TYPE] = pytype
        # Set the state.
        obj.__setstate__(state)
        # Run the custom load functions.
        if interfaces.IPersistentSerializationHooks.providedBy(obj):
            obj._pj_after_load_hook(self._jar._conn)

    def get_ghost(self, dbref, klass=None):
        obj = self._jar._object_cache.get(dbref.as_key(), None)
        if obj is not None:
            return obj
        if klass is None:
            klass = self.resolve(dbref)
        obj = klass.__new__(klass)
        # TODO: convert to Persistent
        if issubclass(klass, broken.Broken):
            return obj
        obj._p_jar = self._jar
        obj._p_oid = dbref
        del obj._p_changed
        # Assign the table after deleting _p_changed, since the attribute
        # is otherwise deleted.
        setattr(obj, interfaces.ATTR_NAME_DATABASE, dbref.database)
        setattr(obj, interfaces.ATTR_NAME_TABLE, dbref.table)
        setattr(obj, interfaces.ATTR_NAME_TX_ID, None)
        self._jar._object_cache[dbref.as_key()] = obj
        return obj

    def load(self, data, table, _id, database=None):
        pytype = data['data'].get(interfaces.ATTR_NAME_PY_TYPE, None)
        if pytype is None:
            pytype = data['package'] + '.' + data['class_name']
            data['data'][interfaces.ATTR_NAME_PY_TYPE] = pytype
        klass = self._prepare_class(self.simple_resolve(pytype))
        obj = klass.__new__(klass)
        obj._p_jar = self._jar
        if database is None:
            database = self._jar.database
        dbref = DBRef(table, _id, database)
        obj._p_oid = dbref
        del obj._p_changed
        # Assign the table after deleting _p_changed, since the attribute
        # is otherwise deleted.
        setattr(obj, interfaces.ATTR_NAME_DATABASE, dbref.database)
        setattr(obj, interfaces.ATTR_NAME_TABLE, dbref.table)
        setattr(obj, interfaces.ATTR_NAME_TX_ID, data['tid'])
        self.set_ghost_state(obj, data['data'])
        return obj


class table:
    """Declare the table used by the class.

    sets also the atrtibute interfaces.ATTR_NAME_TABLE
    but register the fact also in TABLE_KLASS_MAP, this will allow pjpersist
    to optimize class lookup when just one class is stored in one table
    otherwise class lookup always needs the JSONB data from PG
    """

    def __init__(self, table_name):
        self.table_name = table_name

    def __call__(self, ob):
        try:
            setattr(ob, interfaces.ATTR_NAME_TABLE, self.table_name)
            TABLE_KLASS_MAP.setdefault(self.table_name, set()).add(ob)
        except AttributeError:
            raise TypeError(
                "Can't declare %s" % interfaces.ATTR_NAME_TABLE, ob)
        return ob
