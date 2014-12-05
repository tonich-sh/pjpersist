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
"""PostGreSQL/JSONB Persistent Data Manager"""
from __future__ import absolute_import
import UserDict
import logging
import psycopg2.extensions
import psycopg2.extras
import sqlobject.sqlbuilder as sb
import sys
import transaction
import uuid
import zope.interface
from zope.exceptions import exceptionformatter

from pjpersist import interfaces, serialize

PJ_ACCESS_LOGGING = False
TABLE_LOG = logging.getLogger('pjpersist.table')

LOG = logging.getLogger(__name__)

INITIALIZED_TABLES = []


class PJPersistCursor(psycopg2.extras.DictCursor):

    ADD_TB = True
    TB_LIMIT = 10  # 10 should be sufficient to figure

    def __init__(self, datamanager, flush, *args, **kwargs):
        super(PJPersistCursor, self).__init__(*args, **kwargs)
        self.datamanager = datamanager
        self.flush = flush

    def log_query(self, sql, args):
        if self.ADD_TB:
            try:
                raise ValueError('boom')
            except:
                # we need here exceptionformatter, otherwise __traceback_info__
                # is not added
                tb = ''.join(exceptionformatter.extract_stack(
                    sys.exc_info()[2].tb_frame.f_back, limit=self.TB_LIMIT))
        else:
            tb = '  <omitted>'

        txn = transaction.get()
        txn = '%i - %s' % (id(txn), txn.description),

        TABLE_LOG.debug(
            "sql:%r,\n args:%r,\n TXN:%s,\n tb:\n%s", txn, sql, args, tb)

    def execute(self, sql, args=None):
        # Convert SQLBuilder object to string
        if not isinstance(sql, basestring):
            sql = sql.__sqlrepr__('postgres')
        # Flush the data manager before any select.
        if self.flush and sql.strip().split()[0].lower() == 'select':
            self.datamanager.flush()
        # Very useful logging of every SQL command with tracebakc to code.
        if PJ_ACCESS_LOGGING:
            self.log_query(sql, args)

        # XXX: Optimization opportunity to store returned JSONB docs in the
        # cache of the data manager. (SR)

        return super(PJPersistCursor, self).execute(sql, args)


class Root(UserDict.DictMixin):

    table = 'persistence_root'

    def __init__(self, jar, table=None):
        self._jar = jar
        if table is not None:
            self.table = table
        self._init_table()

    def _init_table(self):
        with self._jar.getCursor(False) as cur:
            cur.execute(
                "SELECT * FROM information_schema.tables where table_name=%s",
                (self.table,))
            if cur.rowcount:
                return
            cur.execute('''
                CREATE TABLE %s (
                    id SERIAL PRIMARY KEY,
                    name TEXT,
                    dbref TEXT[])
                ''' %self.table)

    def __getitem__(self, key):
        with self._jar.getCursor(False) as cur:
            tbl = getattr(sb.table, self.table)
            cur.execute(
                sb.Select(sb.Field(self.table, 'dbref'), tbl.name == key))
            if not cur.rowcount:
                raise KeyError(key)
            db, tbl, id = cur.fetchone()['dbref']
            dbref = serialize.DBRef(tbl, id, db)
            return self._jar.load(dbref)

    def __setitem__(self, key, value):
        dbref = self._jar.insert(value)
        if self.get(key) is not None:
            del self[key]
        with self._jar.getCursor(False) as cur:
            cur.execute(
                'INSERT INTO %s (name, dbref) VALUES (%%s, %%s)' %self.table,
                (key, list(dbref.as_tuple()))
                )

    def __delitem__(self, key):
        self._jar.remove(self[key])
        with self._jar.getCursor(False) as cur:
            tbl = getattr(sb.table, self.table)
            cur.execute(sb.Delete(self.table, tbl.name == key))

    def keys(self):
        with self._jar.getCursor(False) as cur:
            tbl = getattr(sb.table, self.table)
            cur.execute(sb.Select(sb.Field(self.table, 'name')))
            return [doc['name'] for doc in cur.fetchall()]


class PJDataManager(object):
    zope.interface.implements(interfaces.IPJDataManager)

    name_map_table = 'persistence_name_map'

    def __init__(self, conn, root_table=None, name_map_table=None):
        self._conn = conn
        self.database = conn.dsn.split()[0][7:]
        self._reader = serialize.ObjectReader(self)
        self._writer = serialize.ObjectWriter(self)
        # All of the following object lists are keys by object id. This is
        # needed when testing containment, since that can utilize `__cmp__()`
        # which can have undesired side effects. `id()` is guaranteed to not
        # use any method or state of the object itself.
        self._registered_objects = {}
        self._loaded_objects = {}
        self._inserted_objects = {}
        self._modified_objects = {}
        self._removed_objects = {}
        # Keeps states as found at the beginning of the transaction.
        self._original_states = {}
        # The latest states written to the database. This is different to the
        # original states, since changes can be flushed to the database
        # multiple times per transaction.
        self._latest_states = {}
        self._needs_to_join = True
        self._object_cache = {}
        self.annotations = {}
        if name_map_table is not None:
            self.name_map_table = name_map_table
        self._init_name_map_table()
        self.transaction_manager = transaction.manager
        self.root = Root(self, root_table)

    def getCursor(self, flush=True):
        def factory(*args, **kwargs):
            return PJPersistCursor(self, flush, *args, **kwargs)
        return self._conn.cursor(cursor_factory=factory)

    def _init_name_map_table(self):
        with self.getCursor(False) as cur:
            cur.execute(
            "SELECT * FROM information_schema.tables where table_name=%s",
                (self.name_map_table,))
            if cur.rowcount:
                return
            cur.execute('''
                CREATE TABLE %s (
                    database varchar,
                    tbl varchar,
                    path varchar,
                    doc_has_type bool)
                ''' % self.name_map_table)

    def _get_name_map_entry(self, database, table, path=None):
        name_map = getattr(sb.table, self.name_map_table)
        clause = (name_map.database == database) & (name_map.tbl == table)
        if path is not None:
            clause &= (name_map.path == path)
        with self.getCursor(False) as cur:
            cur.execute(sb.Select(sb.Field(self.name_map_table, '*'), clause))
            if path is None:
                return cur.fetchall()
            return cur.fetchone() if cur.rowcount else None

    def _insert_name_map_entry(self, database, table, path, doc_has_type):
        with self.getCursor(False) as cur:
            cur.execute(
                sb.Insert(
                    self.name_map_table, values={
                        'database': database,
                        'tbl': table,
                        'path': path,
                        'doc_has_type': doc_has_type})
                )

    def _create_doc_table(self, database, table):
        if self.database != database:
            raise NotImplemented(
                'Cannot store an object of a different database.')

        if (database, table) in INITIALIZED_TABLES:
            return

        with self.getCursor(False) as cur:
            cur.execute(
            "SELECT * FROM information_schema.tables where table_name=%s",
                (table,))
            if not cur.rowcount:
                cur.execute('''
                    CREATE TABLE %s (
                        id uuid primary key,
                        data jsonb);
                    ''' % table)
            INITIALIZED_TABLES.append((database, table))

    def _insert_doc(self, database, table, doc, id=None):
        self._create_doc_table(database, table)
        # Create id if it is None.
        if id is None:
            id = unicode(uuid.uuid4())
        # Insert the document into the table.
        with self.getCursor() as cur:
                cur.execute(
                "INSERT INTO " + table + " (id, data) VALUES (%s, %s)",
                (id, psycopg2.extras.Json(doc))
                )
        return id

    def _update_doc(self, database, table, doc, id):
        # Insert the document into the table.
        with self.getCursor() as cur:
            cur.execute(
                "UPDATE " + table + " SET data=%s WHERE id = %s",
                (psycopg2.extras.Json(doc), id)
                )
        return id

    def _get_doc(self, database, table, id):
        self._create_doc_table(database, table)
        tbl = getattr(sb.table, table)
        with self.getCursor() as cur:
            cur.execute(sb.Select(sb.Field(table, '*'), tbl.id == id))
            res = cur.fetchone()
            return res['data'] if res is not None else None

    def _get_doc_by_dbref(self, dbref):
        return self._get_doc(dbref.database, dbref.table, dbref.id)

    def _get_doc_py_type(self, database, table, id):
        self._create_doc_table(database, table)
        tbl = getattr(sb.table, table)
        with self.getCursor() as cur:
            cur.execute(
                sb.Select(sb.Field(table, interfaces.PY_TYPE_ATTR_NAME),
                          tbl.id == id))
            res = cur.fetchone()
            return res[interfaces.PY_TYPE_ATTR_NAME] if res is not None else None

    def _get_table_from_object(self, obj):
        return self._writer.get_table_name(obj)

    def _flush_objects(self):
        # Now write every registered object, but make sure we write each
        # object just once.
        written = set()
        # Make sure that we do not compute the list of flushable objects all
        # at once. While writing objects, new sub-objects might be registered
        # that also need saving.
        todo = set(self._registered_objects.keys())
        while todo:
            obj_id = todo.pop()
            obj = self._registered_objects[obj_id]
            __traceback_info__ = obj
            obj = self._get_doc_object(obj)
            self._writer.store(obj)
            written.add(obj_id)
            todo = set(self._registered_objects.keys()) - written

    def _get_doc_object(self, obj):
        seen = []
        # Make sure we write the object representing a document in a
        # table and not a sub-object.
        while getattr(obj, '_p_pj_sub_object', False):
            if id(obj) in seen:
                raise interfaces.CircularReferenceError(obj)
            seen.append(id(obj))
            obj = obj._p_pj_doc_object
        return obj

    def dump(self, obj):
        res = self._writer.store(obj)
        if id(obj) in self._registered_objects:
            obj._p_changed = False
            del self._registered_objects[id(obj)]
        return res

    def load(self, dbref, klass=None):
        return self._reader.get_ghost(dbref, klass)

    def reset(self):
        root = self.root
        self.__init__(self._conn)
        self.root = root

    def flush(self):
        # Now write every registered object, but make sure we write each
        # object just once.
        self._flush_objects()
        # Let's now reset all objects as if they were not modified:
        for obj in self._registered_objects.values():
            obj._p_changed = False
        self._registered_objects = {}

    def insert(self, obj, oid=None):
        if self._needs_to_join:
            self.transaction_manager.get().join(self)
            self._needs_to_join = False
        if obj._p_oid is not None:
            raise ValueError('Object._p_oid is already set.', obj)
        res = self._writer.store(obj, id=oid)
        obj._p_changed = False
        self._object_cache[hash(obj._p_oid)] = obj
        self._inserted_objects[id(obj)] = obj
        return res

    def remove(self, obj):
        if obj._p_oid is None:
            raise ValueError('Object._p_oid is None.', obj)
        # If the object is still in the ghost state, let's load it, so that we
        # have the state in case we abort the transaction later.
        if obj._p_changed is None:
            self.setstate(obj)
        # Now we remove the object from PostGreSQL.
        dbname, table = self._get_table_from_object(obj)
        with self.getCursor() as cur:
            cur.execute('DELETE FROM %s WHERE id = %%s' %table, (obj._p_oid.id,))
        if hash(obj._p_oid) in self._object_cache:
            del self._object_cache[hash(obj._p_oid)]

        # Edge case: The object was just added in this transaction.
        if id(obj) in self._inserted_objects:
            # but it still had to be removed from PostGreSQL, because insert
            # inserted it just before
            del self._inserted_objects[id(obj)]

        self._removed_objects[id(obj)] = obj
        # Just in case the object was modified before removal, let's remove it
        # from the modification list. Note that all sub-objects need to be
        # deleted too!
        for key, reg_obj in self._registered_objects.items():
            if self._get_doc_object(reg_obj) is obj:
                del self._registered_objects[key]
        # We are not doing anything fancy here, since the object might be
        # added again with some different state.

    def setstate(self, obj, doc=None):
        # When reading a state from PostGreSQL, we also need to join the
        # transaction, because we keep an active object cache that gets stale
        # after the transaction is complete and must be cleaned.
        if self._needs_to_join:
            self.transaction_manager.get().join(self)
            self._needs_to_join = False
        # If the doc is None, but it has been loaded before, we look it
        # up. This acts as a great hook for optimizations that load many
        # documents at once. They can now dump the states into the
        # _latest_states dictionary.
        if doc is None:
            doc = self._latest_states.get(obj._p_oid, None)
        self._reader.set_ghost_state(obj, doc)
        self._loaded_objects[id(obj)] = obj

    def oldstate(self, obj, tid):
        # I cannot find any code using this method. Also, since we do not keep
        # version history, we always raise an error.
        raise KeyError(tid)

    def register(self, obj):
        if self._needs_to_join:
            self.transaction_manager.get().join(self)
            self._needs_to_join = False

        # Do not bring back removed objects. But only main the document
        # objects can be removed, so check for that.
        if id(self._get_doc_object(obj)) in self._removed_objects:
            return

        if obj is not None:
            if id(obj) not in self._registered_objects:
                self._registered_objects[id(obj)] = obj
            if id(obj) not in self._modified_objects:
                obj = self._get_doc_object(obj)
                self._modified_objects[id(obj)] = obj

    def abort(self, transaction):
        self._conn.rollback()
        self.reset()

    def commit(self, transaction):
        self._flush_objects()
        self._conn.commit()
        self.reset()

    def tpc_begin(self, transaction):
        pass

    def tpc_vote(self, transaction):
        pass

    def tpc_finish(self, transaction):
        self.commit(transaction)

    def tpc_abort(self, transaction):
        self.abort(transaction)

    def sortKey(self):
        return ('PJDataManager', 0)
