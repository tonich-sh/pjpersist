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

import logging
import psycopg2
import psycopg2.extensions
import psycopg2.extras
import psycopg2.errorcodes
import re
import threading
import time
import transaction
import zope.interface

from persistent.interfaces import GHOST
from persistent.mapping import PersistentMapping

from pjpersist import interfaces, serialize
from pjpersist.querystats import QueryReport


PJ_ACCESS_LOGGING = False
# set to True to automatically create tables if they don't exist
# it is relatively expensive, so create your tables with a schema.sql
# and turn this off for production

# Enable query statistics reporting after transaction ends
PJ_ENABLE_QUERY_STATS = False

# Enable logging queries to global query statistics report. If you enable this,
# make sure you set GLOBAL_QUERY_STATS.report to None after each report.
PJ_ENABLE_GLOBAL_QUERY_STATS = False
GLOBAL_QUERY_STATS = threading.local()
GLOBAL_QUERY_STATS.report = None

# Maximum query length to output qith query log
MAX_QUERY_ARGUMENT_LENGTH = 500


PJ_AUTO_CREATE_TABLES = True

# set to True to automatically create IColumnSerialization columns
# will also create tables regardless of the PJ_AUTO_CREATE_TABLES setting
# so this is super expensive
PJ_AUTO_CREATE_COLUMNS = True


TABLE_LOG = logging.getLogger('pjpersist.table')

LOG = logging.getLogger(__name__)

psycopg2.extras.register_uuid()


notify = None


class StoredEvent(object):
    __slots__ = ('obj', )

    def __init__(self, obj):
        self.obj = obj


class Json(psycopg2.extras.Json):
    """In logs, we want to have the JSON value not just Json object at <>"""
    def __repr__(self):
        if PJ_ACCESS_LOGGING:
            try:
                s = self.dumps(self.adapted)
            except:
                s = 'exception'
            return '<%s %s>' % (self.__class__.__name__, s)
        else:
            return '<%s>' % (self.__class__.__name__, )


class PJPersistCursor(psycopg2.extras.DictCursor):
    def __init__(self, datamanager, flush, *args, **kwargs):
        super(PJPersistCursor, self).__init__(*args, **kwargs)
        self.datamanager = datamanager
        self.flush = flush

    def log_query(self, sql, args, duration):

        txn = transaction.get()
        txn = '%i - %s' % (id(txn), txn.description),

        TABLE_LOG.debug(
            "%s,\n args:%r,\n TXN:%s,\n time:%sms",
            sql, args, txn, duration*1000)

    def execute(self, sql, args=None, autocreate=PJ_AUTO_CREATE_TABLES):
        # Convert SQLBuilder object to string
        if not isinstance(sql, basestring):
            sql = sql.__sqlrepr__('postgres')
        # Flush the data manager before any select.
        query_type = sql.strip().split()[0].lower()
        if self.flush and query_type == 'select':
            self.datamanager.flush()

        # XXX: Optimization opportunity to store returned JSONB docs in the
        # cache of the data manager. (SR)
        if autocreate:
            # XXX: need to set a savepoint, just in case the real execute
            #      fails, it would take down all further commands
            super(PJPersistCursor, self).execute("SAVEPOINT before_execute")

            try:
                return self._execute_and_log(sql, args)
            except psycopg2.Error, e:
                # XXX: ugly: we're creating here missing tables on the fly
                msg = e.message
                TABLE_LOG.debug("%s %r failed with %s", sql, args, msg)
                # if the exception message matches
                m = re.search('relation "(.*?)" does not exist', msg)
                if m:
                    # need to rollback to the above savepoint, otherwise
                    # PG would just ignore any further command
                    super(PJPersistCursor, self).execute(
                        "ROLLBACK TO SAVEPOINT before_execute")

                    if query_type == 'select':
                        # just select for an empty result
                        return self._execute_and_log('select * from unnest(array[1]) where false', None)

                    # we extract the tableName from the exception message
                    tableName = m.group(1)
                    self.datamanager._create_doc_table(
                        self.datamanager.database, tableName)

                    try:
                        return self._execute_and_log(sql, args)
                    except psycopg2.Error, e:
                        pass
                check_for_conflict(e, sql)
                # otherwise let it fly away
                raise
        else:
            try:
                # otherwise just execute the given sql
                return self._execute_and_log(sql, args)
            except psycopg2.Error, e:
                # Join the transaction, because failed queries require
                # aborting the transaction.
                # self.datamanager._join_txn()
                check_for_conflict(e, sql)
                raise

    def _sanitize_arg(self, arg):
        r = repr(arg)
        if len(r) > MAX_QUERY_ARGUMENT_LENGTH:
            r = r[:MAX_QUERY_ARGUMENT_LENGTH] + "..."
            return r
        return arg

    def _execute_and_log(self, sql, args):
        # Very useful logging of every SQL command with traceback to code.
        # __traceback_info__ = (self.datamanager.database, sql, args)
        t0 = time.time()
        try:
            res = super(PJPersistCursor, self).execute(sql, args)
        finally:
            t1 = time.time()
            db = self.datamanager.database

            debug = (PJ_ACCESS_LOGGING or
                     PJ_ENABLE_QUERY_STATS or
                     PJ_ENABLE_QUERY_STATS)

            if debug:
                saneargs = [self._sanitize_arg(a) for a in args] \
                    if args else args

            if PJ_ACCESS_LOGGING:
                self.log_query(sql, saneargs, t1-t0)

            if PJ_ENABLE_QUERY_STATS:
                self.datamanager._query_report.record(sql, saneargs, t1-t0, db)

            if PJ_ENABLE_GLOBAL_QUERY_STATS:
                if getattr(GLOBAL_QUERY_STATS, 'report', None) is None:
                    GLOBAL_QUERY_STATS.report = QueryReport()
                GLOBAL_QUERY_STATS.report.record(sql, saneargs, t1-t0, db)
        return res


def check_for_conflict(e, sql):
    """Check whether exception indicates serialization failure and raise
    ConflictError in this case.

    Serialization failures are denoted by postgres codes:
        40001 - serialization_failure
        40P01 - deadlock_detected
    """
    serialization_errors = (
        psycopg2.errorcodes.SERIALIZATION_FAILURE,
        psycopg2.errorcodes.DEADLOCK_DETECTED
    )
    if e.pgcode in serialization_errors:
        LOG.warning("Conflict detected with code %s sql: %s", e.pgcode, sql)
        raise interfaces.ConflictError(str(e), sql)


class DBRoot(PersistentMapping):
    pass


class RootConvenience(object):
    __slots__ = '_v_root'

    def __init__(self, root):
        object.__setattr__(self, '_v_root', root)

    def __getattr__(self, name):
        try:
            vr = object.__getattribute__(self, '_v_root')
            return vr[name]
        except KeyError:
            raise AttributeError(name)

    def __setattr__(self, name, v):
        self._v_root[name] = v

    def __delattr__(self, name):
        try:
            del self._v_root[name]
        except KeyError:
            raise AttributeError(name)

    def __call__(self):
        return self._v_root

    def __repr__(self):
        names = " ".join(sorted(self._v_root))
        if len(names) > 60:
            names = names[:57].rsplit(' ', 1)[0] + ' ...'
        return "<root: %s>" % names


class PJDataManager(object):
    zope.interface.implements(
        interfaces.IPJDataManager,
        transaction.interfaces.IDataManager
    )

    _root = None

    def __init__(self, conn, root_table=None):
        psycopg2.extensions.register_type(psycopg2.extensions.UNICODE, conn)
        psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY, conn)
        self._conn = conn
        self.database = get_database_name_from_dsn(conn.dsn)
        self._reader = serialize.ObjectReader(self)
        self._writer = serialize.ObjectWriter(self)
        self._needs_to_join = True
        self._in_commit = False
        self._commit_failed = False
        self._object_cache = {}
        self._cleanup()

    def _cleanup(self):
        LOG.debug('cleanup!!!')
        # All of the following object lists are keys by object id. This is
        # needed when testing containment, since that can utilize `__cmp__()`
        # which can have undesired side effects. `id()` is guaranteed to not
        # use any method or state of the object itself.
        self._registered_objects = {}
        self._inserted_objects = {}
        self._modified_objects = {}
        self._removed_objects = {}
        self._stored_objects = {}
        self.annotations = {}

        # transaction related
        self._transaction_id = None
        self._prev_transaction_id = None
        self._txn_active = False
        self.requestTransactionOptions()  # No special options

        self.transaction_manager = transaction.manager

        self._query_report = QueryReport()
        self._commit_failed = False
        if self._root is not None:
            LOG.debug('invalidate root')
            self._root._p_invalidate()
        for obj in self._object_cache.itervalues():
            obj._p_invalidate()

    @property
    def root(self):
        if self._root is None:
            self._root = self._create_root()
        return RootConvenience(self._root)

    def _create_root(self):
        # load root
        _root_oid = serialize.DBRef('pjpersist_dot_datamanager_dot_DBRoot', 0, self.database)

        try:
            root = self.load(_root_oid, self._reader.resolve(_root_oid, no_cache=True, use_broken=False))
            LOG.debug('DBRoot loaded successfully!')
        except ImportError:
            root = None

        if root is None:
            # create root
            root = DBRoot()
            self.insert(root, _root_oid)
            root._p_jar = self
            root._p_oid = _root_oid
        return root

    def requestTransactionOptions(self, readonly=None, deferrable=None,
                                  isolation=None):
        if self._txn_active:
            LOG.warning("Cannot set transaction options while transaction "
                        "is already active.")
        self._txn_readonly = readonly
        self._txn_deferrable = deferrable
        self._txn_isolation = isolation

    def _setTransactionOptions(self, cur):
        modes = []
        if self._txn_readonly:
            dfr = "DEFERRABLE" if self._txn_deferrable else ""
            modes.append("READ ONLY %s" % dfr)

        if self._txn_isolation:
            assert self._txn_isolation in ["SERIALIZABLE",
                                           "REPEATABLE READ",
                                           "READ COMMITTED",
                                           "READ UNCOMMITTED"]
            modes.append("ISOLATION LEVEL %s" % self._txn_isolation)

        if not modes:
            return

        stmt = "SET TRANSACTION %s" % (", ".join(modes))
        psycopg2.extras.DictCursor.execute(cur, "BEGIN")
        psycopg2.extras.DictCursor.execute(cur, stmt)

    def get_transaction_id(self):
        if self._transaction_id is None:
            with self.getCursor(False) as cur:
                psycopg2.extras.DictCursor.execute(cur, "SAVEPOINT before_get_tid")
                try:
                    psycopg2.extras.DictCursor.execute(cur, "SELECT NEXTVAL('transaction_id_seq')")
                    self._transaction_id = cur.fetchone()[0]
                except psycopg2.ProgrammingError:
                    psycopg2.extras.DictCursor.execute(cur, "ROLLBACK TO SAVEPOINT before_get_tid")
                    psycopg2.extras.DictCursor.execute(cur, "CREATE SEQUENCE transaction_id_seq")
                    psycopg2.extras.DictCursor.execute(cur, "SELECT NEXTVAL('transaction_id_seq')")
                    self._transaction_id = cur.fetchone()[0]
                else:
                    psycopg2.extras.DictCursor.execute(cur, "RELEASE SAVEPOINT before_get_tid")

        return self._transaction_id

    def getCursor(self, flush=True):
        def factory(*args, **kwargs):
            return PJPersistCursor(self, flush, *args, **kwargs)
        cur = self._conn.cursor(cursor_factory=factory)
        self._join_txn()
        if not self._txn_active:
            self._setTransactionOptions(cur)
            self._txn_active = True
        return cur

    def create_id(self):
        with self.getCursor(False) as cur:
            psycopg2.extras.DictCursor.execute(cur, "SAVEPOINT before_get_id")
            try:
                cur.execute("SELECT NEXTVAL('main_id_seq')")
                _id = cur.fetchone()[0]
            except psycopg2.ProgrammingError:
                psycopg2.extras.DictCursor.execute(cur, "ROLLBACK TO SAVEPOINT before_get_id")
                psycopg2.extras.DictCursor.execute(cur, "CREATE SEQUENCE main_id_seq")
                psycopg2.extras.DictCursor.execute(cur, "SELECT NEXTVAL('main_id_seq')")
                _id = cur.fetchone()[0]
            else:
                psycopg2.extras.DictCursor.execute(cur, "RELEASE SAVEPOINT before_get_id")
        return _id

    def create_tables(self, tables):
        if isinstance(tables, basestring):
            tables = [tables]

        for tbl in tables:
            self._create_doc_table(self.database, tbl)

        with self.getCursor(False) as cur:
            cur.connection.commit()

    def _create_doc_table(self, database, table, extra_columns=''):
        if self.database != database:
            raise NotImplementedError(
                'Cannot store an object of a different database.',
                self.database, database)

        with self.getCursor(False) as cur:
            cur.execute(
                "SELECT * FROM information_schema.tables WHERE table_name=%s",
                (table,))
            if not cur.rowcount:
                LOG.info("Creating data table %s with extra columns: '%s'" % (table, extra_columns))
                if extra_columns:
                    extra_columns += ', '
                cur.execute('''
                    CREATE TABLE %s (
                        id BIGSERIAL PRIMARY KEY,
                        tid BIGINT NOT NULL,
                        package TEXT NOT NULL,
                        class_name TEXT NOT NULL
                    )''' % (table, ))
                cur.execute('''
                    CREATE TABLE %s_state (
                        sid BIGSERIAL PRIMARY KEY,
                        pid BIGINT NOT NULL,
                        tid BIGINT NOT NULL,
                        %s
                        data JSONB,
                        CONSTRAINT %s_pid_tid_unique UNIQUE (pid, tid))''' % (table, extra_columns, table))
                # this index helps a tiny bit with JSONB_CONTAINS queries
                cur.execute('''
                    CREATE INDEX %s_data_gin ON %s_state USING GIN (data);
                    ''' % (table, table))

    def _ensure_sql_columns(self, obj, table):
        # create the table required for the object, with the necessary
        # _pj_column_fields translated to SQL types
        if PJ_AUTO_CREATE_COLUMNS:
            if interfaces.IColumnSerialization.providedBy(obj):
                # XXX: exercise for later, not just create but check
                #      the columns
                # SELECT column_name
                #  FROM INFORMATION_SCHEMA.COLUMNS
                #  WHERE table_name = '<name of table>';
                columns = []
                for field in obj._pj_column_fields:
                    pgtype = serialize.PYTHON_TO_PG_TYPES[field._type]
                    columns.append("%s %s" % (field.__name__, pgtype))

                columns = ', '.join(columns)

                self._create_doc_table(self.database, table, columns)
                return True
        return False

    def _insert_doc(self, database, table, doc, _id=None, column_data=None):

        # Insert the document into the table.
        with self.getCursor() as cur:
            persistent_type = doc[interfaces.ATTR_NAME_PY_TYPE]
            # package = '.'.join(persistent_type[0:-1])
            # class_name = persistent_type[-1]
            i = persistent_type.rfind('.')
            package = persistent_type[0:i]
            class_name = persistent_type[i+1:]
            if _id is None:
                sql = "INSERT INTO %(table)s (tid, package, class_name) VALUES (%%(tid)s, %%(package)s, %%(class_name)s) RETURNING id" % {'table': table}
            else:
                sql = "INSERT INTO %(table)s (id, tid, package, class_name) VALUES (%%(id)s, %%(tid)s, %%(package)s, %%(class_name)s) RETURNING id" % {'table': table}

            data = {'id': _id, 'tid': self.get_transaction_id(), 'package': package, 'class_name': class_name}
            cur.execute(sql, data)
            _id = cur.fetchone()[0]

            del doc[interfaces.ATTR_NAME_PY_TYPE]
            builtins = dict(data=Json(doc))

            if column_data is None:
                column_data = builtins
            else:
                column_data.update(builtins)

            columns = []
            values = []
            for colname, value in column_data.items():
                columns.append(colname)
                values.append(value)
            placeholders = ', '.join(['%s'] * len(columns))
            columns = ', '.join(columns)
            sql = "INSERT INTO %s_state (tid, pid, %s) VALUES (%d, %d, %s)" % (
                table, columns, self.get_transaction_id(), _id, placeholders)

            cur.execute(sql, tuple(values))
        return _id

    def _update_doc(self, database, table, doc, _id, column_data=None):
        # Insert the document into the table.
        with self.getCursor() as cur:
            del doc[interfaces.ATTR_NAME_PY_TYPE]
            builtins = dict(data=Json(doc))

            if column_data is None:
                column_data = builtins
            else:
                column_data.update(builtins)

            columns = []
            values = []
            for colname, value in column_data.items():
                columns.append(colname)
                values.append(value)
            placeholders = ', '.join(['%s'] * len(columns))
            columns = ', '.join(columns)
            sql1 = "INSERT INTO %s_state (tid, pid, %s) VALUES (%d, %d, %s)" % (
                table, columns, self.get_transaction_id(), _id, placeholders)

            psycopg2.extras.DictCursor.execute(cur, "SAVEPOINT before_insert")
            try:
                cur.execute(sql1, tuple(values))
            except psycopg2.IntegrityError:
                psycopg2.extras.DictCursor.execute(cur, "ROLLBACK TO SAVEPOINT before_insert")
                columns = []
                values = []
                for colname, value in column_data.items():
                    columns.append(colname + '=%s')
                    values.append(value)
                columns = ', '.join(columns)
                sql2 = "UPDATE %s_state SET %s WHERE tid=%%s AND pid=%%s" % (table, columns)
                cur.execute(sql2, tuple(values) + (self.get_transaction_id(), _id))
            else:
                psycopg2.extras.DictCursor.execute(cur, "RELEASE SAVEPOINT before_insert")

            sql3 = "UPDATE %s SET tid=%d WHERE id = %d" % (table, self.get_transaction_id(), _id)

            cur.execute(sql3)
        return _id

    def _get_doc(self, database, table, _id):
        with self.getCursor() as cur:
            sql = """
SELECT
    m.tid,
    m.package,
    m.class_name,
    s.data
FROM
    %s m
    JOIN %s_state s ON m.id = s.pid AND m.tid = s.tid
WHERE
    m.id=%%s""" % (table, table)
            cur.execute(sql, (_id, ))
            res = cur.fetchone()
            if res:
                res['data'][interfaces.ATTR_NAME_PY_TYPE] = '%(package)s.%(class_name)s' % res
                res['data'][interfaces.ATTR_NAME_TX_ID] = res.get('tid', None)
                return res['data']
            return None

    def _get_doc_by_dbref(self, dbref):
        return self._get_doc(dbref.database, dbref.table, dbref.id)

    def _get_doc_py_type(self, database, table, _id):
        with self.getCursor() as cur:
            sql = """
SELECT
    package || '.' || class_name
FROM
    %s
WHERE
    id=%%s
""" % table
            cur.execute(sql, (_id,))
            res = cur.fetchone()
            return res[0] if res is not None else None

    def _get_table_from_object(self, obj):
        return self._writer.get_table_name(obj)

    def _flush_objects(self):
        # self.root.on_flush()
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
            # __traceback_info__ = obj
            obj = self._get_doc_object(obj)  # make sure that obj is not a subobject
            self._writer.store(obj)
            written.add(obj_id)
            todo = set(self._registered_objects.keys()) - written

    def _get_doc_object(self, obj):
        seen = []
        # Make sure we write the object representing a document in a
        # table and not a sub-object.
        while getattr(obj, interfaces.ATTR_NAME_SUB_OBJECT, False):
            if id(obj) in seen:
                raise interfaces.CircularReferenceError(obj)
            seen.append(id(obj))
            obj = obj._p_pj_doc_object
        if obj._p_state == GHOST:
            obj._p_activate()
        return obj

    def _join_txn(self):
        if self._needs_to_join:
            txn = self.transaction_manager.get()
            txn.join(self)
            self._needs_to_join = False

    def load(self, dbref, klass=None):
        if dbref.database != self.database:
            # This is a reference of object from different database! We need to
            # locate the suitable data manager for this.
            dmp = zope.component.getUtility(interfaces.IPJDataManagerProvider)
            dm = dmp.get(dbref.database)
            assert dm.database == dbref.database, (dm.database, dbref.database)
            return dm.load(dbref, klass)
        g = self._reader.get_ghost(dbref, klass)
        return g

    def reset(self):
        # we need to issue rollback on self._conn too, to get the latest
        # DB updates, not just reset PJDataManager state
        self.abort(None)
        self._object_cache = {}

    # TODO: remove (use commit)
    def flush(self):
        # Now write every registered object, but make sure we write each
        # object just once.
        self._flush_objects()
        # Let's now reset all objects as if they were not modified:
        for obj in self._registered_objects.values():
            obj._p_changed = False
        self._registered_objects = {}

    def insert(self, obj, oid=None):
        if obj._p_oid is not None:
            raise ValueError('Object._p_oid is already set.', obj)
        if oid is not None:
            if isinstance(oid, serialize.DBRef):
                _id = oid.id
            else:
                _id = oid
        else:
            _id = None
        res = self._writer.store(obj, _id=_id)
        obj._p_changed = False
        self._object_cache[obj._p_oid.as_key()] = obj
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
        _, table = self._get_table_from_object(obj)
        with self.getCursor() as cur:
            try:
                cur.execute('DELETE FROM %s_state WHERE pid = %%s' % table, (obj._p_oid.id, ))
                cur.execute('DELETE FROM %s WHERE id=%%s' % table, (obj._p_oid.id,))
            except:
                pass
        cache_key = obj._p_oid.as_key()
        if cache_key in self._object_cache:
            del self._object_cache[cache_key]
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

    def setstate(self, obj):
        dbref = obj._p_oid
        doc = self._get_doc_by_dbref(dbref)
        self._reader.set_ghost_state(obj, doc)

    # TODO: implement oldstate() method
    def oldstate(self, obj, tid):
        raise KeyError(tid)

    def register(self, obj):
        self._join_txn()

        # Do not bring back removed objects. But only main the document
        # objects can be removed, so check for that.
        if id(self._get_doc_object(obj)) in self._removed_objects:
            return

        if obj is not None:
            if id(obj) not in self._registered_objects:
                self._registered_objects[id(obj)] = obj
                obj_registered = getattr(obj, '_pj_object_registered', None)
                if obj_registered is not None:
                    obj_registered(self)
            if id(obj) not in self._modified_objects:
                obj = self._get_doc_object(obj)
                self._modified_objects[id(obj)] = obj

    def abort(self, transaction):
        LOG.debug('Abort transaction!!!')
        # should not call from two-phase commit
        assert not self._in_commit
        self._report_stats()
        try:
            self._conn.rollback()
        except psycopg2.InterfaceError:
            # this happens usually when PG is restarted and the connection dies
            # our only chance to exit the spiral is to abort the transaction
            pass
        self._cleanup()

    def commit(self, transaction):
        try:
            # Now write every registered object, but make sure we write each
            # object just once.
            self._flush_objects()
            # Let's now reset all objects as if they were not modified:
            for obj in self._registered_objects.values():
                obj._p_changed = False
            self._registered_objects = {}
            self._commit_failed = False
        except:
            self._commit_failed = True
            raise

    def _tpc_cleanup(self):
        """Performs cleanup operations to support tpc_finish and tpc_abort."""
        if not self._needs_to_join:
            self._needs_to_join = True
        self._in_commit = False

    def tpc_begin(self, transaction):
        self._in_commit = True

    def tpc_vote(self, transaction):
        """
        Stores transaction id and commit datetime then performs commit
        """
        with self.getCursor(False) as cur:
            psycopg2.extras.DictCursor.execute(cur, "SAVEPOINT before_insert_transaction")
            isql = "INSERT INTO transactions(tid) VALUES(%s)"
            try:
                psycopg2.extras.DictCursor.execute(cur, isql, (self.get_transaction_id(), ))
            except psycopg2.IntegrityError:
                psycopg2.extras.DictCursor.execute(cur, "ROLLBACK TO SAVEPOINT before_insert_transaction")
                usql = "UPDATE transactions SET created_at = NOW() WHERE tid=%s"
                psycopg2.extras.DictCursor.execute(cur, usql, (self.get_transaction_id(), ))
            except psycopg2.Error, e:
                msg = e.message
                # if the exception message matches
                m = re.search('relation "(.*?)" does not exist', msg)
                if m:
                    psycopg2.extras.DictCursor.execute(cur, "ROLLBACK TO SAVEPOINT before_insert_transaction")
                    sql = """
CREATE TABLE transactions (
    tid bigint PRIMARY KEY,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT clock_timestamp()
)
"""
                    psycopg2.extras.DictCursor.execute(cur, sql)
                    psycopg2.extras.DictCursor.execute(cur, isql, (self.get_transaction_id(), ))
                else:
                    psycopg2.extras.DictCursor.execute(cur, "RELEASE SAVEPOINT before_insert_transaction")
            else:
                psycopg2.extras.DictCursor.execute(cur, "RELEASE SAVEPOINT before_insert_transaction")
        try:
            self._conn.commit()
            if callable(notify):
                for obj in self._stored_objects.values():
                    # notify a store object
                    notify(StoredEvent(obj))
        except psycopg2.Error, e:
            check_for_conflict(e, "DataManager.commit")

    def tpc_finish(self, transaction):
        LOG.debug('tpc finish!!!')
        try:
            self._report_stats()
        except:
            pass
        self._cleanup()
        self._tpc_cleanup()

    def tpc_abort(self, transaction):
        self._tpc_cleanup()
        self.abort(transaction)

    def sortKey(self):
        return ('PJDataManager', 0)

    def _report_stats(self):
        if not PJ_ENABLE_QUERY_STATS:
            return

        stats = self._query_report.calc_and_report()
        TABLE_LOG.info(stats)


def get_database_name_from_dsn(dsn):
    m = re.match(r'.*dbname *= *(.+?)( |$)', dsn)
    if m:
        return m.groups()[0]

    # process connection string like postgresql://[user[:password]@][netloc][:port][/dbname][?param1=value1&...]
    import urlparse
    u = urlparse.urlparse(dsn)
    if u.path:
        return u.path.strip('/')

    LOG.warning("Cannot determine database name from DSN '%s'" % dsn)
    return None
