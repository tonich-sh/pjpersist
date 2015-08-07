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
"""PostGreSQL/JSONB Persistence Zope Containers Tests"""
import atexit
import doctest
import unittest

import mock
import ZODB
import ZODB.DemoStorage
import persistent
import psycopg2
import random
import re
import transaction
import zope.component
import zope.interface
import zope.lifecycleevent
from pprint import pprint
from zope.exceptions import exceptionformatter
from zope.app.testing import placelesssetup
from zope.container.interfaces import IContainer
from zope.container import contained, btree
from zope.interface.verify import verifyObject
from zope.interface.verify import verifyClass
from zope.testing import cleanup, module, renormalizing

from pjpersist import datamanager, interfaces, serialize, testing
from pjpersist.zope import container
from pjpersist.zope import interfaces as zinterfaces

DBNAME = 'pjpersist_container_test'


class ApplicationRoot(container.SimplePJContainer):
    _p_pj_table = 'root'

    def __repr__(self):
        return '<ApplicationRoot>'


class SimplePerson(contained.Contained, persistent.Persistent):
    _p_pj_table = 'person'

    def __init__(self, name):
        self.name = name

    def __str__(self):
        return self.name

    def __repr__(self):
        return '<%s %s>' %(self.__class__.__name__, self)


class Person(container.PJContained, SimplePerson):
    pass


def doctest_PJContained_simple():
    """PJContained: simple use

    The simplest way to use PJContained is to use it without any special
    modification. In this case it is required that the container always sets
    the name and parent after loading the item. It can do so directly by
    setting ``_v_name`` and ``_v_parent`` so that the persistence mechanism
    does not kick in.

      >>> class Simples(container.PJContainer):
      ...     def __init__(self, name):
      ...         super(Simples, self).__init__()
      ...         self.name = name
      ...     def __repr__(self):
      ...         return '<Simples %s>' %self.name

      >>> class Simple(container.PJContained, persistent.Persistent):
      ...     pass

    Let's create a simple component and activate the persistence machinery:

      >>> s = Simple()
      >>> s._p_jar = dm

    As you can see, the changed flag is not changed:

      >>> s._p_changed
      False
      >>> s._v_name = 'simple'
      >>> s._v_parent = Simples('one')
      >>> s._p_changed
      False

    And accessing the name and parent works:

      >>> s.__name__
      'simple'
      >>> s.__parent__
      <Simples one>

    But assignment works as well.

      >>> s.__name__ = 'simple2'
      >>> s.__name__
      'simple2'
      >>> s.__parent__ = Simples('two')
      >>> s.__parent__
      <Simples two>
      >>> s._p_changed
      True
    """

def doctest_PJContained_proxy_attr():
    """PJContained: proxy attributes

    It is also possible to use proxy attributes to reference the name and
    parent. This allows you to have nice attribute names for storage in PJ.

    The main benefit, though is the ability of the object to load its
    location, so that you can load the object without going through the
    container and get full location path.

      >>> class Proxies(container.PJContainer):
      ...     def __init__(self, name):
      ...         super(Proxies, self).__init__()
      ...         self.name = name
      ...     def __repr__(self):
      ...         return '<Proxies %s>' %self.name

      >>> class Proxy(container.PJContained, persistent.Persistent):
      ...     _pj_name_attr = 'name'
      ...     _pj_parent_attr = 'parent'
      ...     def __init__(self, name, parent):
      ...         self.name = name
      ...         self.parent = parent

    Let's create a proxy component and activate the persistence machinery:

      >>> p = Proxy('proxy', Proxies('one'))
      >>> p._p_jar = dm

    So accessing the name and parent works:

      >>> p.__name__
      'proxy'
      >>> p.__parent__
      <Proxies one>

    But assignment is only stored into the volatile variables and the proxy
    attribute values are not touched.

      >>> p.__name__ = 'proxy2'
      >>> p.__name__
      'proxy2'
      >>> p.name
      'proxy'
      >>> p.__parent__ = Proxies('two')
      >>> p.__parent__
      <Proxies two>
      >>> p.parent
      <Proxies one>

    This behavior is intentional, so that containment machinery cannot mess
    with the real attributes. Note that in practice, only PJContainer sets
    the ``__name__`` and ``__parent__`` and it should be always consistent
    with the referenced attributes.

    """

def doctest_PJContained_setter_getter():
    """PJContained: setter/getter functions

    If you need ultimate flexibility of where to get and store the name and
    parent, then you can define setters and getters.

      >>> class Funcs(container.PJContainer):
      ...     def __init__(self, name):
      ...         super(Funcs, self).__init__()
      ...         self.name = name
      ...     def __repr__(self):
      ...         return '<Funcs %s>' %self.name

      >>> class Func(container.PJContained, persistent.Persistent):
      ...     _pj_name_getter = lambda s: s.name
      ...     _pj_name_setter = lambda s, v: setattr(s, 'name', v)
      ...     _pj_parent_getter = lambda s: s.parent
      ...     _pj_parent_setter = lambda s, v: setattr(s, 'parent', v)
      ...     def __init__(self, name, parent):
      ...         self.name = name
      ...         self.parent = parent

    Let's create a func component and activate the persistence machinery:

      >>> f = Func('func', Funcs('one'))
      >>> f._p_jar = dm

    So accessing the name and parent works:

      >>> f.__name__
      'func'
      >>> f.__parent__
      <Funcs one>

    In this case, the setters are used, if the name and parent are changed:

      >>> f.__name__ = 'func2'
      >>> f.__name__
      'func2'
      >>> f.name
      'func2'
      >>> f.__parent__ = Funcs('two')
      >>> f.__parent__
      <Funcs two>
      >>> f.parent
      <Funcs two>
    """


def doctest_PJContained_mixed():
    """PJContained: mixed usage

    When the container is stored in the ZODB or another persistence mechanism,
    a mixed usage of proxy attributes and getter/setter functions is the best
    approach.

      >>> class Mixers(btree.BTreeContainer):
      ...     def __init__(self, name):
      ...         super(Mixers, self).__init__()
      ...         self.name = name
      ...     def __repr__(self):
      ...         return '<Mixers %s>' %self.name
      >>> mixers = Mixers('one')

      >>> class Mixer(container.PJContained, persistent.Persistent):
      ...     _pj_name_attr = 'name'
      ...     _pj_parent_getter = lambda s: mixers
      ...     def __init__(self, name):
      ...         self.name = name

    Let's create a mixer component and activate the persistence machinery:

      >>> m = Mixer('mixer')
      >>> m._p_jar = dm

    So accessing the name and parent works:

      >>> m.__name__
      'mixer'
      >>> m.__parent__
      <Mixers one>
    """


def doctest_SimplePJContainer_basic():
    """SimplePJContainer: basic

      >>> cn = 'pjpersist_dot_zope_dot_container_dot_SimplePJContainer'

    Let's make sure events are fired correctly:

      >>> zope.component.provideHandler(handleObjectModifiedEvent)

    Let's add a container to the root:

      >>> dm.root['c'] = container.SimplePJContainer()

      >>> dumpTable(cn)
      [{'data': {u'_py_persistent_type': u'pjpersist.zope.container.SimplePJContainer',
                 u'data': {}},
        'id': u'0001020304050607080a0b0c0'}]

    As you can see, the serialization is very clean. Next we add a person.

      >>> dm.root['c'][u'stephan'] = SimplePerson(u'Stephan')
      ContainerModifiedEvent: <...SimplePJContainer ...>
      >>> dm.root['c'].keys()
      [u'stephan']
      >>> dm.root['c'][u'stephan']
      <SimplePerson Stephan>

      >>> dm.root['c']['stephan'].__parent__
      <pjpersist.zope.container.SimplePJContainer object at 0x7fec50f86500>
      >>> dm.root['c']['stephan'].__name__
      u'stephan'

    You can also access objects using the ``get()`` method of course:

      >>> stephan = dm.root['c'].get(u'stephan')
      >>> stephan.__parent__
      <pjpersist.zope.container.SimplePJContainer object at 0x7fec50f86500>
      >>> stephan.__name__
      u'stephan'

    Let's commit and access the data again:

      >>> transaction.commit()

      >>> dumpTable('person')
      [{'data': {u'__name__': u'stephan',
                 u'__parent__': {u'_py_type': u'DBREF',
                                 u'database': u'pjpersist_test',
                                 u'id': u'0001020304050607080a0b0c0',
                                 u'table': u'pjpersist_dot_zope_dot_container_dot_SimplePJContainer'},
                 u'_py_persistent_type': u'pjpersist.zope.tests.test_container.SimplePerson',
                 u'name': u'Stephan'},
        'id': u'0001020304050607080a0b0c0'}]

      >>> dm.root['c'].keys()
      [u'stephan']
      >>> dm.root['c']['stephan'].__parent__
      <pjpersist.zope.container.SimplePJContainer object at 0x7fec50f86500>
      >>> dm.root['c']['stephan'].__name__
      u'stephan'

      >>> dumpTable(cn)
      [{'data': {u'_py_persistent_type': u'pjpersist.zope.container.SimplePJContainer',
                 u'data': {u'stephan': {u'_py_type': u'DBREF',
                                        u'database': u'pjpersist_test',
                                        u'id': u'0001020304050607080a0b0c0',
                                        u'table': u'person'}}},
        'id': u'0001020304050607080a0b0c0'}]

      >>> dm.root['c'].items()
      [(u'stephan', <SimplePerson Stephan>)]

      >>> dm.root['c'].values()
      [<SimplePerson Stephan>]

    Now remove the item:

      >>> del dm.root['c']['stephan']
      ContainerModifiedEvent: <...SimplePJContainer ...>

    The changes are immediately visible.

      >>> dm.root['c'].keys()
      []
      >>> dm.root['c']['stephan']
      Traceback (most recent call last):
      ...
      KeyError: 'stephan'

    Make sure it is really gone after committing:

      >>> transaction.commit()
      >>> dm.root['c'].keys()
      []

    The object is also removed from PJ:

      >>> dumpTable('person')
      []

    Check adding of more objects:

      >>> dm.root['c'][u'roy'] = SimplePerson(u'Roy')
      ContainerModifiedEvent: <...SimplePJContainer ...>
      >>> dm.root['c'][u'adam'] = SimplePerson(u'Adam')
      ContainerModifiedEvent: <...SimplePJContainer ...>
      >>> dm.root['c'][u'marius'] = SimplePerson(u'Marius')
      ContainerModifiedEvent: <...SimplePJContainer ...>

      >>> sorted(dm.root['c'].keys())
      [u'adam', u'marius', u'roy']

    """


def doctest_PJContainer_basic():
    """PJContainer: basic

    Let's make sure events are fired correctly:

      >>> zope.component.provideHandler(handleObjectModifiedEvent)

    Let's add a container to the root:

      >>> transaction.commit()
      >>> dm.root['c'] = container.PJContainer('person')

      >>> dumpTable('pjpersist_dot_zope_dot_container_dot_PJContainer')
      [{'data': {u'_pj_table': u'person',
                 u'_py_persistent_type': u'pjpersist.zope.container.PJContainer'},
        'id': u'0001020304050607080a0b0c0'}]

    It is unfortunate that the '_pj_table' attribute is set. This is
    avoidable using a sub-class.

      >>> dm.root['c'][u'stephan'] = Person(u'Stephan')
      ContainerModifiedEvent: <...PJContainer ...>
      >>> dm.root['c'].keys()
      [u'stephan']
      >>> dm.root['c'][u'stephan']
      <Person Stephan>

      >>> dm.root['c']['stephan'].__parent__
      <pjpersist.zope.container.PJContainer object at 0x7fec50f86500>
      >>> dm.root['c']['stephan'].__name__
      u'stephan'

    It is a feature of the container that the item is immediately available
    after assignment, but before the data is stored in the database. Let's
    commit and access the data again:

      >>> transaction.commit()

      >>> dumpTable('person')
      [{'data': {u'_py_persistent_type': u'pjpersist.zope.tests.test_container.Person',
                 u'key': u'stephan',
                 u'name': u'Stephan',
                 u'parent': {u'_py_type': u'DBREF',
                             u'database': u'pjpersist_test',
                             u'id': u'0001020304050607080a0b0c0',
                             u'table': u'pjpersist_dot_zope_dot_container_dot_PJContainer'}},
        'id': u'0001020304050607080a0b0c0'}]

      >>> 'stephan' in dm.root['c']
      True
      >>> dm.root['c'].keys()
      [u'stephan']
      >>> dm.root['c']['stephan'].__parent__
      <pjpersist.zope.container.PJContainer object at 0x7fec50f86500>
      >>> dm.root['c']['stephan'].__name__
      u'stephan'

    We get a usual key error, if an object does not exist:

      >>> dm.root['c']['roy']
      Traceback (most recent call last):
      ...
      KeyError: 'roy'

      >>> 'roy' in dm.root['c']
      False

    Now remove the item:

      >>> del dm.root['c']['stephan']
      ContainerModifiedEvent: <...PJContainer ...>

    The changes are immediately visible.

      >>> dm.root['c'].keys()
      []
      >>> dm.root['c']['stephan']
      Traceback (most recent call last):
      ...
      KeyError: 'stephan'

    Make sure it is really gone after committing:

      >>> transaction.commit()
      >>> dm.root['c'].keys()
      []

    Check adding of more objects:

      >>> dm.root['c'][u'roy'] = SimplePerson(u'Roy')
      ContainerModifiedEvent: <...PJContainer ...>
      >>> dm.root['c'][u'adam'] = SimplePerson(u'Adam')
      ContainerModifiedEvent: <...PJContainer ...>
      >>> dm.root['c'][u'marius'] = SimplePerson(u'Marius')
      ContainerModifiedEvent: <...PJContainer ...>

      >>> sorted(dm.root['c'].keys())
      [u'adam', u'marius', u'roy']
    """

def doctest_PJContainer_constructor():
    """PJContainer: constructor

    The constructor of the PJContainer class has several advanced arguments
    that allow customizing the storage options.

      >>> transaction.commit()
      >>> c = container.PJContainer(
      ...     'person',
      ...     mapping_key = 'name',
      ...     parent_key = 'site')

      >>> c._pj_mapping_key
      'name'

    The parent key is the key/attribute in which the parent reference is
    stored. This is used to suport multiple containers per PJ table.

      >>> c._pj_parent_key
      'site'
    """

def doctest_PJContainer_pj_parent_key_value():
    r"""PJContainer: _pj_parent_key_value()

    This method is used to extract the parent reference for the item.

      >>> c = container.PJContainer('person')

    The default implementation requires the container to be in some sort of
    persistent store, though it does not care whether this store is PJ or a
    classic ZODB. This feature allows one to mix and match ZODB and PJ
    storage.

      >>> c._pj_get_parent_key_value()
      Traceback (most recent call last):
      ...
      ValueError: _p_jar not found.

    Now the ZODB case:

      >>> c._p_jar = object()
      >>> c._p_oid = '\x00\x00\x00\x00\x00\x00\x00\x01'
      >>> c._pj_get_parent_key_value()
      'zodb-0000000000000001'

    And finally the PJ case:

      >>> c._p_jar = c._p_oid = None
      >>> dm.root['people'] = c
      >>> c._pj_get_parent_key_value()
      <pjpersist.zope.container.PJContainer object at 0x32deed8>

    In that final case, the container itself is returned, because upon
    serialization, we simply look up the dbref.
    """

def doctest_PJContainer_many_items():
    """PJContainer: many items

    Let's create an interesting set of data:

      >>> transaction.commit()
      >>> dm.root['people'] = container.PJContainer('person')
      >>> dm.root['people'][u'stephan'] = Person(u'Stephan')
      >>> dm.root['people'][u'roy'] = Person(u'Roy')
      >>> dm.root['people'][u'roger'] = Person(u'Roger')
      >>> dm.root['people'][u'adam'] = Person(u'Adam')
      >>> dm.root['people'][u'albertas'] = Person(u'Albertas')
      >>> dm.root['people'][u'russ'] = Person(u'Russ')

    In order for find to work, the data has to be committed:

      >>> transaction.commit()

    Let's now search and receive documents as result:

      >>> sorted(dm.root['people'].keys())
      [u'adam', u'albertas', u'roger', u'roy', u'russ', u'stephan']
      >>> dm.root['people'][u'stephan']
      <Person Stephan>
      >>> dm.root['people'][u'adam']
      <Person Adam>
"""

def doctest_PJContainer_setitem_with_no_key_PJContainer():
    """PJContainer: __setitem__(None, obj)

    Whenever an item is added with no key, getattr(obj, _pj_mapping_key) is used.

      >>> transaction.commit()
      >>> dm.root['people'] = container.PJContainer(
      ...     'person', mapping_key='name')
      >>> dm.root['people'][None] = Person(u'Stephan')

    Let's now search and receive documents as result:

      >>> sorted(dm.root['people'].keys())
      [u'...']
      >>> stephan = dm.root['people'].values()[0]
      >>> stephan.__name__ == str(stephan.name)
      True
"""

def doctest_PJContainer_setitem_with_no_key_IdNamesPJContainer():
    """IdNamesPJContainer: __setitem__(None, obj)

    Whenever an item is added with no key, the OID is used.

      >>> transaction.commit()
      >>> dm.root['people'] = container.IdNamesPJContainer('person')
      >>> dm.root['people'][None] = Person(u'Stephan')

    Let's now search and receive documents as result:

      >>> sorted(dm.root['people'].keys())
      [u'...']
      >>> stephan = dm.root['people'].values()[0]
      >>> stephan.__name__ == str(stephan._p_oid.id)
      True
"""

def doctest_PJContainer_add_PJContainer():
    """PJContainer: add(value, key=None)

    Sometimes we just do not want to be responsible to determine the name of
    the object to be added. This method makes this optional. The default
    implementation assigns getattr(obj, _pj_mapping_key) as name:

      >>> transaction.commit()
      >>> dm.root['people'] = container.PJContainer(
      ...     'person', mapping_key='name')
      >>> dm.root['people'].add(Person(u'Stephan'))

    Let's now search and receive documents as result:

      >>> sorted(dm.root['people'].keys())
      [u'...']
      >>> stephan = dm.root['people'].values()[0]
      >>> stephan.__name__ == str(stephan.name)
      True
"""

def doctest_PJContainer_add_IdNamesPJContainer():
    """IdNamesPJContainer: add(value, key=None)

    Sometimes we just do not want to be responsible to determine the name of
    the object to be added. This method makes this optional. The default
    implementation assigns the OID as name:

      >>> transaction.commit()
      >>> dm.root['people'] = container.IdNamesPJContainer('person')
      >>> dm.root['people'].add(Person(u'Stephan'))

    Let's now search and receive documents as result:

      >>> sorted(dm.root['people'].keys())
      [u'...']
      >>> stephan = dm.root['people'].values()[0]
      >>> stephan.__name__ == str(stephan._p_oid.id)
      True
"""


def doctest_PJContainer_bool():
  """PJContainers can be evaluated to boolean, however this
  results in an extra query

      >>> dm.root['people'] = container.PJContainer('person')
      >>> bool(dm.root['people'])
      False

      >>> dm.root['people'][u'stephan'] = Person(u'Stephan')
      >>> dm.root['people'][u'roy'] = Person(u'Roy')

  Enable query statistics and make sure we issue COUNT(*) query instead of
  fetching all the data from the table.

      >>> dm.flush()
      >>> dm._query_report.qlog = []
      >>> with mock.patch.object(datamanager, "PJ_ENABLE_QUERY_STATS", True):
      ...   bool(dm.root['people'])
      True

      >>> dm._query_report.qlog[-1].query
      'SELECT COUNT(person.id) FROM person...'
  """


def doctest_PJContainer_find():
    r"""PJContainer: find

    The PJ Container supports direct PJ queries. It does, however,
    insert the additional container filter arguments and can optionally
    convert the documents to objects.

    Let's create an interesting set of data:

      >>> transaction.commit()
      >>> dm.root['people'] = container.PJContainer('person')
      >>> dm.root['people'][u'stephan'] = Person(u'Stephan')
      >>> dm.root['people'][u'roy'] = Person(u'Roy')
      >>> dm.root['people'][u'roger'] = Person(u'Roger')
      >>> dm.root['people'][u'adam'] = Person(u'Adam')
      >>> dm.root['people'][u'albertas'] = Person(u'Albertas')
      >>> dm.root['people'][u'russ'] = Person(u'Russ')

    In order for find to work, the data has to be committed:

      >>> transaction.commit()

    Let's now search and receive documents as result:

      >>> import pjpersist.sqlbuilder as sb

      >>> datafld = sb.Field('person', 'data')
      >>> fld = sb.JSON_GETITEM_TEXT(datafld, 'name')
      >>> qry = fld.startswith('Ro')

      # >>> qry.__sqlrepr__('postgres')
      # "(((person.data) ->> ('name')) LIKE ('Ro%') ESCAPE E'\\\\')"

      >>> res = dm.root['people'].raw_find(qry)
      >>> pprint(list(res))
      [[u'0001020304050607080a0b0c0',
        {u'_py_persistent_type': u'pjpersist.zope.tests.test_container.Person',
         u'key': u'roy',
         u'name': u'Roy',
         u'parent': {u'_py_type': u'DBREF',
                     u'database': u'pjpersist_test',
                     u'id': u'0001020304050607080a0b0c0',
                     u'table': u'pjpersist_dot_zope_dot_container_dot_PJContainer'}}],
       [u'0001020304050607080a0b0c0',
        {u'_py_persistent_type': u'pjpersist.zope.tests.test_container.Person',
         u'key': u'roger',
         u'name': u'Roger',
         u'parent': {u'_py_type': u'DBREF',
                     u'database': u'pjpersist_test',
                     u'id': u'0001020304050607080a0b0c0',
                     u'table': u'pjpersist_dot_zope_dot_container_dot_PJContainer'}}]]

    And now the same query, but this time with object results:

      >>> res = dm.root['people'].find(qry)
      >>> pprint(list(res))
      [<Person Roy>, <Person Roger>]

    When no spec is specified, all items are returned:

      >>> res = dm.root['people'].find()
      >>> pprint(list(res))
      [<Person Stephan>, <Person Roy>, <Person Roger>, <Person Adam>,
       <Person Albertas>, <Person Russ>]


    Let's see some sqlbuilder parameters (passed along as kwargs):

      >>> res = dm.root['people'].raw_find(orderBy=["(data->'name')"])
      >>> pprint(list(res))
      [[u'0001020304050607080a0b0c0',
        {u'_py_persistent_type': u'pjpersist.zope.tests.test_container.Person',
         u'key': u'adam',
         u'name': u'Adam',
         u'parent': {u'_py_type': u'DBREF',
                     u'database': u'pjpersist_test',
                     u'id': u'0001020304050607080a0b0c0',
                     u'table': u'pjpersist_dot_zope_dot_container_dot_PJContainer'}}],
       [u'0001020304050607080a0b0c0',
        {u'_py_persistent_type': u'pjpersist.zope.tests.test_container.Person',
         u'key': u'albertas',
         u'name': u'Albertas',
         u'parent': {u'_py_type': u'DBREF',
                     u'database': u'pjpersist_test',
                     u'id': u'0001020304050607080a0b0c0',
                     u'table': u'pjpersist_dot_zope_dot_container_dot_PJContainer'}}],
       [u'0001020304050607080a0b0c0',
        {u'_py_persistent_type': u'pjpersist.zope.tests.test_container.Person',
         u'key': u'roger',
         u'name': u'Roger',
         u'parent': {u'_py_type': u'DBREF',
                     u'database': u'pjpersist_test',
                     u'id': u'0001020304050607080a0b0c0',
                     u'table': u'pjpersist_dot_zope_dot_container_dot_PJContainer'}}],
       [u'0001020304050607080a0b0c0',
        {u'_py_persistent_type': u'pjpersist.zope.tests.test_container.Person',
         u'key': u'roy',
         u'name': u'Roy',
         u'parent': {u'_py_type': u'DBREF',
                     u'database': u'pjpersist_test',
                     u'id': u'0001020304050607080a0b0c0',
                     u'table': u'pjpersist_dot_zope_dot_container_dot_PJContainer'}}],
       [u'0001020304050607080a0b0c0',
        {u'_py_persistent_type': u'pjpersist.zope.tests.test_container.Person',
         u'key': u'russ',
         u'name': u'Russ',
         u'parent': {u'_py_type': u'DBREF',
                     u'database': u'pjpersist_test',
                     u'id': u'0001020304050607080a0b0c0',
                     u'table': u'pjpersist_dot_zope_dot_container_dot_PJContainer'}}],
       [u'0001020304050607080a0b0c0',
        {u'_py_persistent_type': u'pjpersist.zope.tests.test_container.Person',
         u'key': u'stephan',
         u'name': u'Stephan',
         u'parent': {u'_py_type': u'DBREF',
                     u'database': u'pjpersist_test',
                     u'id': u'0001020304050607080a0b0c0',
                     u'table': u'pjpersist_dot_zope_dot_container_dot_PJContainer'}}]]

      >>> res = dm.root['people'].raw_find(orderBy=["(data->'name') DESC"], limit=1)
      >>> pprint(list(res))
      [[u'0001020304050607080a0b0c0',
        {u'_py_persistent_type': u'pjpersist.zope.tests.test_container.Person',
         u'key': u'stephan',
         u'name': u'Stephan',
         u'parent': {u'_py_type': u'DBREF',
                     u'database': u'pjpersist_test',
                     u'id': u'0001020304050607080a0b0c0',
                     u'table': u'pjpersist_dot_zope_dot_container_dot_PJContainer'}}]]


      >>> res = dm.root['people'].find(orderBy=["(data->'name')"])
      >>> pprint(list(res))
      [<Person Adam>,
       <Person Albertas>,
       <Person Roger>,
       <Person Roy>,
       <Person Russ>,
       <Person Stephan>]

      >>> res = dm.root['people'].find(orderBy=["(data->'name') DESC"], limit=1)
      >>> pprint(list(res))
      [<Person Stephan>]


    You can also search for a single result:

      >>> qry2 = fld.startswith('St')
      >>> res = dm.root['people'].raw_find_one(qry2)
      >>> pprint(res)
      [u'0001020304050607080a0b0c0',
       {u'_py_persistent_type': u'pjpersist.zope.tests.test_container.Person',
        u'key': u'stephan',
        u'name': u'Stephan',
        u'parent': {u'_py_type': u'DBREF',
                    u'database': u'pjpersist_test',
                    u'id': u'0001020304050607080a0b0c0',
                    u'table': u'pjpersist_dot_zope_dot_container_dot_PJContainer'}}]

      >>> stephan = dm.root['people'].find_one(qry2)
      >>> pprint(stephan)
      <Person Stephan>

    If no result is found, ``None`` is returned:

      >>> qry3 = fld.startswith('XXX')
      >>> dm.root['people'].find_one(qry3)

    A query or ID must be passed:

      >>> dm.root['people'].find_one()
      Traceback (most recent call last):
      ...
      ValueError: Missing parameter, at least qry or id must be specified.

    On the other hand, if the spec is an id, we look for it instead:

      >>> dm.root['people'].find_one(id=stephan._p_oid.id)
      <Person Stephan>
    """

def doctest_PJ_Container_count():
  """
  count() provides a quick way to count items without fetching them from database

      >>> import pjpersist.sqlbuilder as sb

      >>> transaction.commit()
      >>> dm.root['people'] = container.PJContainer('person')
      >>> dm.root['people'].count()
      0L

      >>> dm.root['people'][u'stephan'] = Person(u'Stephan')
      >>> dm.root['people'][u'roy'] = Person(u'Roy')
      >>> dm.root['people'][u'roger'] = Person(u'Roger')
      >>> dm.root['people'][u'adam'] = Person(u'Adam')
      >>> dm.root['people'][u'albertas'] = Person(u'Albertas')
      >>> dm.root['people'][u'russ'] = Person(u'Russ')
      >>> dm.root['people'].count()
      6L

      >>> table = Person._p_pj_table
      >>> datafld = sb.Field('person', 'data')
      >>> fld = sb.JSON_GETITEM_TEXT(datafld, 'name')
      >>> qry = fld.startswith('Ro')
      >>> dm.root['people'].count(qry)
      2L
  """


def doctest_PJContainer_cache_complete():
    """PJContainer: _cache_complete

    Let's add a bunch of objects:

      >>> transaction.commit()
      >>> ppl = dm.root['people'] = container.PJContainer('person')
      >>> ppl[u'stephan'] = Person(u'Stephan')
      >>> ppl[u'roy'] = Person(u'Roy')
      >>> ppl[u'roger'] = Person(u'Roger')
      >>> ppl[u'adam'] = Person(u'Adam')
      >>> ppl[u'albertas'] = Person(u'Albertas')
      >>> ppl[u'russ'] = Person(u'Russ')

    Clean the cache on the transaction:

      >>> txn = transaction.manager.get()
      >>> if hasattr(txn, '_v_pj_container_cache'):
      ...     delattr(txn, '_v_pj_container_cache')

    The cache is not complete:

      >>> ppl._cache_complete
      False

    We have 6 objects

      >>> len(ppl.items())
      6

    The cache is complete if it's on

      >>> ppl._cache_complete == container.USE_CONTAINER_CACHE
      True

    Del 1

      >>> del ppl['adam']

    5 remain

      >>> len(ppl.items())
      5

    Add 1

      >>> ppl['joe'] = Person('Joe')

    Back to 6

      >>> len(ppl.items())
      6

    The cache is still complete if it's on

      >>> ppl._cache_complete == container.USE_CONTAINER_CACHE
      True

    Clearing the container

      >>> ppl.clear()
      >>> len(ppl.items())
      0

      >>> ppl._cache_complete == container.USE_CONTAINER_CACHE
      True

    """

def doctest_IdNamesPJContainer_basic():
    """IdNamesPJContainer: basic

    This container uses the PJ ObjectId as the name for each object. Since
    ObjectIds are required to be unique within a table, this is actually
    a nice and cheap scenario.

    Let's add a container to the root:

      >>> transaction.commit()
      >>> dm.root['c'] = container.IdNamesPJContainer('person')

    Let's now add a new person:

      >>> dm.root['c'].add(Person(u'Stephan'))
      >>> keys = dm.root['c'].keys()
      >>> keys
      [u'0001020304050607080a0b0c0']
      >>> name = keys[0]
      >>> dm.root['c'][name]
      <Person Stephan>

      >>> dm.root['c'].values()
      [<Person Stephan>]

      >>> dm.root['c'][name].__parent__
      <pjpersist.zope.container.IdNamesPJContainer object at 0x7fec50f86500>
      >>> dm.root['c'][name].__name__
      u'0001020304050607080a0b0c0'

    It is a feature of the container that the item is immediately available
    after assignment, but before the data is stored in the database. Let's
    commit and access the data again:

      >>> transaction.commit()

      >>> dumpTable('person')
      [{'data': {u'_py_persistent_type': u'pjpersist.zope.tests.test_container.Person',
                 u'name': u'Stephan',
                 u'parent': {u'_py_type': u'DBREF',
                             u'database': u'pjpersist_test',
                             u'id': u'0001020304050607080a0b0c0',
                             u'table': u'pjpersist_dot_zope_dot_container_dot_IdNamesPJContainer'}},
        'id': u'0001020304050607080a0b0c0'}]

    Notice how there is no "key" entry in the document. We get a usual key
    error, if an object does not exist:

      >>> dm.root['c']['0001020304050607080a0b0c0']
      Traceback (most recent call last):
      ...
      KeyError: '0001020304050607080a0b0c0'

      >>> '0001020304050607080a0b0c0' in dm.root['c']
      False

      >>> dm.root['c']['roy']
      Traceback (most recent call last):
      ...
      KeyError: 'roy'

      >>> 'roy' in dm.root['c']
      False

    Now remove the item:

      >>> dm.root['c'][name]
      <Person Stephan>

      >>> del dm.root['c'][name]

    The changes are immediately visible.

      >>> dm.root['c'].keys()
      []
      >>> dm.root['c'][name]
      Traceback (most recent call last):
      ...
      KeyError: u'0001020304050607080a0b0c0'

    Make sure it is really gone after committing:

      >>> transaction.commit()
      >>> dm.root['c'].keys()
      []
    """

def doctest_AllItemsPJContainer_basic():
    """AllItemsPJContainer: basic

    This type of container returns all items of the table without regard
    of a parenting hierarchy.

    Let's start by creating two person containers that service different
    purposes:

      >>> dm.root['friends'] = container.PJContainer('person')
      >>> dm.root['friends'][u'roy'] = Person(u'Roy')
      >>> dm.root['friends'][u'roger'] = Person(u'Roger')

      >>> dm.root['family'] = container.PJContainer('person')
      >>> dm.root['family'][u'anton'] = Person(u'Anton')
      >>> dm.root['family'][u'konrad'] = Person(u'Konrad')

      >>> transaction.commit()
      >>> sorted(dm.root['friends'].keys())
      [u'roger', u'roy']
      >>> sorted(dm.root['family'].keys())
      [u'anton', u'konrad']

    Now we can create an all-items-container that allows us to view all
    people.

      >>> dm.root['all-people'] = container.AllItemsPJContainer('person')
      >>> sorted(dm.root['all-people'].keys())
      [u'anton', u'konrad', u'roger', u'roy']
    """

def doctest_SubDocumentPJContainer_basic():
    r"""SubDocumentPJContainer: basic

    Let's make sure events are fired correctly:

      >>> zope.component.provideHandler(handleObjectModifiedEvent)

    Sub_document PJ containers are useful, since they avoid the creation of
    a commonly trivial tables holding meta-data for the table
    object. But they require a root document:

      >>> dm.root['app_root'] = ApplicationRoot()

    Let's add a container to the app root:

      >>> dm.root['app_root']['people'] = \
      ...     container.SubDocumentPJContainer('person')
      ContainerModifiedEvent: <ApplicationRoot>

      >>> transaction.commit()
      >>> dumpTable('root')
      [{'data':
        {u'_py_persistent_type': u'pjpersist.zope.tests.test_container.ApplicationRoot',
         u'data':
           {u'people': {u'_pj_table': u'person',
                        u'_py_persistent_type': u'pjpersist.zope.container.SubDocumentPJContainer'}}},
        'id': u'0001020304050607080a0b0c0'}]

    It is unfortunate that the '_pj_table' attribute is set. This is
    avoidable using a sub-class. Let's make sure the container can be loaded
    correctly:

      >>> dm.root['app_root']['people']
      <pjpersist.zope.container.SubDocumentPJContainer ...>
      >>> dm.root['app_root']['people'].__parent__
      <ApplicationRoot>
      >>> dm.root['app_root']['people'].__name__
      'people'

    Let's add an item to the container:

      >>> dm.root['app_root']['people'][u'stephan'] = Person(u'Stephan')
      ContainerModifiedEvent: <...SubDocumentPJContainer ...>
      >>> dm.root['app_root']['people'].keys()
      [u'stephan']
      >>> dm.root['app_root']['people'][u'stephan']
      <Person Stephan>

      >>> transaction.commit()
      >>> dm.root['app_root']['people'].keys()
      [u'stephan']
    """

def doctest_PJContainer_with_ZODB():
    r"""PJContainer: with ZODB

    This test demonstrates how a PJ Container lives inside a ZODB tree:

      >>> zodb = ZODB.DB(ZODB.DemoStorage.DemoStorage())
      >>> root = zodb.open().root()
      >>> root['app'] = btree.BTreeContainer()
      >>> root['app']['people'] = container.PJContainer('person')

    Let's now commit the transaction and make sure everything is cool.

      >>> transaction.commit()
      >>> root = zodb.open().root()
      >>> root['app']
      <zope.container.btree.BTreeContainer object at 0x7fbb5842f578>
      >>> root['app']['people']
      <pjpersist.zope.container.PJContainer object at 0x7fd6e23555f0>

    So let's try again:

      >>> root['app']['people'].keys()
      []

    Next we create a person object and make sure it gets properly persisted.

      >>> root['app']['people']['stephan'] = Person(u'Stephan')
      >>> transaction.commit()
      >>> root = zodb.open().root()
      >>> root['app']['people'].keys()
      [u'stephan']

      >>> stephan = root['app']['people']['stephan']
      >>> stephan.__name__
      u'stephan'
      >>> stephan.__parent__
      <pjpersist.zope.container.PJContainer object at 0x7f6b6273b7d0>

      >>> dumpTable('person')
      [{'data': {u'_py_persistent_type': u'pjpersist.zope.tests.test_container.Person',
                 u'key': u'stephan',
                 u'name': u'Stephan',
                 u'parent': u'zodb-01af3b00c5
        'id': u'0001020304050607080a0b0c0'}]

    Note that we produced a nice hex-presentation of the ZODB's OID.
    """


# classes for doctest_Realworldish
class Campaigns(container.PJContainer):
    _pj_table = 'campaigns'

    def __init__(self, name):
        self.name = name
        super(Campaigns, self).__init__()

    def add(self, campaign):
        self[campaign.name] = campaign

    def __repr__(self):
        return '<%s %s>' % (self.__class__.__name__, self.name)


class PJItem(container.PJContained,
                persistent.Persistent):
    pass


class Campaign(PJItem, container.PJContainer):
    _pj_table = 'persons'
    _p_pj_table = 'campaigns'

    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return '<%s %s>' % (self.__class__.__name__, self.name)


class PersonItem(PJItem):
    _p_pj_table = 'persons'

    def __init__(self, name):
        self.name = name

    def __str__(self):
        return self.name

    def __repr__(self):
        return '<%s %s>' % (self.__class__.__name__, self)


def doctest_Realworldish():
    """Let's see some real worldish hierarchic structure is persisted

    Let's make sure events are fired correctly:

      >>> zope.component.provideHandler(handleObjectModifiedEvent)

    Let's add a container to the root:

      >>> transaction.commit()
      >>> dm.root['c'] = Campaigns('foobar')

      >>> dumpTable(
      ...     'pjpersist_dot_zope_dot_tests_dot_test_container_dot_Campaigns')
      [{'data': {u'_py_persistent_type': u'pjpersist.zope.tests.test_container.Campaigns',
                 u'name': u'foobar'},
        'id': u'0001020304050607080a0b0c0'}]

    It is unfortunate that the '_pj_table' attribute is set. This is
    avoidable using a sub-class.

      >>> dm.root['c'][u'one'] = Campaign(u'one')
      ContainerModifiedEvent: <...Campaigns ...>
      >>> dm.root['c'].keys()
      [u'one']
      >>> dm.root['c'][u'one']
      <Campaign one>

      >>> dm.root['c']['one'].__parent__
      <Campaigns foobar>
      >>> dm.root['c']['one'].__name__
      u'one'

    It is a feature of the container that the item is immediately available
    after assignment, but before the data is stored in the database. Let's
    commit and access the data again:

      >>> transaction.commit()

      >>> dumpTable(Campaigns._pj_table)
      [{'data': {u'_py_persistent_type': u'pjpersist.zope.tests.test_container.Campaign',
                 u'key': u'one',
                 u'name': u'one',
                 u'parent': {u'_py_type': u'DBREF',
                             u'database': u'pjpersist_test',
                             u'id': u'0001020304050607080a0b0c0',
                             u'table': u'pjpersist_dot_zope_dot_tests_dot_test_container_dot_Campaigns'}},
        'id': u'0001020304050607080a0b0c0'}]

      >>> 'one' in dm.root['c']
      True
      >>> dm.root['c'].keys()
      [u'one']
      >>> dm.root['c']['one'].__parent__
      <Campaigns foobar>
      >>> dm.root['c']['one'].__name__
      u'one'

    We get a usual key error, if an object does not exist:

      >>> dm.root['c']['roy']
      Traceback (most recent call last):
      ...
      KeyError: 'roy'

      >>> 'roy' in dm.root['c']
      False

    Now remove the item:

      >>> del dm.root['c']['one']
      ContainerModifiedEvent: <...Campaigns ...>

    The changes are immediately visible.

      >>> dm.root['c'].keys()
      []
      >>> dm.root['c']['one']
      Traceback (most recent call last):
      ...
      KeyError: 'one'

    Make sure it is really gone after committing:

      >>> transaction.commit()
      >>> dm.root['c'].keys()
      []

    Check adding of more objects:

      >>> dm.root['c'][u'1'] = c1 = Campaign(u'One')
      ContainerModifiedEvent: <...Campaigns ...>
      >>> dm.root['c'][u'2'] = c2 = Campaign(u'Two')
      ContainerModifiedEvent: <...Campaigns ...>
      >>> dm.root['c'][u'3'] = Campaign(u'Three')
      ContainerModifiedEvent: <...Campaigns ...>

      >>> sorted(dm.root['c'].keys())
      [u'1', u'2', u'3']

    Check adding of more subitems:

      >>> stephan = c1['stephan'] = PersonItem('Stephan')
      ContainerModifiedEvent: <Campaign One>
      >>> roy = c1['roy'] = PersonItem('Roy')
      ContainerModifiedEvent: <Campaign One>

      >>> sorted(c1.keys())
      [u'roy', u'stephan']

      >>> adam = c2['adam'] = PersonItem('Adam')
      ContainerModifiedEvent: <Campaign Two>

      >>> sorted(c1.keys())
      [u'roy', u'stephan']
      >>> sorted(c2.keys())
      [u'adam']

    """


class People(container.AllItemsPJContainer):
    _pj_mapping_key = 'name'
    _p_pj_table = 'people'
    _pj_table = 'person'


class Address(persistent.Persistent):
    _p_pj_table = 'address'

    def __init__(self, city):
        self.city = city


class PeoplePerson(persistent.Persistent, container.PJContained):
    _p_pj_table = 'person'

    def __init__(self, name, age):
        self.name = name
        self.age = age
        self.address = Address('Boston %i' %age)

    def __repr__(self):
        return '<%s %s @ %i [%s]>' %(
            self.__class__.__name__, self.name, self.age, self.__name__)


def doctest_load_does_not_set_p_changed():
    """We need to guarantee that _p_changed is not True on obj load

    Let's add some objects:

      >>> transaction.commit()
      >>> dm.root['people'] = people = People()
      >>> for idx in xrange(2):
      ...     people[None] = PeoplePerson('Mr Number %.5i' %idx, random.randint(0, 100))
      >>> transaction.commit()

      >>> objs = [o for o in people.values()]
      >>> len(objs)
      2
      >>> [o._p_changed for o in objs]
      [False, False]

      >>> [o._p_changed for o in people.values()]
      [False, False]

      >>> transaction.commit()

      >>> x = transaction.begin()
      >>> [o._p_changed for o in people.values()]
      [False, False]

      >>> [o._p_changed for o in people.values()]
      [False, False]

    """


def doctest_firing_events_PJContainer():
    """Events need to be fired when _pj_mapping_key is already set on the object
    and the object gets added to the container

      >>> @zope.component.adapter(zope.component.interfaces.IObjectEvent)
      ... def eventHandler(event):
      ...     print event

      >>> zope.component.provideHandler(eventHandler)

    Let's add some objects:

      >>> transaction.commit()
      >>> dm.root['people'] = people = People()
      >>> for idx in xrange(2):
      ...     people[None] = PeoplePerson('Mr Number %.5i' %idx, random.randint(0, 100))
      <zope.lifecycleevent.ObjectAddedEvent object at ...>
      <zope.container.contained.ContainerModifiedEvent object at ...>
      <zope.lifecycleevent.ObjectAddedEvent object at ...>
      <zope.container.contained.ContainerModifiedEvent object at ...>
      >>> transaction.commit()
      >>> list(people.keys())
      [u'Mr Number 00000', u'Mr Number 00001']

      >>> for idx in xrange(2):
      ...     name = 'Mr Number %.5i' % (idx+10, )
      ...     people.add(PeoplePerson(name, random.randint(0, 100)))
      <zope.lifecycleevent.ObjectAddedEvent object at ...>
      <zope.container.contained.ContainerModifiedEvent object at ...>
      <zope.lifecycleevent.ObjectAddedEvent object at ...>
      <zope.container.contained.ContainerModifiedEvent object at ...>
      >>> transaction.commit()
      >>> list(people.keys())
      [u'Mr Number 00000', u'Mr Number 00001', u'Mr Number 00010', u'Mr Number 00011']

      >>> for idx in xrange(2):
      ...     name = 'Mr Number %.5i' % (idx+20, )
      ...     people[name] = PeoplePerson(name, random.randint(0, 100))
      <zope.lifecycleevent.ObjectAddedEvent object at ...>
      <zope.container.contained.ContainerModifiedEvent object at ...>
      <zope.lifecycleevent.ObjectAddedEvent object at ...>
      <zope.container.contained.ContainerModifiedEvent object at ...>
      >>> transaction.commit()
      >>> list(people.keys())
      [u'Mr Number 00000', u'Mr Number 00001', u'Mr Number 00010', u'Mr Number 00011',
       u'Mr Number 00020', u'Mr Number 00021']

    """


class PeopleWithIDKeys(container.IdNamesPJContainer):
    _p_pj_table = 'people'
    _pj_table = 'person'


def doctest_firing_events_IdNamesPJContainer():
    """Events need to be fired when the object gets added to the container

      >>> @zope.component.adapter(zope.component.interfaces.IObjectEvent)
      ... def eventHandler(event):
      ...     print event

      >>> zope.component.provideHandler(eventHandler)

    Let's add some objects:

      >>> transaction.commit()
      >>> dm.root['people'] = people = PeopleWithIDKeys()
      >>> for idx in xrange(2):
      ...     people[None] = PeoplePerson('Mr Number %.5i' %idx, random.randint(0, 100))
      <zope.lifecycleevent.ObjectAddedEvent object at ...>
      <zope.container.contained.ContainerModifiedEvent object at ...>
      <zope.lifecycleevent.ObjectAddedEvent object at ...>
      <zope.container.contained.ContainerModifiedEvent object at ...>
      >>> transaction.commit()
      >>> list(people.keys())
      [u'4e7ddf12e138237403000000', u'4e7ddf12e138237403000000']

      >>> for idx in xrange(2):
      ...     name = 'Mr Number %.5i' % (idx+10, )
      ...     people.add(PeoplePerson(name, random.randint(0, 100)))
      <zope.lifecycleevent.ObjectAddedEvent object at ...>
      <zope.container.contained.ContainerModifiedEvent object at ...>
      <zope.lifecycleevent.ObjectAddedEvent object at ...>
      <zope.container.contained.ContainerModifiedEvent object at ...>
      >>> transaction.commit()
      >>> list(people.keys())
      [u'4e7ddf12e138237403000000', u'4e7ddf12e138237403000000', u'4e7ddf12e138237403000000', u'4e7ddf12e138237403000000']

    We can set custom keys as well, they will end up in mongo documents as _id
    attributes.

      >>> for idx in xrange(2):
      ...     name = '4e7ddf12e1382374030%.5i' % (idx+20, )
      ...     people[name] = PeoplePerson(name, random.randint(0, 100))
      <zope.lifecycleevent.ObjectAddedEvent object at ...>
      <zope.container.contained.ContainerModifiedEvent object at ...>
      <zope.lifecycleevent.ObjectAddedEvent object at ...>
      <zope.container.contained.ContainerModifiedEvent object at ...>
      >>> transaction.commit()
      >>> list(people.keys())
      [u'4e7ddf12e138237403000000', u'4e7ddf12e138237403000000',
      u'4e7ddf12e138237403000000', u'4e7ddf12e138237403000000',
      u'4e7ddf12e138237403000000', u'4e7ddf12e138237403000000']

    """

import zope.schema
from pjpersist.persistent import SimpleColumnSerialization, select_fields


class ColumnPeople(container.AllItemsPJContainer):
    _pj_mapping_key = 'name'
    _p_pj_table = 'people'
    _pj_table = 'cperson'


class IPerson(zope.interface.Interface):
    name = zope.schema.TextLine(title=u'Name')
    address = zope.schema.TextLine(title=u'Address')
    visited = zope.schema.Datetime(title=u'Visited')
    phone = zope.schema.TextLine(title=u'Phone')


class ColumnPerson(SimpleColumnSerialization, container.PJContained,
                   persistent.Persistent):
    zope.interface.implements(IPerson)
    _p_pj_table = 'cperson'
    _pj_column_fields = select_fields(IPerson, 'name')

    def __init__(self, name, phone=None, address=None, friends=None,
                 visited=(), birthday=None):
        self.name = name
        self.address = address
        self.friends = friends or {}
        self.visited = visited
        self.phone = phone
        self.birthday = birthday

    def __str__(self):
        return self.name

    def __repr__(self):
        return '<%s %s>' % (self.__class__.__name__, self)


def doctest_PJContainer_SimpleColumnSerialization():
    """
    Check PJContainer methods when SimpleColumnSerialization is in effect

      >>> import pjpersist.sqlbuilder as sb

      >>> table = ColumnPerson._p_pj_table
      >>> dm._ensure_sql_columns(ColumnPerson(u'foo'), table)

      >>> dm.root['people'] = people = ColumnPeople()
      >>> for idx in xrange(20):
      ...     people[None] = ColumnPerson(u'Mr Number %.5i' %idx)

      >> dumpTable('cperson')

      >>> qry = sb.Field(table, 'name') == u'Mr Number 00010'
      >>> list(people.find(qry))
      [<ColumnPerson Mr Number 00010>]

      >>> pprint(list(people.raw_find(qry)))
      [[u'54894d3fb25d2b232e0046d6',
        u'Mr Number 00010',
        {u'_py_persistent_type': u'pjpersist.zope.tests.test_container.ColumnPerson',
         u'address': None,
         u'birthday': None,
         u'friends': {},
         u'name': u'Mr Number 00010',
         u'phone': None,
         u'visited': []}]]

      >>> people.find_one(qry)
      <ColumnPerson Mr Number 00010>

      >>> pprint(people.raw_find_one(qry))
      [u'54894d80b25d2b240f00bbf6',
       u'Mr Number 00010',
       {u'_py_persistent_type': u'pjpersist.zope.tests.test_container.ColumnPerson',
        u'address': None,
        u'birthday': None,
        u'friends': {},
        u'name': u'Mr Number 00010',
        u'phone': None,
        u'visited': []}]

      >>> people[u'Mr Number 00007']
      <ColumnPerson Mr Number 00007>

      >>> del people[u'Mr Number 00007']
      >>> people[u'Mr Number 00007']
      Traceback (most recent call last):
      ...
      KeyError: u'Mr Number 00007'

      >>> pprint(list(people))
      [u'Mr Number 00000',
       u'Mr Number 00001',
       u'Mr Number 00002',
       u'Mr Number 00003',
       u'Mr Number 00004',
       u'Mr Number 00005',
       u'Mr Number 00006',
       u'Mr Number 00008',
       u'Mr Number 00009',
       u'Mr Number 00010',
       u'Mr Number 00011',
       u'Mr Number 00012',
       u'Mr Number 00013',
       u'Mr Number 00014',
       u'Mr Number 00015',
       u'Mr Number 00016',
       u'Mr Number 00017',
       u'Mr Number 00018',
       u'Mr Number 00019']

      >>> pprint(list(people.keys()))
      [u'Mr Number 00000',
       u'Mr Number 00001',
       u'Mr Number 00002',
       u'Mr Number 00003',
       u'Mr Number 00004',
       u'Mr Number 00005',
       u'Mr Number 00006',
       u'Mr Number 00008',
       u'Mr Number 00009',
       u'Mr Number 00010',
       u'Mr Number 00011',
       u'Mr Number 00012',
       u'Mr Number 00013',
       u'Mr Number 00014',
       u'Mr Number 00015',
       u'Mr Number 00016',
       u'Mr Number 00017',
       u'Mr Number 00018',
       u'Mr Number 00019']
    """


def doctest_PJContainer_get_sb_fields():
    """Check _get_sb_fields behavour

      >>> c = ColumnPeople()

      >>> c._get_sb_fields(())
      cperson.*

      >>> c._get_sb_fields(None)
      cperson.*

      >>> def printit(res):
      ...     for r in res:
      ...           print r.__sqlrepr__('postgres')

      >>> printit(c._get_sb_fields(('name',)))
      ((cperson.data) -> ('name')) AS name

      >>> printit(c._get_sb_fields(('id', 'name',)))
      cperson.id
      ((cperson.data) -> ('name')) AS name

      >>> printit(c._get_sb_fields(('id', 'name', 'metadata.number')))
      cperson.id
      ((cperson.data) -> ('name')) AS name
      ((cperson.data) #> (array['metadata', 'number'])) AS metadata_number

    """


class ContainerConflictTest(testing.PJTestCase):

    def test_conflict_SimplePJContainer_key_1(self):
        """Check conflict detection. We add the same key in
        different transactions, simulating separate processes."""

        self.dm.root['c'] = container.SimplePJContainer()
        self.dm.tpc_finish(None)

        conn1 = testing.getConnection(testing.DBNAME)
        dm1 = datamanager.PJDataManager(conn1)

        dm1.root['c'][u'stephan'] = SimplePerson(u'Stephan')

        conn2 = testing.getConnection(testing.DBNAME)
        dm2 = datamanager.PJDataManager(conn2)

        dm2.root['c'][u'stephan'] = SimplePerson(u'Stephan')

        #Finish in order 2 - 1

        dm2.tpc_finish(None)
        with self.assertRaises(interfaces.ConflictError):
            dm1.tpc_finish(None)

        transaction.abort()

        conn2.close()
        conn1.close()

    def test_conflict_SimplePJContainer_key_2(self):
        """Check conflict detection. We add the same key in
        different transactions, simulating separate processes."""

        self.dm.root['c'] = container.SimplePJContainer()
        self.dm.tpc_finish(None)

        conn1 = testing.getConnection(testing.DBNAME)
        dm1 = datamanager.PJDataManager(conn1)

        dm1.root['c'][u'stephan'] = SimplePerson(u'Stephan')

        conn2 = testing.getConnection(testing.DBNAME)
        dm2 = datamanager.PJDataManager(conn2)

        dm2.root['c'][u'stephan'] = SimplePerson(u'Stephan')

        #Finish in order 1 - 2

        dm1.tpc_finish(None)
        with self.assertRaises(interfaces.ConflictError):
            dm2.tpc_finish(None)

        transaction.abort()

        conn2.close()
        conn1.close()

    def test_conflict_PJContainer_key_1(self):
        """Check conflict detection. We add the same key in
        different transactions, simulating separate processes."""

        self.dm.root['c'] = container.PJContainer('person')
        # (auto-create the table)
        self.dm.root['c'][u'roy'] = Person(u'Roy')
        self.dm.tpc_finish(None)

        conn1 = testing.getConnection(testing.DBNAME)
        dm1 = datamanager.PJDataManager(conn1)

        dm1.root['c'][u'stephan'] = Person(u'Stephan')

        conn2 = testing.getConnection(testing.DBNAME)
        dm2 = datamanager.PJDataManager(conn2)

        dm2.root['c'][u'stephan'] = Person(u'Stephan')

        #Finish in order 2 - 1

        dm2.tpc_finish(None)
        with self.assertRaises(interfaces.ConflictError):
            dm1.tpc_finish(None)

        transaction.abort()

        conn2.close()
        conn1.close()

    def test_conflict_PJContainer_key_2(self):
        """Check conflict detection. We add the same key in
        different transactions, simulating separate processes."""

        self.dm.root['c'] = container.PJContainer('person')
        # (auto-create the table)
        self.dm.root['c'][u'roy'] = Person(u'Roy')
        self.dm.tpc_finish(None)

        conn1 = testing.getConnection(testing.DBNAME)
        dm1 = datamanager.PJDataManager(conn1)

        dm1.root['c'][u'stephan'] = Person(u'Stephan')

        conn2 = testing.getConnection(testing.DBNAME)
        dm2 = datamanager.PJDataManager(conn2)

        dm2.root['c'][u'stephan'] = Person(u'Stephan')

        #Finish in order 1 - 2

        dm1.tpc_finish(None)
        with self.assertRaises(interfaces.ConflictError):
            dm2.tpc_finish(None)

        transaction.abort()

        conn2.close()
        conn1.close()

    def test_conflict_PJContainer_same_id(self):
        """Check conflict detection. Should never happen that
        PJDataManager.createId creates the very same ID, but you never know..."""

        self.dm.root['c'] = container.PJContainer('person')
        # (auto-create the table)
        self.dm.root['c'][u'roy'] = Person(u'Roy')
        self.dm.tpc_finish(None)

        conn1 = testing.getConnection(testing.DBNAME)
        dm1 = datamanager.PJDataManager(conn1)
        dm1.createId = lambda: 'abcd'
        conn2 = testing.getConnection(testing.DBNAME)
        dm2 = datamanager.PJDataManager(conn2)
        dm2.createId = lambda: 'abcd'

        dm1.root['c'][u'stephan1'] = Person(u'Stephan1')

        # pain: isolation would block inserting stephan2
        #       we have to commit dm1 first
        dm1.tpc_finish(None)

        with self.assertRaises(psycopg2.IntegrityError):
            # XXX: this might need to be translated to ConflictError
            dm2.root['c'][u'stephan2'] = Person(u'Stephan2')
        dm2.tpc_finish(None)

        transaction.abort()

        conn2.close()
        conn1.close()


class PJContainedInterfaceTest(unittest.TestCase):

    def test_verifyClass(self):
        from zope.location.interfaces import IContained
        self.assertTrue(verifyClass(IContained, container.PJContained))

    def test_verifyObject(self):
        from zope.location.interfaces import IContained
        self.assertTrue(verifyObject(IContained, container.PJContained()))


class SimplePJContainerInterfaceTest(unittest.TestCase):

    def test_verifyClass(self):
        self.assertTrue(verifyClass(IContainer, container.SimplePJContainer))

    def test_verifyObject(self):
        self.assertTrue(verifyObject(IContainer, container.SimplePJContainer()))


class PJContainerInterfaceTest(unittest.TestCase):

    def test_verifyClass(self):
        self.assertTrue(
            verifyClass(IContainer, container.PJContainer))
        self.assertTrue(
            verifyClass(zinterfaces.IPJContainer, container.PJContainer))

    def test_verifyObject(self):
        self.assertTrue(
            verifyObject(IContainer, container.PJContainer()))
        self.assertTrue(
            verifyObject(zinterfaces.IPJContainer, container.PJContainer()))


class IdNamesPJContainerInterfaceTest(unittest.TestCase):

    def test_verifyClass(self):
        self.assertTrue(
            verifyClass(IContainer, container.IdNamesPJContainer))
        self.assertTrue(
            verifyClass(zinterfaces.IPJContainer, container.IdNamesPJContainer))

    def test_verifyObject(self):
        self.assertTrue(
            verifyObject(IContainer, container.IdNamesPJContainer()))
        self.assertTrue(
            verifyObject(zinterfaces.IPJContainer, container.IdNamesPJContainer()))


class AllItemsPJContainerInterfaceTest(unittest.TestCase):

    def test_verifyClass(self):
        self.assertTrue(
            verifyClass(IContainer, container.AllItemsPJContainer))
        self.assertTrue(
            verifyClass(zinterfaces.IPJContainer, container.AllItemsPJContainer))

    def test_verifyObject(self):
        self.assertTrue(
            verifyObject(IContainer, container.AllItemsPJContainer()))
        self.assertTrue(
            verifyObject(zinterfaces.IPJContainer, container.AllItemsPJContainer()))


class SubDocumentPJContainerInterfaceTest(unittest.TestCase):

    def test_verifyClass(self):
        self.assertTrue(
            verifyClass(IContainer, container.SubDocumentPJContainer))
        self.assertTrue(
            verifyClass(zinterfaces.IPJContainer, container.SubDocumentPJContainer))

    def test_verifyObject(self):
        self.assertTrue(
            verifyObject(IContainer, container.SubDocumentPJContainer()))
        self.assertTrue(
            verifyObject(zinterfaces.IPJContainer, container.SubDocumentPJContainer()))


checker = renormalizing.RENormalizing([
    (re.compile(r'datetime.datetime(.*)'),
     'datetime.datetime(2011, 10, 1, 9, 45)'),
    (re.compile(r"'[0-9a-f]{24}'"),
     "'0001020304050607080a0b0c0'"),
    (re.compile(r"object at 0x[0-9a-f]*>"),
     "object at 0x001122>"),
    (re.compile(r"zodb-[0-9a-f].*"),
     "zodb-01af3b00c5"),
    ])

@zope.component.adapter(
    zope.interface.Interface,
    zope.lifecycleevent.interfaces.IObjectModifiedEvent
    )
def handleObjectModifiedEvent(object, event):
    print event.__class__.__name__+':', repr(object)


def setUp(test):
    placelesssetup.setUp(test)
    testing.setUp(test)

    # since the table gets created in PJContainer.__init__ we need to provide
    # a IPJDataManagerProvider
    class Provider(object):
        zope.interface.implements(interfaces.IPJDataManagerProvider)

        def get(self, database):
            return test.globs['dm']

    zope.component.provideUtility(Provider())

    # silence this, otherwise half-baked objects raise exceptions
    # on trying to __repr__ missing attributes
    test.orig_DEBUG_EXCEPTION_FORMATTER = \
        exceptionformatter.DEBUG_EXCEPTION_FORMATTER
    exceptionformatter.DEBUG_EXCEPTION_FORMATTER = 0

def noCacheSetUp(test):
    container.USE_CONTAINER_CACHE = False
    setUp(test)

def tearDown(test):
    testing.tearDown(test)
    placelesssetup.tearDown(test)
    exceptionformatter.DEBUG_EXCEPTION_FORMATTER = \
        test.orig_DEBUG_EXCEPTION_FORMATTER

def noCacheTearDown(test):
    container.USE_CONTAINER_CACHE = True
    tearDown(test)

def test_suite():
    return unittest.TestSuite()
    dt = doctest.DocTestSuite(
                setUp=setUp, tearDown=tearDown, checker=checker,
                optionflags=(doctest.NORMALIZE_WHITESPACE|
                             doctest.ELLIPSIS|
                             doctest.REPORT_ONLY_FIRST_FAILURE
                             #|doctest.REPORT_NDIFF
                             ),
                )
    dt.layer = testing.db_layer

    return unittest.TestSuite((
        dt,
        #doctest.DocTestSuite(
        #        setUp=noCacheSetUp, tearDown=noCacheTearDown, checker=checker,
        #        optionflags=(doctest.NORMALIZE_WHITESPACE|
        #                     doctest.ELLIPSIS|
        #                     doctest.REPORT_ONLY_FIRST_FAILURE
        #                     #|doctest.REPORT_NDIFF
        #                     )
        #        ),
        unittest.makeSuite(ContainerConflictTest),
        unittest.makeSuite(PJContainedInterfaceTest),
        unittest.makeSuite(SimplePJContainerInterfaceTest),
        unittest.makeSuite(PJContainerInterfaceTest),
        unittest.makeSuite(IdNamesPJContainerInterfaceTest),
        unittest.makeSuite(AllItemsPJContainerInterfaceTest),
        unittest.makeSuite(SubDocumentPJContainerInterfaceTest),
        ))
