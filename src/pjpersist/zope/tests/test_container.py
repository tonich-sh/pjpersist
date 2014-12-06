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

import ZODB
import ZODB.DemoStorage
import persistent
import random
import re
import transaction
import zope.component
import zope.interface
import zope.lifecycleevent
from pprint import pprint
from zope.exceptions import exceptionformatter
from zope.app.testing import placelesssetup
from zope.container import contained, btree
from zope.testing import cleanup, module, renormalizing

from pjpersist import datamanager, interfaces, serialize, testing
from pjpersist.zope import container

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

      >>> dm.reset()
      >>> dm.root['c'] = container.SimplePJContainer()

      >>> dumpTable(cn)
      [{'data': {u'data': {}}, 'id': '87772716-c1e4-466d-8dfb-0ef8f7db3145'}]

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
      [{'data':
          {u'__name__': u'stephan',
           u'__parent__':
               {u'_py_type': u'DBREF',
                u'database': u'pjpersist_test',
                u'id': u'00000000-0000-0000-0000-000000000000',
                u'table': u'pjpersist_dot_zope_dot_container_dot_SimplePJContainer'},
           u'name': u'Stephan'},
        'id': '00000000-0000-0000-0000-000000000000'}]

      >>> dm.root['c'].keys()
      [u'stephan']
      >>> dm.root['c']['stephan'].__parent__
      <pjpersist.zope.container.SimplePJContainer object at 0x7fec50f86500>
      >>> dm.root['c']['stephan'].__name__
      u'stephan'

      >>> dumpTable(cn)
      [{'data': {u'data':
          {u'stephan': {u'_py_type': u'DBREF',
                        u'database': u'pjpersist_test',
                        u'id': u'967d3248-2ac8-4254-bbea-e0c22b6f6039',
                        u'table': u'person'}}},
        'id': '6cf63e42-9e8f-4e63-8de7-3861cb6ce7d9'}]

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
      [{'data': {u'_pj_table': u'person'},
        'id': '00000000-0000-0000-0000-000000000000'}]

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
      [{'data': {u'key': u'stephan',
                 u'name': u'Stephan',
                 u'parent': {u'_py_type': u'DBREF',
                             u'database': u'pjpersist_test',
                             u'id': u'5a9a2d2c-807a-44fb-84a2-e4320df95c4e',
                             u'table': u'pjpersist_dot_zope_dot_container_dot_PJContainer'}},
        'id': '591d1116-56e7-4b5b-baec-d9e311de57e7'}]

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

def doctest_PJContainer_find():
    """PJContainer: find

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

      >>> res = dm.root['people'].raw_find({'name': {'$regex': '^Ro.*'}})
      >>> pprint(list(res))
      [{u'_id': ObjectId('4e7eb152e138234158000004'),
        u'key': u'roy',
        u'name': u'Roy',
        u'parent': DBRef(u'pjpersist.zope.container.PJContainer',
                         ObjectId('4e7eb152e138234158000000'),
                         u'pjpersist_container_test')},
       {u'_id': ObjectId('4e7eb152e138234158000005'),
        u'key': u'roger',
        u'name': u'Roger',
        u'parent': DBRef(u'pjpersist.zope.container.PJContainer',
                         ObjectId('4e7eb152e138234158000000'),
                         u'pjpersist_container_test')}]

    And now the same query, but this time with object results:

      >>> res = dm.root['people'].find({'name': {'$regex': '^Ro.*'}})
      >>> pprint(list(res))
      [<Person Roy>, <Person Roger>]

    When no spec is specified, all items are returned:

      >>> res = dm.root['people'].find()
      >>> pprint(list(res))
      [<Person Stephan>, <Person Roy>, <Person Roger>, <Person Adam>,
       <Person Albertas>, <Person Russ>]

    You can also search for a single result:

      >>> res = dm.root['people'].raw_find_one({'name': {'$regex': '^St.*'}})
      >>> pprint(res)
      {u'_id': ObjectId('4e7eb259e138234289000003'),
       u'key': u'stephan',
       u'name': u'Stephan',
       u'parent': DBRef(u'pjpersist.zope.container.PJContainer',
                        ObjectId('4e7eb259e138234289000000'),
                        u'pjpersist_container_test')}

      >>> stephan = dm.root['people'].find_one({'name': {'$regex': '^St.*'}})
      >>> pprint(stephan)
      <Person Stephan>

    If no result is found, ``None`` is returned:

      >>> dm.root['people'].find_one({'name': {'$regex': '^XXX.*'}})

    If there is no spec, then simply the first item is returned:

      >>> dm.root['people'].find_one()
      <Person Stephan>

    On the other hand, if the spec is an id, we look for it instead:

      >>> dm.root['people'].find_one(stephan._p_oid.id)
      <Person Stephan>
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
      [u'4e7ddf12e138237403000003']
      >>> name = keys[0]
      >>> dm.root['c'][name]
      <Person Stephan>

      >>> dm.root['c'].values()
      [<Person Stephan>]

      >>> dm.root['c'][name].__parent__
      <pjpersist.zope.container.IdNamesPJContainer object at 0x7fec50f86500>
      >>> dm.root['c'][name].__name__
      u'4e7ddf12e138237403000003'

    It is a feature of the container that the item is immediately available
    after assignment, but before the data is stored in the database. Let's
    commit and access the data again:

      >>> transaction.commit()

      >>> db = dm._conn[DBNAME]
      >>> pprint(list(db['person'].find()))
      [{u'_id': ObjectId('4e7e9d3ae138232d7b000003'),
        u'name': u'Stephan',
        u'parent': DBRef(u'pjpersist.zope.container.IdNamesPJContainer',
                         ObjectId('4e7e9d3ae138232d7b000000'),
                         u'pjpersist_container_test')}]

    Notice how there is no "key" entry in the document. We get a usual key
    error, if an object does not exist:

      >>> dm.root['c']['4e7e9d3ae138232d7b000fff']
      Traceback (most recent call last):
      ...
      KeyError: '4e7e9d3ae138232d7b000fff'

      >>> '4e7e9d3ae138232d7b000fff' in dm.root['c']
      False

      >>> dm.root['c']['roy']
      Traceback (most recent call last):
      ...
      KeyError: 'roy'

      >>> 'roy' in dm.root['c']
      False

    Now remove the item:

      >>> del dm.root['c'][name]

    The changes are immediately visible.

      >>> dm.root['c'].keys()
      []
      >>> dm.root['c'][name]
      Traceback (most recent call last):
      ...
      KeyError: u'4e7e9d3ae138232d7b000003'

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

      >>> transaction.commit()

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

      >>> dm.reset()
      >>> dm.root['app_root'] = ApplicationRoot()

    Let's add a container to the app root:

      >>> dm.root['app_root']['people'] = \
      ...     container.SubDocumentPJContainer('person')
      ContainerModifiedEvent: <ApplicationRoot>

      >>> transaction.commit()
      >>> db = dm._conn[DBNAME]
      >>> pprint(list(db['root'].find()))
      [{u'_id': ObjectId('4e7ea67be138233711000001'),
        u'data':
         {u'people':
          {u'_pj_table': u'person',
           u'_py_persistent_type':
               u'pjpersist.zope.container.SubDocumentPJContainer'}}}]

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

    Trying accessing people fails:

      >>> root['app']['people'].keys()
      Traceback (most recent call last):
      ...
      ComponentLookupError:
       (<InterfaceClass pjpersist.interfaces.IPJDataManagerProvider>, '')

    This is because we have not told the system how to get a datamanager:

      >>> class Provider(object):
      ...     zope.interface.implements(interfaces.IPJDataManagerProvider)
      ...     def get(self):
      ...         return dm
      >>> zope.component.provideUtility(Provider())

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

      >>> pprint(list(dm._get_table(DBNAME, 'person').find()))
      [{u'_id': ObjectId('4e7ed795e1382366a0000001'),
        u'key': u'stephan',
        u'name': u'Stephan',
        u'parent': u'zodb-1058e89d27d8afd9'}]

    Note that we produced a nice hex-presentation of the ZODB's OID.
    """


# classes for doctest_Realworldish
class Campaigns(container.PJContainer):
    _pj_table = 'campaigns'

    def __init__(self, name):
        self.name = name

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
    """Let's see some real worldish hierarchic structure persisted

    Let's make sure events are fired correctly:

      >>> zope.component.provideHandler(handleObjectModifiedEvent)

    Let's add a container to the root:

      >>> transaction.commit()
      >>> dm.root['c'] = Campaigns('foobar')

      >>> db = dm._conn[DBNAME]
      >>> pprint(list(db['pjpersist.zope.tests.test_container.Campaigns'].find()))
      [{u'_id': ObjectId('4e7ddf12e138237403000000'),
        u'name': u'foobar'}]

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

      >>> pprint(list(db['campaigns'].find()))
      [{u'_id': ObjectId('4e7ddf12e138237403000000'),
        u'key': u'one',
        u'name': u'one',
        u'parent': DBRef(u'pjpersist.zope.tests.test_container.Campaigns',
            ObjectId('4e7ddf12e138237403000000'),
            u'pjpersist_container_test')}]

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
    _p_pj_store_type = True

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
      >>> x = transaction.begin()
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
      >>> x = transaction.begin()
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
      >>> x = transaction.begin()
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

checker = renormalizing.RENormalizing([
    (re.compile(r'datetime.datetime(.*)'),
     'datetime.datetime(2011, 10, 1, 9, 45)'),
    (re.compile(r"'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}'"),
     "'00000000-0000-0000-0000-000000000000'"),
    (re.compile(r"ObjectId\('[0-9a-f]{24}'\)"),
     "ObjectId('4e7ddf12e138237403000000')"),
    (re.compile(r"u'[0-9a-f]{24}'"),
     "u'4e7ddf12e138237403000000'"),
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
        def get(self):
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
    try:
        del Person._p_pj_store_type
    except AttributeError:
        pass
    try:
        del SimplePerson._p_pj_store_type
    except AttributeError:
        pass
    exceptionformatter.DEBUG_EXCEPTION_FORMATTER = \
        test.orig_DEBUG_EXCEPTION_FORMATTER

def noCacheTearDown(test):
    container.USE_CONTAINER_CACHE = True
    tearDown(test)

def test_suite():
    return unittest.TestSuite((
        doctest.DocTestSuite(
                setUp=setUp, tearDown=tearDown, checker=checker,
                optionflags=(doctest.NORMALIZE_WHITESPACE|
                             doctest.ELLIPSIS|
                             doctest.REPORT_ONLY_FIRST_FAILURE
                             #|doctest.REPORT_NDIFF
                             )
                ),
        #doctest.DocTestSuite(
        #        setUp=noCacheSetUp, tearDown=noCacheTearDown, checker=checker,
        #        optionflags=(doctest.NORMALIZE_WHITESPACE|
        #                     doctest.ELLIPSIS|
        #                     doctest.REPORT_ONLY_FIRST_FAILURE
        #                     #|doctest.REPORT_NDIFF
        #                     )
        #        ),
        ))
