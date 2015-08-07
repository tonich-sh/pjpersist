pjpersist
=========

A Python PostGreSQL/JSONB Persistence Backend.

Providing transparent persistence of Python objects.

This document outlines the general capabilities of the ``pjpersist``
package. ``pjpersist`` is a PostGreSQL/JSONB storage implementation for
persistent Python objects. It is *NOT* a storage for the ZODB.

The goal of ``pjpersist`` is to provide a data manager that serializes objects
to PostGreSQL using JSONB at transaction boundaries. The PJ data manager is
a persistent data manager, which handles events at transaction boundaries (see
``transaction.interfaces.IDataManager``) as well as events from the
persistency framework (see ``persistent.interfaces.IPersistentDataManager``).

An instance of a data manager is supposed to have the same life time as the
transaction, meaning that it is assumed that you create a new data manager
when creating a new transaction:

  >>> import transaction

Let's now define a simple persistent object:

  >>> import persistent

  >>> class Person(persistent.Persistent, ReprMixin):
  ...
  ...     def __init__(self, name, phone=None, address=None, friends=None,
  ...                  visited=(), birthday=None):
  ...         self.name = name
  ...         self.address = address
  ...         self.friends = friends or {}
  ...         self.visited = visited
  ...         self.phone = phone
  ...         self.birthday = birthday
  ...         self.today = datetime.datetime(2014, 12, 4, 12, 30, 0)
  ...
  ...     def __str__(self):
  ...         return self.name

Let's create a new person and store it in PostGreSQL:

  >>> stephan = Person(u'Stephan')
  >>> dm.root['stephan'] = stephan

By default, persistent objects are stored in a tabke having the Python path of
the class. Since table names cannot statewith an underscore and contain dots,
we have to escpae the path a little bit. Let's see what got stored in
PostGreSQL:

  >>> dumpTable('u__main___dot_Person')  # doctest: +ELLIPSIS
  [{'data': {u'_py_persistent_type': u'__main__.Person',
             u'address': None,
             u'birthday': None,
             u'friends': {},
             u'name': u'Stephan',
             u'phone': None,
             u'today': {u'_py_type': u'datetime.datetime',
                        u'value': u'2014-12-04T12:30:00'},
             u'visited': []},
    'id': ...L}]

Let's now add an address for Stephan. Addresses are also persistent objects:

  >>> class Address(persistent.Persistent, ReprMixin):
  ...     _p_pj_table = 'address'
  ...
  ...     def __init__(self, city, zip):
  ...         self.city = city
  ...         self.zip = zip
  ...
  ...     def __str__(self):
  ...         return '%s (%s)' %(self.city, self.zip)

  >>> stephan.address = Address('Maynard', '01754')

We need to commit the transaction, to push the data to PostGreSQL:

  >>> transaction.commit()

  >>> dumpTable('address')  # doctest: +ELLIPSIS
  [{'data': {u'_py_persistent_type': u'__main__.Address',
             u'city': u'Maynard',
             u'zip': u'01754'},
    'id': ...L}]

As you can see, even the reference to the Address object looks nice and uses
the standard PostGreSQL reference construct.

  >>> dumpTable('u__main___dot_Person')  # doctest: +ELLIPSIS
  [{'data': {u'_py_persistent_type': u'__main__.Person',
             u'address': {u'_py_type': u'DBREF',
                          u'database': u'pjpersist_test',
                          u'id': ...,
                          u'table': u'address'},
             u'birthday': None,
             u'friends': {},
             u'name': u'Stephan',
             u'phone': None,
             u'today': {u'_py_type': u'datetime.datetime',
                        u'value': u'2014-12-04T12:30:00'},
             u'visited': []},
    'id': ...L}]

But what about arbitrary non-persistent, but picklable, objects?
Well, let's create a phone number object for that:

  >>> class Phone(ReprMixin):
  ...
  ...     def __init__(self, country, area, number):
  ...         self.country = country
  ...         self.area = area
  ...         self.number = number
  ...
  ...     def __str__(self):
  ...         return '%s-%s-%s' %(self.country, self.area, self.number)

  >>> stephan = dm.root['stephan']
  >>> stephan.phone = Phone('+1', '978', '394-5124')
  >>> transaction.commit()

  >>> dumpTable('u__main___dot_Person')  # doctest: +ELLIPSIS
  [{'data': {u'_py_persistent_type': u'__main__.Person',
             u'address': {u'_py_type': u'DBREF',
                          u'database': u'pjpersist_test',
                          u'id': ...,
                          u'table': u'address'},
             u'birthday': None,
             u'friends': {},
             u'name': u'Stephan',
             u'phone': {u'_py_type': u'__main__.Phone',
                        u'area': u'978',
                        u'country': u'+1',
                        u'number': u'394-5124'},
             u'today': {u'_py_type': u'datetime.datetime',
                        u'value': u'2014-12-04T12:30:00'},
             u'visited': []},
    'id': ...L}]

Let's now set various attributes:

  >>> stephan = dm.root['stephan']
  >>> stephan.friends = {'roy': Person(u'Roy Mathew')}
  >>> stephan.visited = (u'Germany', u'USA')
  >>> stephan.birthday = datetime.date(1980, 1, 25)

Push the data to PostGreSQL, and dump the results:

  >>> transaction.commit()
  >>> dumpTable('u__main___dot_Person')  # doctest: +ELLIPSIS
  [{'data': {u'_py_persistent_type': u'__main__.Person',
             u'address': {u'_py_type': u'DBREF',
                          u'database': u'pjpersist_test',
                          u'id': ...,
                          u'table': u'address'},
             u'birthday': {u'_py_type': u'datetime.date',
                           u'value': u'1980-01-25'},
             u'friends': {u'roy': {u'_py_type': u'DBREF',
                                   u'database': u'pjpersist_test',
                                   u'id': ...,
                                   u'table': u'u__main___dot_Person'}},
             u'name': u'Stephan',
             u'phone': {u'_py_type': u'__main__.Phone',
                        u'area': u'978',
                        u'country': u'+1',
                        u'number': u'394-5124'},
             u'today': {u'_py_type': u'datetime.datetime',
                        u'value': u'2014-12-04T12:30:00'},
             u'visited': [u'Germany', u'USA']},
    'id': ...L},
   {'data': {u'_py_persistent_type': u'__main__.Person',
             u'address': None,
             u'birthday': None,
             u'friends': {},
             u'name': u'Roy Mathew',
             u'phone': None,
             u'today': {u'_py_type': u'datetime.datetime',
                        u'value': u'2014-12-04T12:30:00'},
             u'visited': []},
    'id': ...L}]

Of course all properties can be retrieved as python objects:

  >>> stephan = dm.root['stephan']
  >>> stephan.address
  <Address Maynard (01754)>

  >>> stephan.address.city
  u'Maynard'

  >>> stephan.birthday
  datetime.date(1980, 1, 25)

  >>> stephan.friends
  {u'roy': <Person Roy Mathew>}

  >>> stephan.phone
  <Phone +1-978-394-5124>

  >>> stephan.today
  datetime.datetime(2011, 10, 1, 9, 45)

  >>> stephan.visited
  [u'Germany', u'USA']


See src/pjpersist/README.txt and the other txt files in the package
for more details.

Travis: |buildstatus|_

.. |buildstatus| image:: https://api.travis-ci.org/Shoobx/pjpersist.png?branch=master
.. _buildstatus: https://travis-ci.org/Shoobx/pjpersist
