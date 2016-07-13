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
"""PostGreSQL/JSONB Mapping Tests"""
import doctest
import persistent
import transaction

from pjpersist import testing, mapping


class Item(persistent.Persistent):
    def __init__(self, name=None, site=None):
        self.name = name
        self.site = site


def doctest_PJTableMapping_simple():
    r"""PJTableMapping: simple

    The PJ Table Mapping provides a Python dict interface for a PostGreSQL
    table. Here is a simple example for our Item class/table:

        >>> class SimpleContainer(mapping.PJTableMapping):
        ...     table = 'pjpersist_dot_tests_dot_test_mapping_dot_Item'
        ...     mapping_key = 'name'

    To initialize the mapping, we need a data manager:

        >>> container = SimpleContainer(dm)

    Let's do some obvious initial manipulations:

        >>> container['one'] = one = Item()
        >>> one.name
        'one'
        >>> transaction.commit()

    After the transaction is committed, we can access the item:

        >>> container.keys()
        [u'one']
        >>> one = container['one']
        >>> one.name
        u'one'

        >>> container['two']
        Traceback (most recent call last):
        ...
        KeyError: 'two'

    Of course we can delete an item, but note that it only removes the name,
    but does not delete the document by default:
        >>> del container['one']
        >>> transaction.commit()
        >>> container.keys()
        []
    """


def doctest_PJTableMapping_filter():
    r"""PJTableMapping: filter

    It is often desirable to manage multiple mappings for the same type of
    object and thus same table. The PJ mapping thus supports filtering
    for all its functions.

        >>> from pjpersist import smartsql
        >>> class SiteContainer(mapping.PJTableMapping):
        ...     table = 'pjpersist_item'
        ...     mapping_key = 'name'
        ...     def __init__(self, jar, site):
        ...         super(SiteContainer, self).__init__(jar)
        ...         self.site = site
        ...     def __pj_filter__(self):
        ...         st = self.get_table_object('st')
        ...         return smartsql.JsonbSuperset(st.data, '{"site": "%s"}' % self.site)

        >>> container1 = SiteContainer(dm, 'site1')
        >>> container2 = SiteContainer(dm, 'site2')

    Let's now add some items:

        >>> container1['1-1'] = Item(site='site1')
        >>> container1['1-2'] = Item(site='site1')
        >>> container1['1-3'] = Item(site='site1')
        >>> container2['2-1'] = Item(site='site2')

    And accessing the items works as expected:

        >>> transaction.commit()
        >>> container1.keys()
        [u'1-1', u'1-2', u'1-3']
        >>> container1['1-1'].name
        u'1-1'
        >>> container1['2-1']
        Traceback (most recent call last):
        ...
        KeyError: '2-1'

        >>> container2.keys()
        [u'2-1']

    Note: The mutator methods (``__setitem__`` and ``__delitem__``) do nto
    take the filter into account by default. They need to be extended to
    properly setup and tear down the filter criteria.
    """


def doctest_PJMapping_simple():
    r"""PJMapping: simple

    The PJ Mapping extends a PersistentMapping to provide a Python dict interface
    for a PostGreSQL table and preserve ZODB compatible.
    Here is a simple example for our Item class/table:

        >>> class SimpleContainer(mapping.PJMapping):
        ...     table = 'pjpersist_item'
        ...     mapping_key = 'name'

    Initialize the mapping: just add it to the data manager's root:

        >>> dm.root.container = container = SimpleContainer()

    Let's do some obvious initial manipulations:

        >>> container['one'] = one = Item()
        >>> one.name
        'one'
        >>> 'one' in container
        True
        >>> transaction.commit()

    After the transaction is committed, we can access the item:

        >>> container = dm.root.container
        >>> list(container.keys())
        [u'one']

        >>> 'one' in container
        True

        >>> one = container['one']
        >>> one.name
        u'one'

        >>> container['two']
        Traceback (most recent call last):
        ...
        KeyError: 'two'

    Of course we can delete an item, but note that it only removes the name,
    but does not delete the document by default:
        >>> del container['one']
        Traceback (most recent call last):
        ...
        NotImplementedError
        >>> transaction.commit()
        >>> list(container.keys())
        [u'one']
    """


def test_suite():
    suite = doctest.DocTestSuite(
        setUp=testing.setUp, tearDown=testing.tearDown,
        checker=testing.checker,
        optionflags=testing.OPTIONFLAGS)
    suite.layer = testing.db_layer
    return suite
