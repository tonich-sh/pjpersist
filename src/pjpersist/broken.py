##############################################################################
#
# Copyright (c) 2004 Zope Foundation and Contributors.
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
"""Broken object support
"""

from __future__ import absolute_import

import zope.interface

from . import interfaces


class BrokenModified(TypeError):
    """Attempt to modify a broken object
    """


@zope.interface.implementer(interfaces.IBroken)
class Broken(object):
    """Broken object base class

       Broken objects are placeholders for objects that can no longer be
       created because their class has gone away.

       Broken objects don't really do much of anything, except hold their
       state.   The Broken class is used as a base class for creating
       classes in lieu of missing classes::

         >>> Atall = type('Atall', (Broken, ), {'__module__': 'not.there'})

       The only thing the class can be used for is to create new objects::

         >>> Atall()
         <broken not.there.Atall instance>
         >>> Atall().__Broken_newargs__
         ()
         >>> Atall().__Broken_initargs__
         ()

         >>> Atall(1, 2).__Broken_newargs__
         (1, 2)
         >>> Atall(1, 2).__Broken_initargs__
         (1, 2)

         >>> a = Atall.__new__(Atall, 1, 2)
         >>> a
         <broken not.there.Atall instance>
         >>> a.__Broken_newargs__
         (1, 2)
         >>> a.__Broken_initargs__

       You can't modify broken objects::

         >>> a.x = 1
         Traceback (most recent call last):
         ...
         BrokenModified: Can't change broken objects

       But you can set their state::

         >>> a.__setstate__({'x': 1, })

       You can pickle broken objects::

         >>> r = a.__reduce__()
         >>> len(r)
         3
         >>> r[0] is rebuild
         True
         >>> r[1]
         ('not.there', 'Atall', 1, 2)
         >>> r[2]
         {'x': 1}

         >>> from ZODB._compat import dumps
         >>> from ZODB._compat import loads
         >>> from ZODB._compat import _protocol
         >>> a2 = loads(dumps(a, _protocol))
         >>> a2
         <broken not.there.Atall instance>
         >>> a2.__Broken_newargs__
         (1, 2)
         >>> a2.__Broken_initargs__
         >>> a2.__Broken_state__
         {'x': 1}

       Cleanup::

         >>> broken_cache.clear()
       """


    __Broken_state__ = __Broken_initargs__ = None

    __name__ = 'broken object'

    def __new__(class_, *args):
        result = object.__new__(class_)
        result.__dict__['__Broken_newargs__'] = args
        return result

    def __init__(self, *args):
        self.__dict__['__Broken_initargs__'] = args

    def __reduce__(self):
        """We pickle broken objects in hope of being able to fix them later
        """
        # return (rebuild,
        #         ((self.__class__.__module__, self.__class__.__name__)
        #          + self.__Broken_newargs__),
        #         self.__Broken_state__,
        #         )
        raise NotImplementedError

    def __setstate__(self, state):
        self.__dict__['__Broken_state__'] = state

    def __repr__(self):
        return "<broken %s.%s instance>" % (
            self.__class__.__module__, self.__class__.__name__)

    def __setattr__(self, name, value):
        raise BrokenModified("Can't change broken objects")
