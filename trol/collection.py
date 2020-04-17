# -*- coding: utf-8 -*-
# doctest: +ELLIPSIS
"""Collections which proxy access to Redis storage

This file is Copyright (c) 2010 Tim Medina
Licenced under the MIT License availible in the root of the trol source repo
It has been modifed to hit the overal model of trol, but is largely the same externally
Thanks to the guys at Redisco for the great work! https://github.com/kiddouk/redisco

Here are some of the modifications from the origonal Redisco conatainers:
    * Values are serialized and deserialized allowing more arbitrary objects to be stored
    * Comparison operations do not trigger network transfer of container members
    * Set copy does not tigger network transfer of set members
    * Functions which require multiple explicitly use the pipeline to avoid multiple network calls
    * Some functions now use a "scratch-pad" in redis to avoid transfering full collections
    * The ``db`` attribute has been renamed to ``redis`` to match other places in trol
    * There is no default expire time
    * A Redis connection must be specified on construction, unless accessed through a Model
    * There is no support for TypedList or NonPersitentList

Unlike properties, collections call out to Redis on every transaction by default
Caching for collections is a trickier prospect, and therefore is not attempted

>>> import trol
>>> from redis import Redis
>>> redis = Redis()

"""

import threading
import collections
import pickle
from . import Serializer, Deserializer

# Use a guid to make sure that no one will define a colliding key by accident
# Every thread gets their own scratch key to make sure there are not threading errors
_scratch_guid = "c05f145e-a1a8-4742-918c-e01d6d40b02a"

def _scratch_key():
    return "scratch:{}:{}".format(_scratch_guid, threading.get_ident())


def _parse_values(values):
    (_values,) = values if len(values) == 1 else (None,)
    if _values and type(_values) == type([]):
        return _values
    return values


class Collection(object):
    """
    Base class for all collections. This class should not used directctly
    This class provides the ``redis`` attribute
    :members:
    """

    _trol_database = None

    def __init__(self, name=None, redis=None, typ=None, key=None, serializer=None, deserializer=None):
        self._name = name
        self._redis = redis
        self._key = key
        self._threadlocal = None

        self._typ = typ
        if serializer is None:
            if typ is None:
                self.serialize = pickle.dumps
            else:
                self.serialize = Serializer(typ)
        else:
            self.serialize = serializer

        if deserializer is None:
            if typ is None:
                self.deserialize = pickle.loads
            else:
                self.deserialize = Deserializer(typ)
        else:
            self.deserialize = deserializer

    def __get__(self, obj, typ=None):
        if obj is None:
            return self

        if self._key is not None:
            key = self._key
        else:
            if self._name is not None:
                key = ':'.join((obj.key, self._name))
            else:
                raise AttributeError(
                    "{self.__class__.__name__} does not have it's 'name' or 'key' attributes set. If bound to a class which is not a Model, at least one must be set explicitly")

        if self._redis is not None:
            redis = self._redis
        else:
            redis = obj.redis

        return self.__class__(name=self._name, key=key, redis=redis, typ=self._typ, serializer=self.serialize, deserializer=self.deserialize)

    def clear(self):
        """
        Remove the collection from the redis storage

        :return: None

        >>> s = trol.Set('test_clear', redis)
        >>> s.add('1')
        1
        >>> s.clear()
        >>> s.members
        set()


        """
        self.redis.delete(self.key)

    def set_expire(self, time):
        """
        Allow the key to expire after ``time`` seconds.

        :param time: time expressed in seconds.
        :return: None

        >>> s = trol.Set('test_set_expire', redis)
        >>> s.add("1")
        1
        >>> s.set_expire(1)
        >>> from time import sleep
        >>> sleep(1.5)
        >>> s.members
        set()

        """
        self.redis.expire(self.key, time)

    @property
    def name(self):
        """``str``: The name for this collection, which will be used to determine the key if bound to a Model

        >>> class Alpha(trol.Model):
        ...     def __init__(self, ident):
        ...         self.id = ident
        ...
        ...     storage = trol.Set(name="store")
        ...
        >>> a = Alpha('xyz')
        >>> a.key
        'Alpha:xyz'
        >>> a.storage.key
        'Alpha:xyz:store'

        """
        return self._name

    @name.setter
    def name(self, value):
        self._name = value

    @property
    def redis(self):
        """``redis.Redis``: A connection object for interacting with Redis"""
        if self._redis is not None:
            return self._redis
        elif self._trol_database is not None:
            return self._trol_database.redis
        else:
            raise AttributeError(
                "'{self.__class__.__name__}' has no connection set. If not bound to a class, a collection must have it's connection specified".format(self=self))

    @redis.setter
    def redis(self, value):
        self._redis = value

    @property
    def key(self):
        """``str``: The key in Redis which contains the data this object references

        Setting this attribute directly to non-None will override ``name`` in determining ``key``

        >>> s = trol.Set(name='foo')
        >>> s.key
        'foo'
        >>> s.key = 'bar'
        >>> s.key
        'bar'

        """
        if self._key is not None:
            return self._key
        if self.name is not None:
            return self.name
        else:
            raise AttributeError(
                "'{self.__class__.__name__}' has no name or key set. If not bound to a class, a collection must have either it's key or name attribute specified".format(self=self))

    @key.setter
    def key(self, value):
        self._key = value

    @property
    def pipeline(self):
        """``redis.Pipeline``: A pipeline object to execute buffered commands

        The collection waits until you call this function for the first time to intialize the pipeline
        A unique pipeline will be created for each thread that accesses this property, so it is thread safe
        """
        if self._threadlocal is None:
            self._threadlocal = threading.local()

        try:
            pipe = self._threadlocal.pipeline
        except AttributeError:
            pipe = self.redis.pipeline()
            self._threadlocal.pipeline = pipe
        return pipe


class Set(Collection):
    """
    .. default-domain:: set

    This class represents a Set in redis.
    """

    def __repr__(self):
        """Gets the string representation of this Set

        >>> s = trol.Set('test', redis)
        >>> repr(s)
        "<Set 'test'>"

        """
        return "<%s '%s'>" % (self.__class__.__name__, self.key)

    def sadd(self, *values):
        """
        Add the specified members to the Set.

        :param values: a list of values or a simple value.
        :return: integer representing the number of value added to the set.

        >>> s = trol.Set('test_sadd', redis)
        >>> s.sadd(1, 2, 3)
        3
        >>> s.sadd(4)
        1
        >>> s.sadd(4)
        0
        >>> s.sadd()
        0
        >>> s.members == {1, 2, 3, 4}
        True

        """
        values = [self.serialize(v) for v in _parse_values(values)]
        if not values:
            return 0

        return self.redis.sadd(self.key, *values)

    def srem(self, *values):
        """
        Remove the values from the Set if they are present.

        :param values: a list of values or a simple value.
        :return: boolean indicating if the values have been removed.

        >>> s = trol.Set('test_srem', redis)
        >>> s.add([1, 2, 3])
        3
        >>> s.srem([1, 3])
        2

        """
        values = [self.serialize(v) for v in _parse_values(values)]
        return self.redis.srem(self.key, *values)

    def spop(self):
        """
        Remove and return (pop) a random element from the Set.

        :return: String representing the value poped.

        >>> s = trol.Set('test_spop', redis)
        >>> s.add("a")
        1
        >>> s.spop()
        'a'
        >>> s.members
        set()

        """
        value = self.redis.spop(self.key)
        if value is None:
            return None
        else:
            return self.deserialize(value)

    def isdisjoint(self, other):
        """
        Return True if the set has no elements in common with other.

        :param other: another ``Set``
        :return: boolean

        >>> s1 = trol.Set('test_isdisjoint1', redis)
        >>> s2 = trol.Set('test_isdisjoint2', redis)
        >>> s1.add(['a', 'b', 'c'])
        3
        >>> s2.add(['c', 'd', 'e'])
        3
        >>> s1.isdisjoint(s2)
        False
        >>> s2.remove('c')
        1
        >>> s1.isdisjoint(s2)
        True

        """
        self.pipeline.sinterstore(_scratch_key(), [self.key, other.key])
        self.pipeline.delete(_scratch_key())
        return self.pipeline.execute()[0] == 0

    def issubset(self, other_set):
        """
        Test whether every element in the set is in other.

        :param other_set: another ``Set`` to compare to.

        >>> s1 = trol.Set('test_issubset1', redis)
        >>> s2 = trol.Set('test_issubset2', redis)
        >>> s1.add(['a', 'b', 'c'])
        3
        >>> s2.add('b')
        1
        >>> s2.issubset(s1)
        True
        >>> s2.add('d')
        1
        >>> s2.issubset(s1)
        False

        """
        return self <= other_set

    def __le__(self, other_set):
        """Test whether the set is a subset of other.

        :return: True if this set is a subset of other.

        >>> s1 = trol.Set('test_le1', redis)
        >>> s2 = trol.Set('test_le2', redis)
        >>> s1.add(['a', 'b'])
        2
        >>> s2.add(['a', 'b'])
        2
        >>> s2 <= s1
        True
        >>> s2.add('c')
        1
        >>> s2 <= s1
        False

        """
        self.pipeline.sdiffstore(_scratch_key(), [self.key, other_set.key])
        self.pipeline.delete(_scratch_key())
        return self.pipeline.execute()[0] == 0

    def __lt__(self, other_set):
        """Test whether the set is a strict subset of other.

        :return: True if this set is a strict subset of other.

        >>> s1 = trol.Set('test_lt1', redis)
        >>> s2 = trol.Set('test_lt2', redis)
        >>> s1.add(['a', 'b'])
        2
        >>> s2.add(['a', 'b'])
        2
        >>> s2 < s1
        False
        >>> s1.add('c')
        1
        >>> s2 < s1
        True

        """
        self.pipeline.sdiffstore(_scratch_key(), [self.key, other_set.key])
        self.pipeline.scard(self.key)
        self.pipeline.scard(other_set.key)
        self.pipeline.delete(_scratch_key())
        result = self.pipeline.execute()
        return result[0] == 0 and result[1] != result[2]

    def __eq__(self, other_set):
        """
        Test equality of keys first, then members is they are not equal

        >>> s1 = trol.Set('test_eq1', redis)
        >>> s2 = trol.Set('test_eq2', redis)
        >>> s1.add(['a', 'b'])
        2
        >>> s1 == s1
        True
        >>> s2.add(['a', 'b'])
        2
        >>> s1 == s2
        True
        >>> s1.add('c')
        1
        >>> s1 == s2
        False

        """
        if other_set.key == self.key:
            return True

        self.pipeline.sunionstore(_scratch_key(), [self.key, other_set.key])
        self.pipeline.scard(self.key)
        self.pipeline.scard(other_set.key)
        self.pipeline.delete(_scratch_key())
        result = self.pipeline.execute()
        return result[0] == result[1] and result[1] == result[2]

    def __ne__(self, other_set):
        return not self.__eq__(other_set)

    def issuperset(self, other_set):
        """
        Test whether every element in other is in the set.

        :param other_set: another ``Set`` to compare to.

        >>> s1 = trol.Set('test_issuperset1', redis)
        >>> s2 = trol.Set('test_issuperset2', redis)
        >>> s1.add(['a', 'b', 'c'])
        3
        >>> s2.add('b')
        1
        >>> s1.issuperset(s2)
        True

        """
        return self >= other_set

    def __ge__(self, other_set):
        """Test whether every element in other is in the set."""
        return other_set <= self

    def __gt__(self, other_set):
        """Test whether the set is a strict superset of other."""
        return other_set < self

    # SET Operations
    def union(self, key, *other_sets):
        """
        Return a new ``Set`` representing the union of the other sets.

        :param key: String representing the key where to store the result.
        :param other_sets: list of other ``Set``.
        :return: a new ``Set`` representing the union of the other sets.

        >>> s1 = trol.Set('test_union1', redis)
        >>> s2 = trol.Set('test_union2', redis)
        >>> s1.add(['a', 'b', 'c'])
        3
        >>> s2.add(['d', 'e'])
        2
        >>> s3 = s1.union('test_union3', s2)
        >>> s3.members == {'a', 'c', 'b', 'e', 'd'}
        True

        """
        self.redis.sunionstore(key, [self.key] + [o.key for o in other_sets])
        return Set(key, redis=self.redis)

    def intersection(self, key, *other_sets):
        """
        Return a new ``Set`` representing the intersection of the other sets.

        :param key: String representing the key where to store the result.
        :param other_sets: list of other ``Set``.
        :return: a new ``Set`` representing the intersection of the other sets.

        >>> s1 = trol.Set('test_intersection1', redis)
        >>> s2 = trol.Set('test_intersection2', redis)
        >>> s1.add(['a', 'b', 'c'])
        3
        >>> s2.add(['c', 'e'])
        2
        >>> s3 = s1.intersection('test_intersection3', s2)
        >>> s3.members
        {'c'}

        """

        self.redis.sinterstore(key, [self.key] + [o.key for o in other_sets])
        return Set(key, redis=self.redis)

    def difference(self, key, *other_sets):
        """
        Return a new ``Set`` representing the difference of *n* sets.

        :param key: String representing the key where to store the result.
        :param other_sets: list of other ``Set``.
        :return: a new ``Set`` representing the difference of this set and the other sets.

        >>> s1 = trol.Set('test_difference1', redis)
        >>> s2 = trol.Set('test_difference2', redis)
        >>> s1.add(['a', 'b', 'c'])
        3
        >>> s2.add(['c', 'e'])
        2
        >>> s3 = s1.difference('test_difference3', s2)
        >>> s3.members == {'a', 'b'}
        True

        """

        self.redis.sdiffstore(key, [self.key] + [o.key for o in other_sets])
        return Set(key, redis=self.redis)

    def update(self, *other_sets):
        """Update the set, adding elements from all other_sets.

        :param other_sets: list of ``Set``
        :return: None

        >>> s1 = trol.Set('test_set_update1', redis)
        >>> s1.add(['a', 'b', 'c'])
        3
        >>> s2 = trol.Set('test_set_update2', redis)
        >>> s2.add(['b', 'c', 'd'])
        3
        >>> s1.update(s2)
        >>> s1.members == {'a', 'b', 'c', 'd'}
        True

        """
        self.redis.sunionstore(self.key, [self.key] + [o.key for o in other_sets])

    def __ior__(self, other_set):
        self.redis.sunionstore(self.key, [self.key, other_set.key])
        return self

    def intersection_update(self, *other_sets):
        """
        Update the set, keeping only elements found in it and all other_sets.

        :param other_sets: list of ``Set``
        :return: None

        >>> s1 = trol.Set('test_intersection_update1', redis)
        >>> s1.add(['a', 'b', 'c'])
        3
        >>> s2 = trol.Set('test_intersection_update2', redis)
        >>> s2.add(['b', 'c', 'd'])
        3
        >>> s1.intersection_update(s2)
        >>> s1.members == {'b', 'c'}
        True

        """
        self.redis.sinterstore(
            self.key, [self.key] + [o.key for o in other_sets])

    def __iand__(self, other_set):
        self.redis.sinterstore(self.key, [self.key, other_set.key])
        return self

    def difference_update(self, *other_sets):
        """
        Update the set, removing elements found in others.

        :param other_sets: list of ``Set``
        :return: None

        >>> s1 = trol.Set('test_difference_update1', redis)
        >>> s1.add(['a', 'b', 'c'])
        3
        >>> s2 = trol.Set('test_difference_update2', redis)
        >>> s2.add(['b', 'c', 'd'])
        3
        >>> s1.difference_update(s2)
        >>> s1.members
        {'a'}

        """
        self.redis.sdiffstore(
            self.key, [self.key] + [o.key for o in other_sets])

    def __isub__(self, other_set):
        self.redis.sdiffstore(self.key, [self.key, other_set.key])
        return self

    def all(self):
        return {self.deserialize(m) for m in self.redis.smembers(self.key)}

    members = property(all)
    """
    Return the stored content of the Set.
    """

    def copy(self, key):
        """
        Copy the set to another key and return the new Set.

        .. WARNING::
            If the new key already contains a value, it will be overwritten.

        >>> s1 = trol.Set('test_set_copy1', redis)
        >>> s1.add(['a', 'b', 'c'])
        3
        >>> s2 = s1.copy('test_set_copy2')
        >>> s2.members == {"a", "b", "c"}
        True

        """

        copy = Set(key, redis=self.redis)
        self.redis.sunionstore(copy.key, self.key, self.key)
        return copy

    def __iter__(self):
        """Get an iterator for this set

        >>> s = trol.Set('test_set_iter', redis)
        >>> s.add('a', 'b', 'b', 'c')
        3
        >>> for count, letter in enumerate(s):
        ...     print('{} Ah, Ah, Ah'.format(count+1))
        1 Ah, Ah, Ah
        2 Ah, Ah, Ah
        3 Ah, Ah, Ah

        """
        return self.members.__iter__()

    def sinter(self, *other_sets):
        """
        Performs an intersection between Sets and return the result without storing.

        .. NOTE::
          This function return a Python ``set`` object, not a ``Set``. See func:``intersection``.

        >>> s1 = trol.Set('test_sinter1', redis)
        >>> s2 = trol.Set('test_sinter2', redis)
        >>> s1.add(['a', 'b', 'c'])
        3
        >>> s2.add(['c', 'e'])
        2
        >>> s1.sinter(s2)
        {'c'}

        """
        return {self.deserialize(elem) for elem in self.redis.sinter([self.key] + [s.key for s in other_sets])}

    def sunion(self, *other_sets):
        """
        Performs a union between Sets and return the result without storing.

        .. NOTE::
          This function return a Python ``set`` object, not a ``Set``. See func:``union``.

        >>> s1 = trol.Set('test_sunion1', redis)
        >>> s2 = trol.Set('test_sunion2', redis)
        >>> s1.add(['a', 'b', 'c'])
        3
        >>> s2.add(['c', 'e'])
        2
        >>> s1.sunion(s2) == {'a', 'b', 'c', 'e'}
        True

        """
        return {self.deserialize(elem) for elem in self.redis.sunion([self.key] + [s.key for s in other_sets])}

    def sdiff(self, *other_sets):
        """
        Performs a difference between Sets and return the result without storing.

        .. NOTE::
          This function return a Python ``set`` object, not a ``Set``. See func:``difference``.

        >>> s1 = trol.Set('test_sdiff1', redis)
        >>> s2 = trol.Set('test_sdiff2', redis)
        >>> s1.add(['a', 'b', 'c'])
        3
        >>> s2.add(['c', 'e'])
        2
        >>> s1.sdiff(s2) == {'a', 'b'}
        True

        """
        return {self.deserialize(elem) for elem in self.redis.sdiff([self.key] + [s.key for s in other_sets])}

    def scard(self):
        """
        Returns the cardinality of the Set.

        :return: integer cardinality of the Set.

        >>> s = trol.Set('test_scard', redis)
        >>> s.add(['a', 'b', 'c'])
        3
        >>> s.scard()
        3

        """
        return self.redis.scard(self.key)

    def sismember(self, value):
        """
        Return ``True`` if the provided value is in the ``Set``.

        >>> s = trol.Set('test_sismember', redis)
        >>> s.add(['a', 'b', 'c'])
        3
        >>> s.sismember('d')
        False
        >>> s.clear()

        """
        return self.redis.sismember(self.key, self.serialize(value))

    def srandmember(self):
        """
        Return a random member of the set.

        >>> s = trol.Set('test_srandmember', redis)
        >>> s.add(['a', 'b', 'c'])
        3
        >>> s.srandmember() in { 'a', 'b', 'c' }
        True

        """
        value = self.redis.srandmember(self.key)
        if value is None:
            return None
        else:
            return self.deserialize(value)

    add = sadd
    pop = spop
    remove = srem
    __contains__ = sismember
    __len__ = scard


class List(Collection):
    """
    This class represent a list object as seen in redis.
    """

    def all(self):
        """
        Returns all items in the list.
        """
        return self.lrange(0, -1)

    members = property(all)
    """Return all items in the list."""

    def llen(self):
        """
        Returns the length of the list.
        """
        return self.redis.llen(self.key)

    __len__ = llen

    def __getitem__(self, index):
        if isinstance(index, int):
            return self.lindex(index)
        elif isinstance(index, slice):
            indices = index.indices(len(self))
            return self.lrange(indices[0], indices[1] - 1)
        else:
            raise TypeError

    def __setitem__(self, index, value):
        self.lset(index, self.serialize(value))

    def lrange(self, start, stop):
        """
        Returns a range of items.

        :param start: integer representing the start index of the range
        :param stop: integer representing the size of the list.

        >>> l = trol.List('test_lrange', redis)
        >>> l.push(['a', 'b', 'c', 'd'])
        4
        >>> l.lrange(1, 2)
        ['b', 'c']

        """
        return [self.deserialize(v) for v in self.redis.lrange(self.key, start, stop)]

    def lpush(self, *values):
        """
        Push the value into the list from the *left* side

        :param values: a list of values or single value to push
        :return: long representing the number of values pushed.

        >>> l = trol.List('test_lpush', redis)
        >>> l.lpush(['a', 'b'])
        2
        >>> l.lpush(['c', 'd'])
        4
        >>> l.members
        ['d', 'c', 'b', 'a']
        >>> l.lpush()
        4

        """
        values = [self.serialize(v) for v in _parse_values(values)]

        if not values:
            return len(self)

        return self.redis.lpush(self.key, *values)

    def rpush(self, *values):
        """
        Push the value into the list from the *right* side

        :param values: a list of values or single value to push
        :return: long representing the size of the list.

        >>> l = trol.List('test_rpush', redis)
        >>> l.rpush(['a', 'b'])
        2
        >>> l.rpush(['c', 'd'])
        4
        >>> l.members
        ['a', 'b', 'c', 'd']
        >>> l.rpush()
        4

        """

        values = [self.serialize(v) for v in _parse_values(values)]

        if not values:
            return len(self)

        return self.redis.rpush(self.key, *values)

    def extend(self, iterable):
        """
        Extend list by appending elements from the iterable.

        :param iterable: an iterable objects.

        >>> l = trol.List('test_extend', redis)
        >>> l.extend(['a', 'b'])
        >>> l.members
        ['a', 'b']
        >>> l.extend(['c', 'd'])
        >>> l.members
        ['a', 'b', 'c', 'd']

        """
        self.rpush(*iterable)

    def count(self, value):
        """
        Return number of occurrences of value.

        :param value: a value tha *may* be contained in the list

        >>> l = trol.List('test_count', redis)
        >>> l.extend(['duck', 'duck', 'duck', 'goose'])
        >>> l.count("duck")
        3
        >>> l.count("goose")
        1
        >>> l.count("possum")
        0

        """
        return self.members.count(value)

    def lpop(self):
        """
        Pop the first object from the left.

        :return: the popped value.

        >>> l = trol.List('test_lpop', redis)
        >>> l.extend(['a', 'b', 'c'])
        >>> l.lpop()
        'a'
        >>> l.lpop()
        'b'
        >>> l.members
        ['c']
        >>> l.lpop()
        'c'
        >>> l.lpop() is None
        True

        """
        value = self.redis.lpop(self.key)
        if value is None:
            return None
        else:
            return self.deserialize(value)

    def rpop(self):
        """
        Pop the first object from the right.

        :return: the popped value.

        >>> l = trol.List('test_rpop', redis)
        >>> l.extend(['a', 'b', 'c'])
        >>> l.rpop()
        'c'
        >>> l.rpop()
        'b'
        >>> l.members
        ['a']
        >>> l.rpop()
        'a'
        >>> l.rpop() is None
        True

        """
        value = self.redis.rpop(self.key)
        if value is None:
            return None
        else:
            return self.deserialize(value)

    def rpoplpush(self, key):
        """
        Remove an element from the list,
        atomically add it to the head of the list indicated by key

        :param key: the key of the list receiving the popped value.
        :return: the popped (and pushed) value

        >>> l = trol.List('list_rpoplpush1', redis)
        >>> l.extend(['a', 'b', 'c'])
        >>> l.rpoplpush('list_rpoplpush2')
        'c'
        >>> l2 = trol.List('list_rpoplpush2', redis)
        >>> l2.members
        ['c']

        """
        value = self.redis.rpoplpush(self.key, key)
        if value is None:
            return None
        else:
            return self.deserialize(value)

    def lrem(self, value, num=1):
        """
        Remove first occurrence of value.

        :return: the number of removed elements

        >>> l = trol.List('test_lrem', redis)
        >>> l.extend(['duck', 'duck', 'duck', 'goose'])
        >>> l.lrem("duck")
        1
        >>> l.lrem("duck", 3)
        2
        >>> l.members
        ['goose']

        """
        return self.redis.lrem(self.key, num, self.serialize(value))

    def reverse(self):
        """
        Reverse the list in place.

        .. NOTE::
          This command must make two network calls, transferring the list each way

        :return: None

        >>> l = trol.List('test_reverse', redis)
        >>> l.extend(['a', 'b', 'c'])
        >>> l.members
        ['a', 'b', 'c']
        >>> l.reverse()
        >>> l.members
        ['c', 'b', 'a']

        """
        def reversefn(pipe):
            values = pipe.lrange(self.key, 0, -1)
            pipe.multi()
            pipe.delete(self.key)
            if values:
                pipe.lpush(self.key, *values)

        self.redis.transaction(reversefn, self.key)

    def copy(self, key):
        """Copy the list to a new list.

        .. WARNING::
            If destination key already contains a value, it clears it before copying.

        :return: a list object pointing to the copy

        >>> l = trol.List('test_list_copy', redis)
        >>> l.extend(['a', 'b', 'c'])
        >>> copy = l.copy('copy')
        >>> copy.members
        ['a', 'b', 'c']

        """
        def copyfn(pipe):
            values = pipe.lrange(self.key, 0, -1)
            pipe.multi()
            pipe.delete(key)
            if values:
                pipe.rpush(key, *values)

        self.redis.transaction(copyfn, self.key, key)
        copy = List(key, self.redis)
        return copy

    def ltrim(self, start, end):
        """
        Trim the list from such that it only includes elements from start to end inclusive.

        :return: True if the operation succeeded

        >>> l = trol.List('test_ltrim', redis)
        >>> l.extend(['a', 'b', 'c'])
        >>> l.ltrim(0, 1)
        True
        >>> l.members
        ['a', 'b']

        """
        return self.redis.ltrim(self.key, start, end)

    def lindex(self, idx):
        """
        Return the value at the index *idx*

        :param idx: the index to fetch the value.
        :return: the value or None if out of range.

        >>> l = trol.List('test_lindex', redis)
        >>> l.extend(['a', 'b', 'c'])
        >>> l.lindex(1)
        'b'

        """
        value = self.redis.lindex(self.key, idx)
        if value is None:
            return None
        else:
            return self.deserialize(value)

    def lset(self, idx, value):
        """
        Set the value in the list at index *idx*

        :return: True is the operation succeed.

        >>> l = trol.List('test_lset', redis)
        >>> l.push(['a', 'b', 'c'])
        3
        >>> l.lset(0, 'e')
        True
        >>> l.members
        ['e', 'b', 'c']

        """
        return self.redis.lset(self.key, idx, self.serialize(value))

    def __iter__(self):
        """Get an iterator for this list

        >>> l = trol.List('test_list_iter', redis)
        >>> l.lpush('a', 'b', 'c')
        3
        >>> for letter in l:
        ...     print(letter)
        c
        b
        a

        """
        return self.members.__iter__()

    def __repr__(self):
        """Get the string representation of this set

        >>> l = trol.List('test', redis)
        >>> repr(l)
        "<List 'test'>"

        """
        return "<%s '%s'>" % (self.__class__.__name__, self.key)

    __len__ = llen
    remove = lrem
    trim = ltrim
    shift = lpop
    unshift = lpush
    pop = rpop
    pop_onto = rpoplpush
    push = rpush
    append = rpush


class SortedSet(Collection):
    """
    This class represents a SortedSet in redis.
    Use it if you want to arrange your set in any order.

    """

    def __getitem__(self, index):
        if isinstance(index, slice):
            return [self.deserialize(v) for v in self.zrange(index.start, index.stop)]
        else:
            return self.deserialize(self.zrange(index, index)[0])

    def score(self, member):
        """
        Returns the score of member.
        """
        return self.zscore(member)

    def __contains__(self, val):
        return self.zscore(val) is not None

    @property
    def members(self):
        """
        Returns the members of the set.
        """
        return self.zrange(0, -1)

    @property
    def revmembers(self):
        """
        Returns the members of the set in reverse.
        """
        return [self.deserialize(v) for v in self.zrevrange(0, -1)]

    def __iter__(self):
        return self.members.__iter__()

    def __reversed__(self):
        return self.revmembers.__iter__()

    @property
    def _min_score(self):
        """
        Returns the minimum score in the SortedSet.
        """
        try:
            return self.zscore(self.__getitem__(0))
        except IndexError:
            return None

    @property
    def _max_score(self):
        """
        Returns the maximum score in the SortedSet.
        """
        try:
            return self.zscore(self.__getitem__(-1))
        except IndexError:
            return None

    def lt(self, v, limit=None, offset=None):
        """
        Returns the list of the members of the set that have scores
        less than v.

        :param v: the score to compare to.
        :param limit: limit the result to *n* elements
        :param offset: Skip the first *n* elements
        """
        if limit is not None and offset is None:
            offset = 0
        return self.zrangebyscore("-inf", "(%f" % v, start=offset, num=limit)

    def le(self, v, limit=None, offset=None):
        """
        Returns the list of the members of the set that have scores
        less than or equal to v.

        :param v: the score to compare to.
        :param limit: limit the result to *n* elements
        :param offset: Skip the first *n* elements

        """
        if limit is not None and offset is None:
            offset = 0
        return self.zrangebyscore("-inf", v,
                                  start=offset, num=limit)

    def gt(self, v, limit=None, offset=None, withscores=False):
        """Returns the list of the members of the set that have scores
        greater than v.
        """
        if limit is not None and offset is None:
            offset = 0
        return self.zrangebyscore("(%f" % v, "+inf", start=offset, num=limit, withscores=withscores)

    def ge(self, v, limit=None, offset=None, withscores=False):
        """Returns the list of the members of the set that have scores
        greater than or equal to v.

        :param v: the score to compare to.
        :param limit: limit the result to *n* elements
        :param offset: Skip the first *n* elements

        """
        if limit is not None and offset is None:
            offset = 0
        return self.zrangebyscore("%f" % v, "+inf", start=offset, num=limit, withscores=withscores)

    def between(self, min, max, limit=None, offset=None):
        """
        Returns the list of the members of the set that have scores
        between min and max.

        .. Note::
            The min and max are inclusive when comparing the values.

        :param min: the minimum score to compare to.
        :param max: the maximum score to compare to.
        :param limit: limit the result to *n* elements
        :param offset: Skip the first *n* elements

        >>> s = trol.SortedSet('test_between', redis)
        >>> s.add('a', 10)
        1
        >>> s.add('b', 20)
        1
        >>> s.add('c', 30)
        1
        >>> s.between(20, 30)
        ['b', 'c']

        """
        if limit is not None and offset is None:
            offset = 0
        return self.zrangebyscore(min, max, start=offset, num=limit)

    def zadd(self, members, score=1):
        """
        Add members in the set and assign them the score.

        :param members: a list of item or a single item
        :param score: the score the assign to the item(s)

        >>> s = trol.SortedSet('test_zadd', redis)
        >>> s.add('a')
        1
        >>> s.zscore('a')
        1.0
        >>> s.add('b', 20)
        1
        >>> s.zscore('b')
        20.0
        >>> s.add({'c':5, 'd':6})
        2
        >>> s.zscore('d')
        6.0

        """
        if isinstance(members, dict):
            mapping = {self.serialize(member): score for member, score in  members.items()}
        else:
            mapping = {self.serialize(members): score}

        return self.redis.zadd(self.key, mapping)

    def zrem(self, *values):
        """
        Remove the values from the SortedSet

        :return: True if **at least one** value is successfully
                 removed, False otherwise

        >>> s = trol.SortedSet('test_zrem', redis)
        >>> s.add('a', 10)
        1
        >>> s.zrem('a')
        1
        >>> s.members
        []

        """
        values = [self.serialize(v) for v in _parse_values(values)]
        return self.redis.zrem(self.key, *values)

    def zincrby(self, value, att):
        """
        Increment the score of the item by ``value``

        :param value: the value to add to the current score
        :param att: the member to increment
        :returns: the new score of the member

        >>> s = trol.SortedSet('test_zincrby', redis)
        >>> s.add('a', 10)
        1
        >>> s.zincrby(10, 'a')
        20.0

        """
        return self.redis.zincrby(self.key, value, self.serialize(att))

    def zrevrank(self, member):
        """
        Returns the ranking in reverse order for the member

        >>> s = trol.SortedSet('test_zrevrank', redis)
        >>> s.add('a', 10)
        1
        >>> s.add('b', 20)
        1
        >>> s.revrank('a')
        1

        """
        return self.redis.zrevrank(self.key, self.serialize(member))

    def zrange(self, start, stop, withscores=False):
        """
        Returns all the elements including between ``start`` (non included) and
        ``stop`` (included).

        :param withscore: True if the score of the elements should
                          also be returned

        >>> s = trol.SortedSet('test_range', redis)
        >>> s.add('a', 10)
        1
        >>> s.add('b', 20)
        1
        >>> s.add('c', 30)
        1
        >>> s.zrange(1, 3)
        ['b', 'c']
        >>> s.zrange(1, 3, withscores=True)
        [('b', 20.0), ('c', 30.0)]

        """
        if withscores:
            return [(self.deserialize(v), s) for (v, s) in self.redis.zrange(self.key, start, stop, withscores=True)]
        else:
            return [self.deserialize(v) for v in self.redis.zrange(self.key, start, stop, withscores=False)]

    def zrevrange(self, start, end, **kwargs):
        """
        Returns the range of items included between ``start`` and ``stop``
        in reverse order (from high to low)

        >>> s = trol.SortedSet('test_zrevrange', redis)
        >>> s.add('a', 10)
        1
        >>> s.add('b', 20)
        1
        >>> s.add('c', 30)
        1
        >>> s.zrevrange(1, 2)
        ['b', 'a']
        >>> s.clear()

        """
        return[self.deserialize(v) for v in self.redis.zrevrange(self.key, start, end, **kwargs)]

    def zrangebyscore(self, min, max, **kwargs):
        """
        Returns the range of elements included between the scores (min and max)

        >>> s = trol.SortedSet('test_zrangebyscore', redis)
        >>> s.add('a', 10)
        1
        >>> s.add('b', 20)
        1
        >>> s.add('c', 30)
        1
        >>> s.zrangebyscore(20, 30)
        ['b', 'c']

        """
        return [self.deserialize(v) for v in self.redis.zrangebyscore(self.key, min, max, **kwargs)]

    def zrevrangebyscore(self, max, min, **kwargs):
        """
        Returns the range of elements included between the scores (min and max)

        >>> s = trol.SortedSet('test_zrevrangebyscore', redis)
        >>> s.add('a', 10)
        1
        >>> s.add('b', 20)
        1
        >>> s.add('c', 30)
        1
        >>> s.zrevrangebyscore(30, 20)
        ['c', 'b']

        """
        return [self.deserialize(v) for v in self.redis.zrevrangebyscore(self.key, max, min, **kwargs)]

    def zcard(self):
        """
        Returns the cardinality of the SortedSet.

        >>> s = trol.SortedSet('test_zcard', redis)
        >>> s.add("a", 1)
        1
        >>> s.add("b", 2)
        1
        >>> s.add("c", 3)
        1
        >>> s.zcard()
        3

        """
        return self.redis.zcard(self.key)

    def zscore(self, elem):
        """
        Return the score of an element

        >>> s = trol.SortedSet('test_zscore', redis)
        >>> s.add("a", 10)
        1
        >>> s.score("a")
        10.0

        """
        return self.redis.zscore(self.key, self.serialize(elem))

    def zremrangebyrank(self, start, stop):
        """
        Remove a range of element between the rank ``start`` and ``stop`` both included.

        :return: the number of item deleted

        >>> s = trol.SortedSet('test_zremrangebyrank', redis)
        >>> s.add("a", 10)
        1
        >>> s.add("b", 20)
        1
        >>> s.add("c", 30)
        1
        >>> s.zremrangebyrank(1, 2)
        2
        >>> s.members
        ['a']

        """
        return self.redis.zremrangebyrank(self.key, start, stop)

    def zremrangebyscore(self, min_value, max_value):
        """
        Remove a range of element by between score ``min_value`` and
        ``max_value`` both included.

        :returns: the number of items deleted.

        >>> s = trol.SortedSet('test_zremrangebyscore', redis)
        >>> s.add("a", 10)
        1
        >>> s.add("b", 20)
        1
        >>> s.add("c", 30)
        1
        >>> s.zremrangebyscore(10, 20)
        2
        >>> s.members
        ['c']

        """

        return self.redis.zremrangebyscore(self.key, min_value, max_value)

    def zrank(self, elem):
        """
        Returns the rank of the element.

        >>> s = trol.SortedSet('test_zrank', redis)
        >>> s.add({'a': 30, 'b':20, 'c':10})
        3
        >>> s.zrank('b')
        1

        """
        return self.redis.zrank(self.key, self.serialize(elem))

    def eq(self, value):
        """
        Returns the elements that have ``value`` for score.
        """
        return self.zrangebyscore(value, value)

    def __repr__(self):
        """Gets the string representation of this object

        >>> h = trol.SortedSet('test', redis)
        >>> repr(h)
        "<SortedSet 'test'>"

        """
        return "<%s '%s'>" % (self.__class__.__name__, self.key)

    __len__ = zcard
    revrank = zrevrank
    score = zscore
    rank = zrank
    incr_by = zincrby
    add = zadd
    remove = zrem


class Hash(Collection, collections.MutableMapping):
    """This class represent a hash (i.e. dict) object as seen in Redis."""

    def __init__(self, *args, field_typ=str, field_serializer=None, field_deserializer=None, **kwargs):
        super().__init__(*args, **kwargs)

        self.serialize_field = field_serializer or Serializer(field_typ)
        self.deserialize_field = field_serializer or Deserializer(field_typ)

    def hlen(self):
        """Returns the number of elements in the Hash.

        :return: the number of elements in the hash.

        >>> h = trol.Hash('test_hlen', redis)
        >>> h.update(a=1, b=2, c=3)
        >>> h.hlen()
        3

        """
        return self.redis.hlen(self.key)

    def hset(self, field, value):
        """
        Set ``field`` in the Hash to ``value``.

        :returns: 1 if ``field`` is a new value and 0 if it was updated.

        >>> h = trol.Hash('test_hset', redis)
        >>> h.hset("bar", "foo")
        1
        >>> h.hset("bar", "baz")
        0
        >>> h.dict
        {'bar': 'baz'}

        """
        return self.redis.hset(self.key, self.serialize_field(field), self.serialize(value))

    def hdel(self, *fields):
        """
        Delete one or more hash fields by key.

        :param fields: on or more fields to remove.
        :return: the number of fields that were removed

        >>> h = trol.Hash('test_hdel', redis)
        >>> h.update(a=1, b=2, c=3)
        >>> h.hdel("a", "b")
        2
        >>> h.dict
        {'c': 3}

        """
        return self.redis.hdel(self.key, *(self.serialize_field(field) for field in fields))

    def hkeys(self):
        """
        Returns all fields name in the Hash

        >>> h = trol.Hash('test_hkeys', redis)
        >>> h.update(a=1, b=2, c=3)
        >>> h.hkeys()
        ['a', 'b', 'c']

        """
        return [self.deserialize_field(field) for field in self.redis.hkeys(self.key)]

    def hgetall(self):
        """
        Returns all the fields and values in the Hash.

        :return: ``dict`` with all the hash fields and values

        >>> h = trol.Hash('test_hgetall', redis)
        >>> h.update(a=1, b=2, c=3)
        >>> h.dict == {"a": 1, "b": 2, "c": 3}
        True

        """
        return {self.deserialize_field(k): self.deserialize(v) for k, v in self.redis.hgetall(self.key).items()}

    def hvals(self):
        """
        Returns all the values in the Hash

        :return: ``list`` with all the hash values

        >>> h = trol.Hash('test_hvals', redis)
        >>> h.update(a=1, b=2, c=3)
        >>> h.hvals()
        [1, 2, 3]

        """
        return [self.deserialize(v) for v in self.redis.hvals(self.key)]

    def hget(self, field, default=None, raise_error=False):
        """
        Returns the value stored in the field, or the default value unless ``raise_error`` is True.

        :param field: the bytes or string field key to look up.
        :param default: the value to return if the field is not found.
        :param raise_error: whether to raise a ``KeyError`` if the key is not found.

        >>> h = trol.Hash('test_hget', redis)
        >>> h.update(a=1, b=2, c=3)
        >>> h.hget("b")
        2
        >>> h.hget("d") is None
        True
        >>> h.hget("d", default=0)
        0
        >>> h.hget("d", raise_error=True)
        Traceback (most recent call last):
        ...
        KeyError: 'd'

        """
        value = self.redis.hget(self.key, self.serialize_field(field))
        if value is not None:
            return self.deserialize(value)
        elif raise_error:
            raise KeyError(field)
        else:
            return default

    def hmget(self, fields, default=None, raise_error=False):
        """
        Returns the values stored in the fields, or the default value unless ``raise_error`` is True.

        :param fields: an iterable of byte or string fields to retrieve.
        :param default: the value to return if the field is not found.
        :param raise_error: whether to raise a ``KeyError`` if the key is not found.

        >>> h = trol.Hash('test_hmget', redis)
        >>> h.update(a=1, b=2, c=3)
        >>> h.hmget(["a", "b"])
        [1, 2]
        >>> h.hmget(["c", "d"])
        [3, None]
        >>> h.hmget(["c", "d"], default=0)
        [3, 0]
        >>> h.hmget(["c", "d"], raise_error=True)
        Traceback (most recent call last):
        ...
        KeyError: 'd'

        """
        def deserialize(field, value):
            if value is not None:
                return self.deserialize(value)
            elif raise_error:
                raise KeyError(field)
            else:
                return default

        return [deserialize(k, v) for k, v in zip(fields, self.redis.hmget(self.key, *(self.serialize_field(field) for field in fields)))]

    def hexists(self, field):
        """
        Returns ``True`` if the field exists, ``False`` otherwise.

        >>> h = trol.Hash('test_hexists', redis)
        >>> h.update(a=1, b=2, c=3)
        >>> h.hexists("a")
        True
        >>> h.hexists("d")
        False

        """
        return self.redis.hexists(self.key, self.serialize_field(field))

    def hincrby(self, field, increment=1):
        """
        Increment the value of the field.
        :returns: the value of the field after incrementation

        >>> h = trol.Hash('test_hincrby', redis)
        >>> h.hincrby("key", 10)
        10
        >>> h.hincrby("key", 2)
        12

        """
        return self.redis.hincrby(self.key, self.serialize_field(field), increment)

    def hmset(self, mapping):
        """
        Sets or updates the fields with their corresponding values.

        :param mapping: a dict with keys and values
        :return: True if the operation succeeded

        >>> h = trol.Hash('test_hmset', redis)
        >>> h.hmset({"a": 1, "b": 2, "c": 3})
        True
        >>> h.dict == {"a": 1, "b": 2, "c": 3}
        True
        >>> h.hmset({})
        True
        >>> h.dict == {"a": 1, "b": 2, "c": 3}
        True

        """
        if not mapping:
            return True

        mapping = {k: self.serialize(v) for k, v in mapping.items()}
        return self.redis.hmset(self.key, mapping)

    def update(self, *args, **kwargs):
        """
        Sets or updates the fields with their corresponding values, accepting args like the native python dict.update

        :return: None

        >>> h = trol.Hash('test_set_update', redis)
        >>> h.update({"a": 1, "b": 2, "c": 3})
        >>> h.dict == {"a": 1, "b": 2, "c": 3}
        True
        >>> h.update(d=4)
        >>> h["d"]
        4
        >>> h.update([("e", 5)])
        >>> h["e"]
        5

        """

        self.hmset(dict(*args, **kwargs))

    def __repr__(self):
        """Gets the string representation of this object

        >>> h = trol.Hash('test', redis)
        >>> repr(h)
        "<Hash 'test'>"

        """
        return "<%s '%s'>" % (self.__class__.__name__, self.key)

    def __iter__(self):
        return iter(self.dict)

    def __getitem__(self, item):
        return self.hget(item, raise_error=True)

    def _set_dict(self, new_dict):
        self.clear()
        self.hmset(new_dict)

    _get_dict = hgetall

    dict = property(_get_dict, _set_dict)

    def items(self):
        return self.dict.items()

    keys = hkeys
    values = hvals
    get = hget
    __setitem__ = hset
    __delitem__ = hdel
    __len__ = hlen
    __contains__ = hexists
