"""A set of utility functions for use in multiple parts of trol

This module currently contains the serialize and deserialize methods for the Property and Collection classes I implement
"""

import weakref
import trol


def serialize_str(obj):
    return obj

def serialize_int(obj):
    return obj

def serialize_float(obj):
    return obj

def serialize_bytes(obj):
    return obj

def serialize_bool(obj):
    if obj:
        return b'True'
    else:
        return b'False'

serializers = {
    str: serialize_str,
    int: serialize_int,
    float: serialize_float,
    bytes: serialize_bytes,
    bool: serialize_bool,
}
""" dict[type, Callable[[object], bytes]]: A dictionary of serializers known trol classes

Additonal entries can be added to support new serializable types

>>> import redis
>>> import trol
...
>>> class HotNewClass:
...     def __init__(self, howhot):
...         self.howhot = howhot
...
>>> @trol.serializer(HotNewClass)
... def hotnew_serializer(hnc):
...     print("HNC IS BEING SERIALIZED!")
...     return '<HOT>{}'.format(hnc.howhot)
...
>>> @trol.deserializer(HotNewClass)
... def hotnew_deserializer(byts):
...     print("RETURN OF THE HNC!")
...     howhot = int(byts.decode('utf-8').strip('<HOT>'))
...     return HotNewClass(howhot)
...
>>> class SweetModel(trol.Model):
...     def __init__(self, ident, redis):
...         self.id = ident
...         self.redis = redis
...
...     bar = trol.Property(typ=HotNewClass)
...
>>> r = redis.Redis('localhost')
>>> sm = SweetModel('xyz', r)
>>> sm.bar = HotNewClass(10)
HNC IS BEING SERIALIZED!
>>> r.get(sm.key + ':bar')
b'<HOT>10'
>>> sm.invalidate()
>>> sm.bar.howhot
RETURN OF THE HNC!
10
>>> r.flushall()
True

"""


def serializer(cls):
    """A convinience decorator to register a serializer"""
    def decorator(f):
        serializers[cls] = f
        return f
    return decorator

class Serializer:
    """A class containing the provided serialize functions for selected type

    The serializers here are aim for human-readability, which means they are not their most optimized
    If you want better performance through avoidance to and from `str`, implement a new serializer

    Attributes:
        func (Callable[[object], object]: The serialization function this class is constructed to use
            The input and return types are determined by the type used to construct this object
            The output type will be a serial type accepted by redis-py

    Args:
        typ (type): The type of objects which will be serialized by this Serializer
            Currently supported types are: `str`, `int`, `float`, and `bytes`
    """

    def __init__(self, typ):
        try:
            self.func = serializers[typ]
        except KeyError:
            raise ValueError("{} is not supported".format(typ.__name__))

    def __call__(self, obj):
        """Serialize the input object into a form acceptable to redis-py

        Args:
            obj (object): An object of the same type used to construct this Serializer

        Returns:
            object: A serialized version acceptable to redis-py
        """
        return self.func(obj)


def deserialize_str(byts):
    return byts.decode('utf-8')

def deserialize_int(byts):
    return int(byts.decode('utf-8'))

def deserialize_float(byts):
    return float(byts.decode('utf-8'))

def deserialize_bytes(byts):
    return byts

def deserialize_bool(byts):
    if byts == b'True':
        return True
    else:
        return False

deserializers = {
    str: deserialize_str,
    int: deserialize_int,
    float: deserialize_float,
    bytes: deserialize_bytes,
    bool: deserialize_bool
}
""" dict[type, Callable[[bytes], object]]: A dictionary of deserializers known trol classes

Additonal entries can be added to support new deserializable types
There should be an entry here for each one in serializers
"""

def deserializer(cls):
    """A convinience decorator to register a deserializer"""
    def decorator(f):
        deserializers[cls] = f
        return f
    return decorator

class Deserializer:
    """A class containing the provided deserialize functions for selected type

    The deserializers here are aim for human-readability, which means they are not their most optimized
    If you want better performance through avoidance to and from `str`, implement a new deserializer

    Attributes:
        func (Callable[[bytes], object]: The deserialization function this class is constructed to use
            The return type is determined by the type used to construct this object

    Args:
        typ (type): The type of objects which will be deserialized by this Deserializer
            Currently supported types are: `str`, `int`, `float`, and `bytes`
    """

    def __init__(self, typ):
        try:
            self.func = deserializers[typ]
        except KeyError:
            raise ValueError("{} is not supported".format(typ.__name__))

    def __call__(self, obj):
        """Deserialize the input bytes from redis-py into the desired object type

        Args:
            byts (bytes): A bytes object containing the data you want to deserialize

        Returns:
            object: An object of the desired type
        """
        return self.func(obj)
