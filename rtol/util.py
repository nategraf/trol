"""A set of utility functions for use in multiple parts of rtol

This module currently contains the serialize and deserialize methods for the Property and Collection classes I implement
"""

import weakref
import rtol

# redis-py did this for me already, no need to reinvent the wheel :P

def serialize_str(obj):
    return obj


def serialize_int(obj):
    return obj


def serialize_float(obj):
    return obj


def serialize_bytes(obj):
    return obj

_model_cache = weakref.WeakValueDictionary()
_cls_splitter = b'\xfc'
_inst_splitter = b'\xfe'
def serialize_model(model):
    """Serialize a model instance into a key reference

    Args:
        model (Model): The model to serialize

    Returns:
        bytes: The key reference
    """
    # Walk the madel tree to assemble the ref
    # The resulting key will be a tuple of structure (class, id, class, id, ...)
    key = []
    curr = model
    untethered = False
    while curr is not None:
        key = [curr.model_name, curr.id] + key
        curr = curr.__class__._rtol_parent

        if isinstance(curr, rtol.ModelType):
            untethered = True
            clskey = []
            while curr is not None:
                clskey = [curr.model_name] + clskey
                curr = curr._rtol_parent
            key.insert(0, tuple(clskey))

    key = tuple(key)
    
    # To make deserialization of this model easier
    _model_cache[key[0:-1]] = model.__class__

    if untethered:
        serial = _cls_splitter.join([str(k).encode('utf-8') for k in key[0]]) + _cls_splitter
        return serial + _inst_splitter.join([str(k).encode('utf-8') for k in key[1:]])
    return _inst_splitter.join([str(k).encode('utf-8') for k in key])


serializers = {
    str: serialize_str,
    int: serialize_int,
    float: serialize_float,
    bytes: serialize_bytes,
    #Model: serialize_model
}
""" dict[type, Callable[[object], bytes]]: A dictionary of serializers known rtol classes

Additonal entries can be added to support new serializable types

>>> import redis
>>> import rtol
>>> class HotNewClass:
...     def __init__(self, howhot):
...         self.howhot = howhot
...
>>> def hotnew_serializer(hnc):
...     print("HNC IS BEING SERIALIZED!")
...     return '<HOT>{}'.format(hnc.howhot)
...
>>> rtol.serializers[HotNewClass] = hotnew_serializer
>>> def hotnew_deserializer(byts):
...     print("RETURN OF THE HNC!")
...     howhot = int(byts.decode('utf-8').strip('<HOT>'))
...     return HotNewClass(howhot)
...
>>> rtol.deserializers[HotNewClass] = hotnew_deserializer
>>> class SweetModel(rtol.Model):
...     def __init__(self, ident, redis):
...         self.id = ident
...         self.redis = redis
...
...     bar = rtol.Property(typ=HotNewClass)
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

"""


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

def _break_key(byts):
    broken = byts.split(_inst_splitter)
    clspart = None
    if broken:
        if _cls_splitter in broken[0]:
            clspart = tuple(piece.decode('utf-8') for piece in broken.pop(0).split(_cls_splitter))

    key = [piece.decode('utf-8') for piece in broken]
    if clspart:
        key.insert(0, clspart)
    return tuple(key)

class ModelDeserializationError(Exception):
    def __init___(self, key):
        self.key = key

    def __str__(self):
        return "Failed to deserialize '{}' to a Model".format(self.key)

def deserialize_model(byts):
    key = _break_key(byts)
    
    def dereference_class_key

    def dereference_key(key):
        if not key:
            # Empty key. no way to resolve this one
            return None

        try:
            # Best case: This Model was serialized locally and cached
            cls = _model_cache[key[0:-1]]
        except KeyError:
            if len(key) > 2:
                parent = dereference_key(key[0:-2])
                if parent is not None:
                    # We found a parent. Now look for a child with the right name
                    for cls in parent._rtol_child_classes:
                        if cls.model_name == key[-2]:
                            break
                        else:
                            # We couldn't walk down the tree. Resolution failed
                            return None
                else:
                    # The parent could not be found. Must have failed for one of the other reasons
                    return None

            elif len(key) == 2:
                # This is the last segment. Hopefully it's a model root and we can walk the tree
                for cls in rtol.model._model_roots:
                    if cls.model_name == key[-2]:
                        break
                    else:
                        # This model name was not found in the roots. Possibly a custom key or model we don't have
                        return None
            else:
                # This key has an odd number of parts. Must be malformed
                return None

        _model_cache[key[0:-1]] = cls

        inst = cls.__new__(cls)
        inst.id = key[-1]
        return inst

    inst = dereference_key(key)
    if inst is None:
        raise ModelDeserializationError(key)

    return inst


deserializers = {
    str: deserialize_str,
    int: deserialize_int,
    float: deserialize_float,
    bytes: deserialize_bytes,
    #Model: deserialize_model
}
""" dict[type, Callable[[bytes], object]]: A dictionary of deserializers known rtol classes

Additonal entries can be added to support new deserializable types
There should be an entry here for each one in serializers
"""


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
