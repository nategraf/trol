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


_seperator = b'\xfe'
_indicator = b'\xfc'


def serialize_model(model):
    """Serialize a model instance into a key reference

    The model class, id, model_name, and key will be preserved on serialization
    Any custom attributes of the instance will not

    Args:
        model (Model): The model to serialize

    Returns:
        bytes: The key reference
    """
    model_id = getattr(model, 'id', None)
    class_name = model.__class__.__name__.encode('utf-8')
    model_id = _indicator if model_id is None else str(
        model_id).encode('utf-8')
    model_name = _indicator if model._model_name is None else model._model_name.encode(
        'utf-8')
    model_key = _indicator if model._key is None else model._key.encode(
        'utf-8')

    key = (class_name, model_id, model_name, model_key)
    return _seperator.join(key)


serializers = {
    str: serialize_str,
    int: serialize_int,
    float: serialize_float,
    bytes: serialize_bytes,
    # Model: serialize_model
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


class ModelDeserializationError(Exception):
    def __init___(self, key):
        self.key = key

    def __str__(self):
        return "Failed to deserialize '{}' to a Model".format(self.key)


def deserialize_model(byts):
    """Deserialize a key reference into a model instance

    The model class, id, model_name, and key will be set on deserialization
    Any custom attributes of the instance will not, and __init__ will not be called

    Args:
        bytes: The key reference

    Returns:
        model (Model): The deserialized model
    """
    try:
        pieces = byts.split(_seperator)

        key = []
        for piece in pieces:
            if piece == _indicator:
                key.append(None)
            else:
                key.append(piece.decode('utf-8'))

        cls = rtol.model._all_models[key[0]]
        inst = cls.__new__(cls)

        if key[1] is not None:
            inst.id = key[1]
        inst._model_name = key[2]
        inst._key = key[3]
        return inst

    except Exception as err:
        raise ModelDeserializationError(byts) from err


deserializers = {
    str: deserialize_str,
    int: deserialize_int,
    float: deserialize_float,
    bytes: deserialize_bytes,
    # Model: deserialize_model
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
