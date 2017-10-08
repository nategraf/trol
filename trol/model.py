from functools import wraps
from trol import Property, Collection, deserializer, serializer
import weakref

"""Provides the Model and ModelType classes, which are the basic blocks of trol

The Model class is what trol data models derive from.
The examples in these docs will use the following data model

"""

_all_models = weakref.WeakValueDictionary()


class ModelType(type):
    """A metaclass which provides awareness of properties and collections

    This type embeds a dict of :obj:`Property` which tracks any properties assigned at class load.
    It assigns the names of properties who are unamed
    This type embeds a dict of :obj:`Collection` which tracks any collections assigned at class load
    It will assign the names to any collections which did not have their names assigned
    """
    def __init__(cls, *args, **kwargs):
        cls._rtol_properties = dict()
        cls._rtol_collections = dict()

        for attrname, attr in cls.__dict__.items():
            if isinstance(attr, Property):
                cls._rtol_properties[attrname] = attr

                if attr._name is None:
                    attr._name = attrname

            if isinstance(attr, Collection):
                cls._rtol_collections[attrname] = attr

                if attr._name is None:
                    attr._name = attrname

        _all_models[cls.__name__] = cls

        super().__init__(*args, **kwargs)


class Model(metaclass=ModelType):
    """A class to support object oriented Redis communication

    Attributes:
        autocommit (bool): If `True`, properties will be immediatly commited to Redis when modified. Default is `True`
            This attribute can be overriden for a single property by setting that properties `autocommit` attribute
        alwaysfetch (bool): If `True`, property values will be fetched from Redis on every access. Deafault is `False`
            This attribute can be overriden for a single property by setting that properties `alwaysfetch` attribute
    """
    _redis = None
    _key = None
    _model_name = None

    autocommit = True
    alwaysfetch = False

    @property
    def key(self):
        """str: Redis key which prefixes any properties stored under this Model instance

        By default this is {model_name}:{id}

        Changing the key of an instance may cause data to be lost.
        It's best to think of these models as a pointer and changing the keys is changing the value of the pointer

        Example:
            TODO: Write a new example

        """
        if self._key is not None:
            return self._key

        return ':'.join((self.model_name, self.id))

    @key.setter
    def key(self, key):
        self._key = key

    @property
    def model_name(self):
        """str: A name for this model which, in addition to its id which identify it in Redis

        By default this is the class name

        Example:
            TODO: Write a new example

        """
        if self._model_name is not None:
            return self._model_name

        return self.__class__.__name__

    @model_name.setter
    def model_name(self, name):
        self._model_name = name

    @property
    def redis(self):
        """Redis: The active Redis connection for this model

        This model can have it's own Redis connection of use connection of the :obj:`Database` that holds it
        """
        if self._redis is not None:
            return self._redis

        # Use try/except rather than if/else for this block because it should succeed if models are correct
        try:
            return self._rtol_database.redis
        except AttributeError:
            return None

    @redis.setter
    def redis(self, redis):
        self._redis = redis

    def invalidate(self, *propnames):
        """Mark properties in this model as invalid and requiring a fetch

        Args:
            *propnames (list[str]): The attribute nanes of properties which should be invalidated.
                If none are provided, the default is to invalidate all propertoes in the model.
        """
        if propnames:
            props = list()
            for propname in propnames:
                props.append(self._rtol_properties[propname])
        else:
            props = self._rtol_properties.values()

        for prop in props:
            prop.invalidate(self)

    def commit(self, *propnames):
        """Saves properties in this model to Redis

        Args:
            *propnames (list[str]): The attribute nanes of properties which should be committed.
                If none are provided, the default is to commits all propertoes in the model.
        """
        if propnames:
            props = list()
            for propname in propnames:
                props.append(self._rtol_properties[propname])
        else:
            props = self._rtol_properties.values()

        mappings = dict()
        for prop in props:
            value = prop.value(self)
            if value is not prop.null:
                mappings[prop.key(self)] = prop.serialize(value)

        self.redis.mset(mappings)

    def delete(self, *propnames):
        """Deletes properties in this model from Redis

        Args:
            *propnames (list[str]): The attribute nanes of properties which should be deleted.
                If none are provided, the default is to delted all propertoes in the model.
        """
        if propnames:
            props = list()
            for propname in propnames:
                props.append(self._rtol_properties[propname])
        else:
            props = self._rtol_properties.values()

        keys = []
        for prop in props:
            keys.append(prop.key(self))

        self.redis.delete(*keys)

        for prop in props:
            prop.set(self, prop.null)

    def update(self, **kwargs):
        """Updates the local values of multiple properties and commits them if autocommit is set

        Args:
            **kwargs (dict[str, object]): Key value pairs where the key is the property name and value is what it should be set to
        """
        commits = list()
        for propname, value in kwargs.items():
            prop = self._rtol_properties[propname]
            prop.set(self, value)

            if prop.autocommit or (prop.autocommit is None and self.autocommit):
                commits.append(propname)

        if commits:
            self.commit(*commits)


_seperator = b'\xfe'
_indicator = b'\xfc'


@serializer(Model)
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


class ModelDeserializationError(Exception):
    def __init___(self, key):
        self.key = key

    def __str__(self):
        return "Failed to deserialize '{}' to a Model".format(self.key)


@deserializer(Model)
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

        cls = _all_models[key[0]]
        inst = cls.__new__(cls)

        if key[1] is not None:
            inst.id = key[1]
        inst._model_name = key[2]
        inst._key = key[3]
        return inst

    except Exception as err:
        raise ModelDeserializationError(byts) from err