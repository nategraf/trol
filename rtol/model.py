from functools import wraps
from rtol import Property, Collection
import weakref

"""Provides the Model and ModelType classes, which are the basic blocks of rtol

The Model class is what rtol data models derive from.
The examples in these docs will use the following data model

Example:
    >>> import rtol
    >>> import uuid
    >>> import redis
    >>>
    >>> class Brewery(rtol.Model):
    ...     def __init__(self, name, location=None):
    ...         self.name = name
    ...         self.location = location
    ...
    ...     redis = redis.StrictRedis('localhost')
    ...     name = rtol.Property()
    ...     location = rtol.Property()
    ...
    ...     @property
    ...     def id(self):
    ...         return self.name
    ...
    ...     class Employee(rtol.Model):
    ...         def __init__(self, firstname, lastname):
    ...             self.update(
    ...                 first_name = firstname,
    ...                 last_name = lastname
    ...             )
    ...
    ...         first_name = rtol.Property()
    ...         last_name = rtol.Property()
    ...         # Add a schedule list here when it is ready
    ...
    ...         @property
    ...         def id(self):
    ...             return "{} {}".format(self.first_name, self.last_name)
    ...
    ...         class Schedule(rtol.Model):
    ...             def __init__(self):
    ...                 self.id = uuid.uuid4()
    ...
    ...             date = rtol.Property()
    ...             time_start = rtol.Property()
    ...             time_end = rtol.Property()
    ...
    ...     class Beer(rtol.Model):
    ...         def __init__(self, shorthand, name):
    ...             self.id = shorthand
    ...             self.name = name
    ...
    ...         name = rtol.Property()
    ...         ingredients = rtol.Property()
    ...         price = rtol.Property()
    ...

"""

_live_model_set = weakref.WeakSet()

class ModelType(type):
    """A metaclass which provides hiearchy awareness and a list of rtol properties

    To achieve the desired model structure, rtol models need to aware of the model whichs hold them, if one exists
    This is not a feature normally in python, so this type is what facilitates runtime awareness of class hiearchy
    Each time an instance is created, the Models which it contain are subclassed and replaced for that instance

    Exmaple:
        >>> Brewery.Beer._rtol_parent is Brewery
        True
        >>> fremont = Brewery('Fremont Brewing')
        >>> fremont.Employee._rtol_parent is fremont
        True
        >>> fremont.Employee is not Brewery.Employee
        True
        >>> issubclass(fremont.Employee, Brewery.Employee)
        True
        >>> alex = fremont.Employee("Alex", "Montreal")
        >>> alex.Schedule._rtol_parent is alex
        True
        >>> alex._rtol_parent._rtol_parent is None
        True

    This type also embeds a dict of rtol.Property which it holder and stores it as _rtol_properties and sets the names if None
    """
    def __init__(cls, *args, **kwargs):
        cls._rtol_properties = dict()
        cls._rtol_collections = dict()
        cls._rtol_child_classes = dict()
        cls._rtol_parent = None
        cls._rtol_model_name = None

        for attrname, attr in cls.__dict__.items():
            if isinstance(attr, Property):
                cls._rtol_properties[attrname] = attr

                if attr._name is None:
                    attr._name = attrname

            if isinstance(attr, Collection):
                cls._rtol_collections[attrname] = attr

                if attr._name is None:
                    attr._name = attrname

            if isinstance(attr, ModelType):
                cls._rtol_child_classes[attrname] = attr
                attr._rtol_parent = cls

        _live_model_set.add(cls)

        super().__init__(*args, **kwargs)

    def __get__(cls, obj, typ):
        if obj is None:
            return cls

        cache_attr = '_rtol_child_class_{}'.format(cls.__name__)
        if not hasattr(obj, cache_attr):
            proxycls = ModelType.__new__(
                ModelType,
                "&{}".format(cls.__name__),
                (cls,),
                {
                    '_rtol_parent': obj
                })
            setattr(obj, cache_attr, proxycls)
            _live_model_set.add(proxycls)
        return getattr(obj, cache_attr)

    @property
    def model_name(cls):
        if cls._rtol_model_name is not None:
            return cls._rtol_model_name
        else:
            return cls.__name__.strip('&')

    @model_name.setter
    def model_name(cls, name):
        cls._rtol_model_name = name

    @property
    def key(cls):
        if cls._key is not None:
            return cls._key

        if cls._rtol_parent is not None:
            return '{}{};'.format(cls._rtol_parent.key, cls.model_name)
        else:
            return '{};'.format(cls.model_name)

    @key.setter
    def key(cls, key):
        self._key = key


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

    autocommit = True
    alwaysfetch = False

    @property
    def model_name(self):
        """str: The name which will be used to identify this model, and it's insatnces in Redis
        
        Model name can only be set at the class level.
        It is also highly recomemded that you not the model_name exact in the defininition of the class 
        Doing so will move the key and make any previously set data inaccessible

        Example:
            >>> class University(rtol.Model):
            ...     def __init__(self, name):
            ...         self.id = name
            ...
            >>> university = University("tamu")
            >>> university.key
            'University:tamu'
            >>> University.model_name = "uni"
            >>> university.key
            'uni:tamu'

        """
        # The '&' is added as a marker that this is a copy of the original class
        return self.__class__.model_name

    @property
    def key(self):
        """str: Redis key which prefixes any properties stored under this Model instance

        This key is generated recursively walking up the model tree until the root is reached

        Example:
            >>> class Host(rtol.Model):
            ...     def __init__(self, ip):
            ...         self.id = ip
            ...
            ...     class Service(rtol.Model):
            ...         def __init__(self, protocol, port):
            ...             self.id = '{}/{}'.format(protocol, port)
            ...
            ...         class Resource(rtol.Model):
            ...             def __init__(self, path):
            ...                 self.id = path
            ...
            >>> host = Host('192.30.253.167')
            >>> host.key
            'Host:192.30.253.167'
            >>> serv = host.Service('tcp', 22)
            >>> serv.key
            'Host:192.30.253.167:Service:tcp/22'
            >>> res = serv.Resource('nategraf/RedisThinObjectLayer')
            >>> res.key
            'Host:192.30.253.167:Service:tcp/22:Resource:nategraf/RedisThinObjectLayer'

        """
        if self._key is not None:
            return self._key
        
        if self.__class__._rtol_parent is not None:
            if isinstance(self.__class__._rtol_parent, ModelType):
                return '{}{}:{}'.format(self.__class__._rtol_parent.key, self.model_name, self.id)
            else:
                return '{}:{}:{}'.format(self.__class__._rtol_parent.key, self.model_name, self.id)
        else:
            return '{}:{}'.format(self.model_name, self.id)

    @key.setter
    def key(self, key):
        self._key = key

    @property
    def redis(self):
        """StrictRedis: The active Redis connection for this model

        This model will walk up the model tree and use the first connection it encounters
        """
        if self._redis is not None:
            return self._redis

        # Use try/except rather than if/else for this block because it should succeed if models are correct
        try:
            return self._rtol_parent.redis
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
