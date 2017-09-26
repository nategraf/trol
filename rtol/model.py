from functools import wraps
from rtol import Property
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


class ModelType(type):
    """A metaclass which provides hiearchy awareness and a list of rtol properties

    To achieve the desired model structure, rtol models need to aware of the model whichs hold them, if one exists
    This is not a feature normally in python, so this type is what facilitates runtime awareness of class hiearchy
    Each time an instance is created, the Models which it contain are subclassed and replaced for that instance

    Exmaple:
        >>> Brewery.Beer._rtol_parent is None
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
        cls._rtol_child_classes = dict()
        cls._rtol_parent = None
        cls._rtol_parent_class = None

        if cls.model_name is None:
            cls.model_name = cls.__name__.strip('&')

        for attrname, attr in cls.__dict__.items():
            if isinstance(attr, Property):
                cls._rtol_properties[attrname] = attr

                if attr.name is None:
                    attr.name = attrname

            if type(attr) is ModelType:
                attr._rtol_parent_class = cls
                cls._rtol_child_classes[attrname] = attr

        initfn_origonal = cls.__init__

        @wraps(initfn_origonal)
        def initfn_wrapper(self, *args, **kwargs):
            initfn_origonal(self, *args, **kwargs)

            for attrname, childcls in self._rtol_child_classes.items():
                cpycls = ModelType.__new__(ModelType, "&{}".format(
                    childcls.__name__), (childcls,), {})
                cpycls._rtol_parent = self
                setattr(self, attrname, cpycls)

        cls.__init__ = initfn_wrapper

        super().__init__(*args, **kwargs)


class Model(metaclass=ModelType):
    """A class to support object oriented Redis communication"""
    id = None
    model_name = None
    autocommit = True
    alwaysfetch = False

    @property
    def key(self):
        """str: Redis key which prefixes any properties stored under this Model instance

        This key is generated recursively walking up the model tree until the root is reached

        Example:

        """
        if self._rtol_parent is not None:
            return ':'.join((self._rtol_parent.key, self.model_name, self.id))
        else:
            return ':'.join((self.model_name, self.id))

    @property
    def redis(self):
        """StrictRedis: The active Redis connection for this model

        This model will walk up the model tree and use the first connection it encounters
        """
        # Use try/except rather than if/else for this block because it should succeed if models are correct
        try:
            return self._rtol_parent.redis
        except AttributeError:
            return None

    def invalidate(self, *propnames):
        if propnames:
            props = list()
            for propname in propnames:
                props.append(self._rtol_properties[propname])
        else:
            props = self._rtol_properties.values()

        for prop in props:
            prop.invalidate(self)

    def commit(self, *propnames):
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
                mappings[prop.key(self)] = value

        self.redis.mset(mappings)

    def delete(self, *propnames):
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
        commits = list()
        for propname, value in kwargs.items():
            prop = self._rtol_properties[propname]
            prop.set(self, value)

            if prop.autocommit or (prop.autocommit is None and self.autocommit):
                commits.append(propname)

        if commits:
            self.commit(*commits)
