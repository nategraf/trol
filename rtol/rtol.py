#!/usr/bin/env python3
from weakref import WeakValueDictionary

def highlander(*attrs):
    """A decorator which enables class level object caching

    Add a class level cache and an obtain function which allows retrieval
    This is used for objects where only one needs to or should exist at a time and that object can be shared safely among threads

    "There can only be one!"

    Args:
        *attrs (list[str]): The attribute names which uniquely identify the object in the cache. 
             These are also the arguments which will be passed to __init__ when building the cached object in `obtain`

    Returns:
        Callable[[type], type]: The function which will be applied to decorate the class
    """
    def decoratorfn(cls):
        cls._highlander_cache = WeakValueDictionary()
        
        @classmethod
        def obtain(cls, *args, **kwargs):
            arglen = len(args) + len(kwargs) 
            if arglen != len(attrs):
                raise TypeError("obtain() takes exactly {} argument(s) but {} were given".format(len(attrs), arglen))

            # Build the identifier by adding positional args to a list, then using attrs to disern an order for kwaegs 
            identifier = [*args]
            for attr in attrs[len(args):]:
                identifier.append(kwargs[attr])
            identifier = tuple(identifier) # FREEZE!

            try:
                return cls._highlander_cache[identifier]
            except KeyError:
                inst = cls(*args, **kwargs)
                cls._highlander_cache[identifier] = inst
                return inst

        obtain.__func__.__doc__ = \
        """Retrieves or builds an instance of {cls.__name__} uniquely identified by {attrs}

        If the object must be built, the arguments to this function will be passed to __init__

        Args:
            {attrsfmt}

        Returns:
            {cls.__name__}: Obtained from the cache or built and stored there
        """.format(attrs=attrs, cls=cls, attrsfmt='\n            '.join(attrs))


        cls.obtain = obtain
        return cls

    return decoratorfn


class RemoteProperty(property):
    class Null:
        """A class to act as an indicator value"""

    null = Null()
    """RemoteProperty.Null: An indicator field to show the value needs to be fetched"""

    def __init__(self, name):
        def getter(this):
            try:
                value = getattr(this, this._mangle(name))
            except AttributeError:
                setattr(this, this._mangle(name), self.null)
                value = self.null
            
            if value is self.null:
                pass

            return value

        def setter(this, value):
            setattr(this, this._mangle(name), value)

            if this.autocommit:
                this.commit(name)

        super().__init__(fget = getter, fset = setter)

class RedisModelMeta(type):
    """A metaclass for RedisModel which initialises the instance cache and remote properties""" 
    def __init__(cls, *args, **kwargs):
        cls._remote_properties = [attr for attr, value in cls.__dict__.items() if isinstance(value, RemoteProperty)]
        super().__init__(*args, **kwargs)

class RedisModel(metaclass=RedisModelMeta):
    """A class to support object oriented Redis communication"""
    idprefix = None
    autocommit = True

    @staticmethod
    def _mangle(name):
        return "_property_{}".format(name)

    def rediskey(self):
        if self.idprefix:
            return '{}:{}'.format(self.idprefix, self.identifier)
        return identifier

    def invalidate(self, propname=None):
        if not propname:
            for propname in self._remote_properties:
                setattr(self, self._mangle(propname), RemoteProperty.null)
        else:
            setattr(self, self._mangle(propname), RemoteProperty.null)

    def commit(self, propname=None):
        pass

    def delete(self):
        pass
