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
                raise TypeError("obtain() takes exactly {} argument(s) but {} were given".format(
                    len(attrs), arglen))

            # Build the identifier by adding positional args to a list, then using attrs to disern an order for kwargs
            identifier = [*args]
            for attr in attrs[len(args):]:
                identifier.append(kwargs[attr])
            identifier = tuple(identifier)  # FREEZE!

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
