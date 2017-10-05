from trol import ModelType, Collection, Property


class DatabaseType(type):
    def __init__(cls, *args, **kwargs):
        """
        TODO: Write a docstring
        """
        cls._rtol_properties = dict()
        cls._rtol_collections = dict()
        cls._rtol_models = dict()

        for attrname, attr in cls.__dict__.items():
            if isinstance(attr, Property):
                cls._rtol_properties[attrname] = attr

                if attr._name is None:
                    attr._name = attrname

            if isinstance(attr, Collection):
                cls._rtol_collections[attrname] = attr

                if attr._name is None:
                    attr._name = attrname

                if attr._redis is None:
                    attr._redis = getattr(cls, 'redis', None)

            if isinstance(attr, ModelType):
                cls._rtol_models[attrname] = attr
                attr._rtol_database = cls

        super().__init__(*args, **kwargs)


class Database(metaclass=DatabaseType):
    # Wait a minute! This class is empty
    # Ya, but it gives you the nice syntactic sugar of:
    #     class MyDB(Database):
    #
    # and avoids the question "What's a metaclass?"
    redis = None
