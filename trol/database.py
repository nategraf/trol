from . import ModelType, Collection, Property


class DatabaseType(type):
    def __init__(cls, *args, **kwargs):
        """DatabaseType represensents a trol managed database.
        
        Model, Collection and Property objects are gain reflected name awareness and a database collection from the
        parent Database class.
        """
        cls._trol_properties = dict()
        cls._trol_collections = dict()
        cls._trol_models = dict()

        for attrname, attr in cls.__dict__.items():
            if isinstance(attr, Property):
                cls._trol_properties[attrname] = attr

                if attr._name is None:
                    attr._name = attrname

            if isinstance(attr, Collection):
                cls._trol_collections[attrname] = attr

                if attr._name is None:
                    attr._name = attrname

                attr._trol_database = cls

            if isinstance(attr, ModelType):
                cls._trol_models[attrname] = attr
                attr._trol_database = cls

        super().__init__(*args, **kwargs)


class Database(metaclass=DatabaseType):
    # Wait a minute! This class is empty
    # Ya, but it gives you the nice syntactic sugar of:
    #     class MyDB(Database):
    #
    # and avoids the question "What's a metaclass?"
    redis = None
