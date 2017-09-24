class ModelMeta(type):
    """A metaclass for Model which initialises the instance cache and remote properties"""
    def __init__(cls, *args, **kwargs):
        cls._remote_properties = [
            attr for attr, value in cls.__dict__.items() if isinstance(value, Property)]
        super().__init__(*args, **kwargs)


class Model(metaclass=ModelMeta):
    """A class to support object oriented Redis communication"""
    idprefix = None
    autocommit = True

    def rediskey(self):
        if self.idprefix:
            return '{}:{}'.format(self.idprefix, self.identifier)
        return identifier

    def invalidate(self, propname=None):
        if not propname:
            for propname in self._remote_properties:
                setattr(self, Property.mangle(propname), Property.null)
        else:
            setattr(self, Property.mangle(propname), Property.null)

    def commit(self, propname=None):
        pass

    def delete(self):
        pass
