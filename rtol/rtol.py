class Property(property):
    class Null:
        """A class to act as an indicator value"""

    null = Null()
    """Property.Null: An indicator field to show the value needs to be fetched"""

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

class ModelMeta(type):
    """A metaclass for Model which initialises the instance cache and remote properties""" 
    def __init__(cls, *args, **kwargs):
        cls._remote_properties = [attr for attr, value in cls.__dict__.items() if isinstance(value, Property)]
        super().__init__(*args, **kwargs)

class Model(metaclass=ModelMeta):
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
                setattr(self, self._mangle(propname), Property.null)
        else:
            setattr(self, self._mangle(propname), Property.null)

    def commit(self, propname=None):
        pass

    def delete(self):
        pass
