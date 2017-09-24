class Property(property):
    class Null:
        """A class to act as an indicator value"""

    null = Null()
    """Property.Null: An indicator field to show the value needs to be fetched"""

    @staticmethod
    def mangle(name):
        """Creates a mangled version of the inputted name

        Mangling is the process of changing an attribute name in such a way that it will liekly not collide with other attributes

        Args:
            name (str): The name which should be mangled

        Returns:
            str: The mangled name
        """
        return "_property_{}".format(name)

    def __init__(self, name, autocommit=True, alwaysfetch=False):
        self.name = name
        self.autocommit = autocommit
        self.alwaysfetch = alwaysfetch

        def getter(obj):
            value = self.value(obj)

            if value is self.null or self.alwaysfetch:
                value = self.fetch(obj)

            return value

        def setter(obj, value):
            self.set(obj, value)

            if self.autocommit:
                self.commit(obj)

        super().__init__(fget=getter, fset=setter)

    def fetch(self, obj):
        """Retrieves and sets the value of this property

        Args:
            obj (object): This property's holder

        Returns:
            bytes: The data retrieved or None in the case of a key not found
        """
        value = obj.redis.get(self.key(obj))
        self.set(obj, value)
        return value

    def commit(self, obj):
        """Commits this properties value to Redis

        Does nothing if the value is Property.null, which means there is nothing to write

        Args:
            obj (object): This property's holder

        Returns:
            bool: True if the transaction was successful. False otherwise
        """
        value = self.value(obj)
        if self.value(obj) is self.null:
            return True

        return obj.redis.set(self.key(obj), value)

    def invalidate(self, obj):
        """Invalidates the local value to indicate a fetch must be done

        Args:
            obj (object): This property's holder
        """
        self.set(obj, self.null)

    def key(self, obj):
        """Gets the key where this property's data exists  in Redis

        The key for this property is the key of it's holder, with `:<name>` appended

        Args:
            obj (object): This propery's holder

        Returns:
            str: The key which can be used to get this property's data from Redis
        """
        if obj.key is not None:
            return "{}:{}".format(obj.key, self.name)
        else:
            return self.name

    def value(self, obj):
        """Gets the value stored in the holder obj

        Sets the property value attribute in the holder if it does not already exist

        Args:
            obj (object): This propery's holder

        Returns:
            object: The local value of this property
        """
        try:
            value = getattr(obj, self.mangle(self.name))
        except AttributeError:
            self.set(obj, self.null)
            value = self.null
        return value

    def set(self, obj, value):
        """Sets the value stored in the holder obj

        Args:
            obj (object): This propery's holder
            value (object): The value to set
        """
        setattr(obj, self.mangle(self.name), value)
