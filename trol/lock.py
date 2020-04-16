import redis
import threading

class Lock:
    """Lock provides a Redis-backed distributed lock on a Model or Database instance.

    Lock should be a class member of a Model class, and provides a unique lock to each instance of
    that model. It is essentially a factory class for `redis-py's Lock objects`_

    .. _redis-py's Lock objects: https://redis-py.readthedocs.io/en/latest/#redis.Redis.lock

    Attributes:
        timeout (float or None): Maximum time the lock can be held before it expires, releasing it automatically. When set to None, the lock never expired.
        sleep (float): Time, in seconds, to sleep between each attempt to acquire the lock.
        blocking_timeout (float or None): Maximum time to spend acquiring the lock.
        lock_class (type or None): Class used to construct the lock. See `redis.Lock`_ for the canonical version.
        thread_local (bool): Whether to use threadlocal storage when to store the reservation token.

    .. _redis.Lock: https://github.com/andymccurdy/redis-py/blob/master/redis/lock.py

    .. TODO:: Lock currently only works when bound to an object, and not directly from the Database
        class. Lock should be refactored as a subclass of redis.lock.Lock to allow direct operations
        (instead of being built during the ``__get__`` access) and/or Database should be refactored to no
        longer rely on janky "class-binding".

    >>> import trol
    >>> import time
    >>> from threading import Thread
    >>> from redis import Redis
    ...
    >>> class Sleepy(trol.Model):
    ...     redis = Redis()
    ...     sleepy_lock = trol.Lock()
    ...     sleep = time.sleep
    ...
    ...     def __init__(self, id):
    ...         self.id = id
    ...     
    ...     def print(self, t, msg):
    ...         with self.sleepy_lock:
    ...             self.sleep(t)
    ...             print(msg)
    ...
    >>> sleepy = Sleepy('foo')
    >>> Thread(target=sleepy.print, args=(3, "ok, go ahead")).start()
    >>> Thread(target=sleepy.print, args=(1, "my turn!")).start()
    >>> time.sleep(5)
    ok, go ahead
    my turn!

    Just like with Property, the name of the lock is used to form it's Redis key and can either by
    specified explicitly in the constructor or inferred from the attribute name in a Model.

    >>> with sleepy.sleepy_lock:
    ...     print(sleepy.redis.keys())
    [b'Sleepy:foo:sleepy_lock']
    >>> print(sleepy.redis.keys())
    []

    """

    @staticmethod
    def mangle(name):
        """Creates a mangled version of the given name.

        Mangling produces a name unlikely to collide with other attribute names.

        Args:
            name (str): The name which should be mangled

        Returns:
            str: Mangled name
        """
        return "_trol_lock_{}".format(name)

    def __init__(self, name=None, timeout=None, sleep=0.1, blocking_timeout=None, lock_class=None, thread_local=True):
        self.name = name
        self.timeout = timeout
        self.sleep = sleep
        self.blocking_timeout = blocking_timeout
        self.lock_class = lock_class
        self.thread_local = thread_local
        self._inception_lock = threading.Lock()

    def __get__(self, obj, cls=None):
        if obj is None:
            return self

        lock = getattr(obj, self.mangle(self.name), None)

        # If the lock instance doesn't exist, build it.
        if lock is None:
            with self._inception_lock:
                lock = getattr(obj, self.mangle(self.name), None)
                if lock is None:
                    lock = self.build(obj)

        return lock

    @property
    def name(self):
        """``str``: The name for this property, which will be used to determine the key when bound to a Model"""
        return self._name

    @name.setter
    def name(self, value):
        self._name = value

    def key(self, obj):
        """Gets the key where this lock exists in Redis

        The key for this lock is the key of it's holder, with ``:<name>`` appended

        Args:
            obj (object): Model instance for which locking is provided.

        Returns:
            str: The key which can be used to synchronize this lock data in Redis
        """
        if obj.key is not None:
            return "{}:{}".format(obj.key, self.name)
        else:
            return self.name

    def build(self, obj):
        """Builds a lock instance and assigns it to a field in obj for later retrieval.

        Args:
            obj (object): Model instance for which locking is provided.

        Returns:
            object: Lock of type specified as lock_class. Probably redis.Lock.
        """
        lock = obj.redis.lock(
            name=self.key(obj),
            timeout=self.timeout,
            sleep=self.sleep,
            blocking_timeout=self.blocking_timeout,
            lock_class=self.lock_class,
            thread_local=self.thread_local
        )
        setattr(obj, self.mangle(self.name), lock)
        return lock
