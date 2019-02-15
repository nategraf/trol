import docker
import unittest
import pickle
from redis import Redis
from trol import Property
from .common import ensure_redis_is_online


class A:
    def __init__(self, redis=None, autocommit=False):
        self.redis = redis

    alpha = Property("abc", autocommit=False)


class OfflinePropertyTests(unittest.TestCase):
    def test_mangle(self):
        nameA, nameB = "fred", "wilma"
        self.assertNotEqual(Property.mangle(nameA), nameA)
        self.assertNotEqual(Property.mangle(nameA), Property.mangle(nameB))

    def test_key(self):
        class X:
            prop = Property("prop")
            key = None

        x = X()
        self.assertEquals(X.prop.key(x), "prop")

        x.key = "xkey"
        self.assertEquals(X.prop.key(x), "xkey:prop")

    def test_value_and_set(self):
        class X:
            prop = Property("prop")
            key = None

        x = X()
        self.assertIs(X.prop.value(x), X.prop.null)
        canary = object()
        X.prop.set(x, canary)
        self.assertIs(X.prop.value(x), canary)


class OnlinePropertyTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.container_token = ensure_redis_is_online()
        cls.redis = Redis(host='localhost', port=6379, db=0)

    def setUp(self):
        self.redis.flushall()

    def test_self(self):
        """Make sure the docker container can be booted and redis can be connected to

        If this test fails, then the rest surely won't pass
        """
        self.redis.set("key", "value")
        self.assertEquals(self.redis.get("key").decode('utf-8'), "value")
        self.redis.delete("key")

    def test_commit_nothing(self):
        class X:
            prop = Property("p")
            key = "xkey"

        x = X()
        key = X.prop.key(x)
        self.assertIsNone(self.redis.get(key))

        self.redis.set(key, "canary")
        self.assertTrue(X.prop.commit(x))
        self.assertEquals(self.redis.get(key).decode('utf-8'), "canary")

    def test_commit_something(self):
        class X:
            redis = self.redis
            prop = Property("p")
            key = "xkey"

        x = X()
        key = X.prop.key(x)
        X.prop.set(x, "something")
        self.assertIsNone(self.redis.get(key))

        self.redis.set(key, "canary")
        self.assertTrue(X.prop.commit(x))
        self.assertEquals(pickle.loads(self.redis.get(key)), "something")

    def test_setter_autocommit(self):
        class X:
            redis = self.redis
            prop = Property("p", autocommit=True)
            key = "xkey"

        x = X()
        key = X.prop.key(x)
        self.assertIsNone(self.redis.get(key))
        self.redis.set(key, "canary")

        x.prop = "something"
        self.assertEquals(pickle.loads(self.redis.get(key)), "something")

    def test_setter_no_autocommit(self):
        class X:
            redis = self.redis
            prop = Property("p", autocommit=False)
            key = "xkey"

        x = X()
        key = X.prop.key(x)
        self.assertIsNone(self.redis.get(key))
        self.redis.set(key, "canary")

        x.prop = "something"
        self.assertEquals(self.redis.get(key).decode('utf-8'), "canary")
        self.assertTrue(X.prop.commit(x))
        self.assertEquals(pickle.loads(self.redis.get(key)), "something")

    def test_redis_returns_null(self):
        class X:
            redis = self.redis
            prop = Property("p", alwaysfetch=True)
            key = "xkey"

        x = X()
        key = X.prop.key(x)
        self.assertIs(x.prop, Property.null)

    def test_getter_alwaysfetch(self):
        class X:
            redis = self.redis
            prop = Property("p", alwaysfetch=True)
            key = "xkey"

        x = X()
        key = X.prop.key(x)

        self.assertIs(x.prop, Property.null)

        self.redis.set(key, pickle.dumps("canary"))
        self.assertEquals(x.prop, "canary")

    def test_getter_no_alwaysfetch(self):
        class X:
            redis = self.redis
            prop = Property("p", alwaysfetch=False)
            key = "xkey"

        x = X()
        key = X.prop.key(x)

        self.assertIs(x.prop, Property.null)

        self.redis.set(key, pickle.dumps("canary"))
        self.assertEquals(x.prop, "canary")

        self.redis.set(key, pickle.dumps("sparrow"))
        self.assertEquals(x.prop, "canary")

        X.prop.invalidate(x)
        self.assertEquals(x.prop, "sparrow")

    def test_delete(self):
        class X:
            redis = self.redis
            prop = Property("p")
            key = "xkey"

        x = X()
        key = X.prop.key(x)

        self.redis.set(key, "canary")
        self.assertTrue(X.prop.delete(x))
        self.assertIsNone(self.redis.get(key))
        self.assertFalse(X.prop.delete(x))

    def test_exists(self):
        class X:
            redis = self.redis
            prop = Property("p")
            key = "xkey"

        x = X()
        key = X.prop.key(x)

        self.assertFalse(X.prop.exists(x))
        self.redis.set(key, "canary")
        self.assertTrue(X.prop.exists(x))
