from .common import redis_test_client
from trol import Property, nil
import pickle
import time
import unittest


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
        self.assertIs(X.prop.value(x), nil)
        canary = object()
        X.prop.set(x, canary)
        self.assertIs(X.prop.value(x), canary)


class OnlinePropertyTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.redis = redis_test_client()

    def setUp(self):
        self.redis.flushall()

    def test_self(self):
        """Make sure the redis server can be booted and be connected to

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

    def test_redis_returns_nil(self):
        class X:
            redis = self.redis
            prop = Property("p", alwaysfetch=True)
            key = "xkey"

        x = X()
        key = X.prop.key(x)
        self.assertIs(x.prop, nil)

    def test_getter_alwaysfetch(self):
        class X:
            redis = self.redis
            prop = Property("p", alwaysfetch=True)
            key = "xkey"

        x = X()
        key = X.prop.key(x)

        self.assertIs(x.prop, nil)

        self.redis.set(key, pickle.dumps("canary"))
        self.assertEquals(x.prop, "canary")

    def test_getter_no_alwaysfetch(self):
        class X:
            redis = self.redis
            prop = Property("p", alwaysfetch=False)
            key = "xkey"

        x = X()
        key = X.prop.key(x)

        self.assertIs(x.prop, nil)

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

        self.redis.set(key, pickle.dumps("canary"))
        self.assertEquals(x.prop, "canary")
        self.assertTrue(X.prop.delete(x))
        self.assertIsNone(self.redis.get(key))
        self.assertIs(x.prop, nil)
        self.assertFalse(X.prop.delete(x))

    def test_expire_nonzero(self):
        class X:
            redis = self.redis
            prop = Property("p")
            key = "xkey"

        x = X()
        key = X.prop.key(x)

        self.redis.set(key, pickle.dumps("canary"))
        self.assertEquals(x.prop, "canary")
        self.assertLess(self.redis.ttl(key), 0)
        self.assertTrue(X.prop.expire(x, 1.5))
        self.assertGreater(self.redis.ttl(key), 0)
        time.sleep(1.5)
        self.assertIsNone(self.redis.get(key))
        self.assertIs(X.prop.fetch(x), nil)

    def test_expire_zero(self):
        class X:
            redis = self.redis
            prop = Property("p")
            key = "xkey"

        x = X()
        key = X.prop.key(x)

        self.redis.set(key, pickle.dumps("canary"))
        self.assertEquals(x.prop, "canary")
        self.assertLess(self.redis.ttl(key), 0)
        self.assertTrue(X.prop.expire(x, 0))
        self.assertLess(self.redis.ttl(key), 0)
        self.assertIs(x.prop, nil)

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
