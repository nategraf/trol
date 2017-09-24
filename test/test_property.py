import docker
import unittest
from redis import StrictRedis
from rtol import Property


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
        cls.dockerc = docker.from_env()
        cls.container = cls.dockerc.containers.run(
            "redis:latest", name='rtol-test-redis', network_mode='host', detach=True)
        cls.redis = StrictRedis(host='localhost', port=6379, db=0)

    @classmethod
    def tearDownClass(cls):
        cls.container.remove(force=True)

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
        self.assertEquals(self.redis.get(key).decode('utf-8'), "something")

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
        self.assertEquals(self.redis.get(key).decode('utf-8'), "something")

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
        self.assertEquals(self.redis.get(key).decode('utf-8'), "something")

    def test_getter_alwaysfetch(self):
        class X:
            redis = self.redis
            prop = Property("p", alwaysfetch=True)
            key = "xkey"

        x = X()
        key = X.prop.key(x)
        self.assertIsNone(x.prop)
        self.assertIsNone(self.redis.get(key))

        self.redis.set(key, "canary")
        self.assertEquals(x.prop.decode('utf-8'), "canary")

    def test_getter_no_alwaysfetch(self):
        class X:
            redis = self.redis
            prop = Property("p", alwaysfetch=False)
            key = "xkey"

        x = X()
        key = X.prop.key(x)
        self.assertIsNone(x.prop)
        self.assertIsNone(self.redis.get(key))

        self.redis.set(key, "canary")
        self.assertIsNone(x.prop)
        X.prop.invalidate(x)
        self.assertEquals(x.prop.decode('utf-8'), "canary")
