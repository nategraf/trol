import unittest
from redis import Redis
from .common import ensure_redis_is_online
from trol import Model, ModelType, Property


class Alpha(Model):
    pass


class OfflineModelTests(unittest.TestCase):

    def test_key_retrieval(self):
        a = Alpha()

        with self.assertRaises(AttributeError):
            a.key

        a.id = "a"
        self.assertEquals(a.key, "Alpha:a")

        a.model_name = "FOO"
        self.assertEquals(a.key, "FOO:a")

    def test_redis_retrieval(self):
        a = Alpha()

        self.assertIsNone(a.redis)

        canary = object()
        a.redis = canary
        self.assertIs(a.redis, canary)


class X(Model):
    def __init__(self, id):
        self.id = id

    one = Property(autocommit=False, typ=bytes)
    two = Property('2', typ=str)
    three = Property(alwaysfetch=True, typ=int)
    four = Property(autocommit=False, typ=float)


class OnlineModelTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.container_token = ensure_redis_is_online()
        X.redis = Redis(host='localhost', port=6379, db=0)

    def setUp(self):
        X.redis.flushall()

    def test_invalidate(self):
        x = X("xyz")

        X.redis.set("X:xyz:one", b'\xDE\xAD\xBE\xEF')
        X.redis.set("X:xyz:2", "canary")
        X.redis.set("X:xyz:three", 42)
        X.redis.set("X:xyz:four", 3.14)

        self.assertEquals(x.one, b'\xDE\xAD\xBE\xEF')
        self.assertEquals(x.two,  "canary")
        self.assertEquals(x.three, 42)
        self.assertEquals(x.four, 3.14)

        X.redis.set("X:xyz:one", b'\x8B\xAD\xF0\x0D')
        X.redis.set("X:xyz:2", "swallow")
        X.redis.set("X:xyz:three", 1024)
        X.redis.set("X:xyz:four", 2.71828)

        self.assertEquals(x.one, b'\xDE\xAD\xBE\xEF')
        self.assertEquals(x.two,  "canary")
        self.assertEquals(x.three, 1024)
        self.assertEquals(x.four, 3.14)

        x.invalidate('two', 'one')

        self.assertEquals(x.one, b'\x8B\xAD\xF0\x0D')
        self.assertEquals(x.two, "swallow")
        self.assertEquals(x.three, 1024)
        self.assertEquals(x.four, 3.14)

        x.invalidate()

        self.assertEquals(x.one, b'\x8B\xAD\xF0\x0D')
        self.assertEquals(x.two, "swallow")
        self.assertEquals(x.three, 1024)
        self.assertEquals(x.four, 2.71828)

    def test_commit(self):
        x = X("xyz")

        x.one = b'\xDE\xAD\xBE\xEF'
        x.two = "canary"
        x.three = 42
        x.four = 3.14

        self.assertIsNone(X.redis.get("X:xyz:one"))
        self.assertEquals(X.redis.get("X:xyz:2").decode('utf-8'),  "canary")
        self.assertEquals(X.redis.get("X:xyz:three").decode('utf-8'), '42')
        self.assertIsNone(X.redis.get("X:xyz:four"))

        x.commit('one')

        self.assertEquals(X.redis.get("X:xyz:one"), b'\xDE\xAD\xBE\xEF')
        self.assertEquals(X.redis.get("X:xyz:2").decode('utf-8'),  "canary")
        self.assertEquals(X.redis.get("X:xyz:three").decode('utf-8'), '42')
        self.assertIsNone(X.redis.get("X:xyz:four"))

        x.commit()

        self.assertEquals(X.redis.get("X:xyz:one"), b'\xDE\xAD\xBE\xEF')
        self.assertEquals(X.redis.get("X:xyz:2").decode('utf-8'),  "canary")
        self.assertEquals(X.redis.get("X:xyz:three").decode('utf-8'), '42')
        self.assertEquals(X.redis.get("X:xyz:four").decode('utf-8'), '3.14')

    def test_delete(self):
        x = X("xyz")

        x.one = b'\xDE\xAD\xBE\xEF'
        x.two = "canary"
        x.three = 42
        x.four = 3.14
        x.commit()

        self.assertEquals(X.redis.get("X:xyz:one"), b'\xDE\xAD\xBE\xEF')
        self.assertEquals(X.redis.get("X:xyz:2").decode('utf-8'),  "canary")
        self.assertEquals(X.redis.get("X:xyz:three").decode('utf-8'), '42')
        self.assertEquals(X.redis.get("X:xyz:four").decode('utf-8'), '3.14')

        x.delete('two', 'three')

        self.assertEquals(X.redis.get("X:xyz:one"), b'\xDE\xAD\xBE\xEF')
        self.assertIsNone(X.redis.get("X:xyz:2"))
        self.assertIsNone(X.redis.get("X:xyz:three"))
        self.assertEquals(X.redis.get("X:xyz:four").decode('utf-8'), '3.14')

        x.delete()

        self.assertIsNone(X.redis.get("X:xyz:one"))
        self.assertIsNone(X.redis.get("X:xyz:2"))
        self.assertIsNone(X.redis.get("X:xyz:three"))
        self.assertIsNone(X.redis.get("X:xyz:four"))

    def test_update(self):
        x = X("xyz")

        x.update(
            one=b'\xDE\xAD\xBE\xEF',
            two="canary",
            three=42
        )

        self.assertEquals(X.one.value(x), b'\xDE\xAD\xBE\xEF')
        self.assertEquals(X.two.value(x),  "canary")
        self.assertEquals(X.three.value(x), 42)
        self.assertIs(X.four.value(x), X.four.null)

        self.assertIsNone(X.redis.get("X:xyz:one"))
        self.assertEquals(X.redis.get("X:xyz:2").decode('utf-8'),  "canary")
        self.assertEquals(X.redis.get("X:xyz:three").decode('utf-8'), '42')
        self.assertIsNone(X.redis.get("X:xyz:four"))

    def test_exists(self):
        x = X("xyz")

        self.assertFalse(x.exists())

        x.update(
            one=b'\xDE\xAD\xBE\xEF',
            two="canary",
            three=42
        )

        self.assertTrue(x.exists())
        self.assertFalse(x.exists('three', 'four'))
        self.assertTrue(x.exists('two', 'three'))
