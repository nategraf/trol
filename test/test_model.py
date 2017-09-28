import unittest
import doctest
from redis import StrictRedis
from .common import ensure_redis_is_online
from rtol import Model, ModelType, Property, StrProperty, IntProperty, FloatProperty, BytesProperty


class Alpha(Model):
    class Bravo(Model):
        class Charlie(Model):
            pass

    class Delta(Model):
        pass


class OfflineModelTests(unittest.TestCase):

    def test_key_retrieval(self):
        a = Alpha()
        b = a.Bravo()
        c = b.Charlie()

        with self.assertRaises(AttributeError):
            c.key

        c.id = "c"

        with self.assertRaises(AttributeError):
            c.key

        b.id = "b"

        with self.assertRaises(AttributeError):
            c.key

        a.id = "a"

        self.assertEquals(c.key, "Alpha:a:Bravo:b:Charlie:c")
        self.assertEquals(b.key, "Alpha:a:Bravo:b")
        self.assertEquals(a.key, "Alpha:a")

    def test_redis_retrieval(self):
        a = Alpha()
        b = a.Bravo()
        c = b.Charlie()
        self.assertIsNone(c.redis)

        canary = object()
        a.redis = canary
        self.assertIs(c.redis, canary)

        swallow = object()
        b.redis = swallow
        self.assertIs(c.redis, swallow)


class X(Model):
    def __init__(self, id):
        self.id = id

    one = BytesProperty(autocommit=False)
    two = StrProperty('2')
    three = IntProperty(alwaysfetch=True)
    four = FloatProperty(autocommit=False)


class OnlineModelTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.container_token = ensure_redis_is_online()
        X.redis = StrictRedis(host='localhost', port=6379, db=0)

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
