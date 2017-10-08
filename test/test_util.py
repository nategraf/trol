import unittest
import ddt
import docker
from .common import ensure_redis_is_online
from redis import Redis
from trol import Serializer, Deserializer, Model


@ddt.ddt
class OnlineUtilTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.container_token = ensure_redis_is_online()
        cls.redis = Redis(host='localhost', port=6379, db=0)

    def setUp(self):
        self.redis.flushall()

    @ddt.data(
        (str, "I am a test string!"),
        (int, 42),
        (float, 3.1415),
        (bytes, b'\xDE\xAD\xBE\xEF'),
        (bool, True),
        (bool, False)
    )
    @ddt.unpack
    def test_roundtrip(self, typ, data):
        key = "key"

        s = Serializer(typ)
        d = Deserializer(typ)

        self.redis.set(key, s(data))
        result = d(self.redis.get(key))

        self.assertEqual(data, result)


class A(Model):
    pass


class B(Model):
    def __init__(self):
        self.model_name = 'BAR'
        self.id = 'x'


class C(Model):
    def __init__(self):
        self.key = 'foobar'


@ddt.ddt
class OfflineUtilTests(unittest.TestCase):

    def test_serialize_model(self):
        s = Serializer(Model)
        d = Deserializer(Model)
        a = A()

        replicon = d(s(a))
        self.assertIsInstance(a, A)
        self.assertEquals(a.model_name, replicon.model_name)

        with self.assertRaises(AttributeError):
            a.id

        with self.assertRaises(AttributeError):
            a.key

        b = B()

        replicon = d(s(b))

        self.assertIsInstance(b, B)
        self.assertEquals(b.model_name, replicon.model_name)
        self.assertEquals(b.id, replicon.id)
        self.assertEquals(b.key, replicon.key)

        c = C()

        replicon = d(s(c))

        self.assertIsInstance(c, C)
        self.assertEquals(c.model_name, replicon.model_name)
        with self.assertRaises(AttributeError):
            a.id
        self.assertEquals(c.key, replicon.key)
