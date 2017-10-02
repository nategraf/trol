import unittest
import ddt
import docker
from .common import ensure_redis_is_online
from redis import StrictRedis
from rtol import Serializer, Deserializer, Model
from rtol.util import serialize_model, deserialize_model


@ddt.ddt
class OnlineUtilTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.container_token = ensure_redis_is_online()
        cls.redis = StrictRedis(host='localhost', port=6379, db=0)

    def setUp(self):
        self.redis.flushall()

    @ddt.data(
        (str, "I am a test string!"),
        (int, 42),
        (float, 3.1415),
        (bytes, b'\xDE\xAD\xBE\xEF')
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
        a = A()

        replicon = deserialize_model(serialize_model(a))
        self.assertIsInstance(a, A)
        self.assertEquals(a.model_name, replicon.model_name)

        with self.assertRaises(AttributeError):
            a.id

        with self.assertRaises(AttributeError):
            a.key

        b = B()

        replicon = deserialize_model(serialize_model(b))

        self.assertIsInstance(b, B)
        self.assertEquals(b.model_name, replicon.model_name)
        self.assertEquals(b.id, replicon.id)
        self.assertEquals(b.key, replicon.key)

        c = C()

        replicon = deserialize_model(serialize_model(c))

        self.assertIsInstance(c, C)
        self.assertEquals(c.model_name, replicon.model_name)
        with self.assertRaises(AttributeError):
            a.id
        self.assertEquals(c.key, replicon.key)
