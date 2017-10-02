import unittest
import ddt
import docker
from .common import ensure_redis_is_online
from redis import StrictRedis
from rtol import Serializer, Deserializer, Model
from rtol.util import serialize_model, deserialize_model, _break_key


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
    id = 'x'
    class B(Model):
        model_name = 'FOO'
        id = 'y'
        class C(Model):
            id = 'z'

class D(Model):
    id = 'w'
    model_name = 'BAR'

@ddt.ddt
class OfflineUtilTests(unittest.TestCase):
    
    @ddt.data(
        (A().B().C(), b'A\xfex\xfeFOO\xfey\xfeC\xfez'),
        (A.B.C(), b'A\xfcFOO\xfcC\xfez'),
        (A().B.C(), b'A\xfcFOO\xfcC\xfez'),
        (A.B(), b'A\xfcFOO\xfey'),
        (D(), b'BAR\xfew'),
    )
    @ddt.unpack
    def test_serialize(self, model, serial):
        self.assertEquals(serialize_model(model), serial)

    @ddt.data(
        (b'A\xfex\xfeFOO\xfey\xfeC\xfez', ('A', 'x', 'FOO', 'y', 'C', 'z')),
        (b'A\xfcFOO\xfcC\xfez', (('A', 'FOO', 'C'), 'z')),
        (b'A\xfcFOO\xfey', (('A', 'FOO'), 'y')),
        (b'BAR\xfew', ('BAR', 'w')),
    )
    @ddt.unpack
    def test__break_key(self, serial, key):
        self.assertEquals(_break_key(serial), key)
