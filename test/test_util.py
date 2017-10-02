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

class D(Model):
    id = 'w'
    model_name = 'BAR'

@ddt.ddt
class OfflineUtilTests(unittest.TestCase):
    pass
