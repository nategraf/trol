import unittest
from trol import Model, Property, Set, Database


class TestDatabase(unittest.TestCase):
    def test_init(self):
        canary = object()

        class X(Database):
            redis = canary

            prop = Property()

            collect = Set()

            class Mod(Model):
                pass

        self.assertIs(X.prop, X._rtol_properties['prop'])
        self.assertEquals(X.prop.name, 'prop')

        self.assertIs(X.collect, X._rtol_collections['collect'])
        self.assertEquals(X.collect.name, 'collect')

        self.assertIs(X.Mod, X._rtol_models['Mod'])
        mod = X.Mod()
        self.assertIs(mod.redis, canary)
