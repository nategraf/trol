import unittest
from trol import highlander


@highlander()
class Z:
    def __init__(self):
        pass


@highlander('alpha')
class A:
    def __init__(self, alpha):
        pass


@highlander('alpha', 'beta')
class B:
    def __init__(self, alpha, beta):
        pass


@highlander('alpha', 'beta')
class C:
    def __init__(self, alpha, beta, gamma=None):
        pass


class HighlanderTests(unittest.TestCase):

    def test_obtain_returns_instance(self):
        self.assertIsInstance(A.obtain("X"), A)
        self.assertIsInstance(B.obtain("X", "Y"), B)
        self.assertIsInstance(C.obtain("X", "Y"), C)
        self.assertIsInstance(Z.obtain(), Z)

    def test_obtain_returns_shared_instance(self):
        z1, z2 = Z.obtain(), Z.obtain()
        self.assertIs(z1, z2)

        a1, a2 = A.obtain(1), A.obtain(1)
        self.assertIs(a1, a2)

        b1, b2 = B.obtain(1, "X"), B.obtain(1, "X")
        self.assertIs(b1, b2)

        c1, c2 = C.obtain(1, "X"), C.obtain(1, "X")
        self.assertIs(c1, c2)

    def test_obtain_returns_unique_instance(self):
        a1, a2 = A.obtain(1), A.obtain(2)
        self.assertIsNot(a1, a2)

        b1, b2 = B.obtain(1, "X"), B.obtain(2, "X")
        self.assertIsNot(b1, b2)

        c1, c2 = C.obtain(1, "X"), C.obtain(1, "Y")
        self.assertIsNot(c1, c2)

    def test_obtain_raises_type_error(self):
        with self.assertRaises(TypeError):
            A.obtain("too", "many", "arguments")

        with self.assertRaises(TypeError):
            B.obtain("too_many_arguments")

    def test_obtain_calls_init(self):
        @highlander('alpha', 'beta')
        class X:
            def __init__(this, alpha, beta):
                this.alpha = alpha
                this.beta = beta

        keyA, keyB = "keyA", 5
        x1 = X.obtain(keyA, keyB)

        self.assertIs(x1.alpha, keyA)
        self.assertIs(x1.beta, keyB)

    def test_obtain_calls_init_once(self):
        @highlander()
        class X:
            def __init__(this):
                self.assertFalse(hasattr(this, 'mark'))
                this.mark = None

        x1 = X.obtain()
        x2 = X.obtain()
