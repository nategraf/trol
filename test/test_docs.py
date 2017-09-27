import doctest
import unittest
import rtol
import docker
from .common import ensure_redis_is_online

container_token = None


def setUp(dtest):
    global container_token

    container_token = ensure_redis_is_online()


def load_tests(loader, tests, ignore):
    tests.addTests(doctest.DocFileSuite(
        'model.py', package=rtol, setUp=setUp))
    return tests
