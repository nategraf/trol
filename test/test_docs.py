from .common import redis_test_client
import doctest
import trol
import unittest

def setUp(dtest):
    redis_test_client().flushall()

def load_tests(loader, tests, ignore):
    tests.addTests(doctest.DocFileSuite(
        'model.py', 'collection.py', 'util.py', '../README.rst', 'lock.py', package=trol, setUp=setUp))
    return tests
