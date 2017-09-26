import doctest
import unittest
import rtol
import docker

container = None


def setUp(dtest):
    global container

    dockerc = docker.from_env()
    container = dockerc.containers.run(
        "redis:latest", name='rtol-test-redis', network_mode='host', detach=True)


def tearDown(dtest):
    container.remove(force=True)


def load_tests(loader, tests, ignore):
    tests.addTests(doctest.DocFileSuite(
        'model.py', package=rtol, setUp=setUp, tearDown=tearDown))
    return tests
