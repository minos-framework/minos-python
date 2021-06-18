"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import os
import unittest
from unittest import (
    mock,
)

from minos.common import (
    MinosConfig,
)
from tests.utils import (
    BASE_PATH,
)


class TestMinosConfigWithEnvironment(unittest.TestCase):
    def setUp(self) -> None:
        self.config_file_path = BASE_PATH / "test_config.yml"
        self.config = MinosConfig(path=self.config_file_path)

    @mock.patch.dict(os.environ, {"MINOS_REPOSITORY_DATABASE": "foo"})
    def test_overwrite_with_environment(self):
        repository = self.config.repository
        self.assertEqual("foo", repository.database)

    @mock.patch.dict(os.environ, {"MINOS_REPOSITORY_DATABASE": "foo"})
    def test_overwrite_with_environment_false(self):
        self.config._with_environment = False
        repository = self.config.repository
        self.assertEqual("order_db", repository.database)

    @mock.patch.dict(os.environ, {"MINOS_SAGA_BROKER": "TestHost"})
    @mock.patch.dict(os.environ, {"MINOS_SAGA_PORT": "2222"})
    def test_config_saga_broker(self):
        saga = self.config.saga

        broker = saga.broker
        self.assertEqual("TestHost", broker.host)
        self.assertEqual(2222, broker.port)

    @mock.patch.dict(os.environ, {"MINOS_DISCOVERY_HOST": "some-host"})
    @mock.patch.dict(os.environ, {"MINOS_DISCOVERY_PORT": "333"})
    @mock.patch.dict(os.environ, {"MINOS_DISCOVERY_ENDPOINTS_SUBSCRIBE_PATH": "subscribe-test"})
    @mock.patch.dict(os.environ, {"MINOS_DISCOVERY_ENDPOINTS_SUBSCRIBE_METHOD": "TEST-METHOD"})
    @mock.patch.dict(os.environ, {"MINOS_DISCOVERY_ENDPOINTS_UNSUBSCRIBE_PATH": "unsubscribe-test"})
    @mock.patch.dict(os.environ, {"MINOS_DISCOVERY_ENDPOINTS_UNSUBSCRIBE_METHOD": "TEST-METHOD"})
    @mock.patch.dict(os.environ, {"MINOS_DISCOVERY_ENDPOINTS_DISCOVER_PATH": "discover-test"})
    @mock.patch.dict(os.environ, {"MINOS_DISCOVERY_ENDPOINTS_DISCOVER_METHOD": "TEST-METHOD"})
    def test_config_discovery(self):
        discovery = self.config.discovery
        self.assertEqual("some-host", discovery.host)
        self.assertEqual("333", discovery.port)

        endpoints = discovery.endpoints
        subscribe = endpoints.subscribe
        self.assertEqual("subscribe-test", subscribe.path)
        self.assertEqual("TEST-METHOD", subscribe.method)

        unsubscribe = endpoints.unsubscribe
        self.assertEqual("unsubscribe-test", unsubscribe.path)
        self.assertEqual("TEST-METHOD", unsubscribe.method)

        discover = endpoints.discover
        self.assertEqual("discover-test", discover.path)
        self.assertEqual("TEST-METHOD", discover.method)
