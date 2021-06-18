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


class MyTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.config_file_path = BASE_PATH / "test_config.yml"

    def test_overwrite_with_parameter(self):
        config = MinosConfig(path=self.config_file_path, repository_database="foo")
        repository = config.repository
        self.assertEqual("foo", repository.database)

    @mock.patch.dict(os.environ, {"MINOS_REPOSITORY_DATABASE": "foo"})
    def test_overwrite_with_parameter_priority(self):
        config = MinosConfig(path=self.config_file_path, repository_database="bar")
        repository = config.repository
        self.assertEqual("bar", repository.database)

    def test_config_saga_broker(self):
        config = MinosConfig(path=self.config_file_path, saga_broker="bar", saga_port=333)
        saga = config.saga

        broker = saga.broker
        self.assertEqual("bar", broker.host)
        self.assertEqual(333, broker.port)

    def test_config_discovery(self):
        config = MinosConfig(
            path=self.config_file_path,
            minos_discovery_host="some-host",
            minos_discovery_port=333,
            minos_discovery_endpoints_subscribe_path="subscribe-test",
            minos_discovery_endpoints_subscribe_method="TEST-METHOD",
            minos_discovery_endpoints_unsubscribe_path="unsubscribe-test",
            minos_discovery_endpoints_unsubscribe_method="TEST-METHOD",
            minos_discovery_endpoints_discover_path="discover-test",
            minos_discovery_endpoints_discover_method="TEST-METHOD",
        )
        discovery = config.discovery
        self.assertEqual("some-host", discovery.host)
        self.assertEqual(333, discovery.port)

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
