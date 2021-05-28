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

    @mock.patch.dict(os.environ, {"MINOS_SAGA_HOST": "TestHost"})
    @mock.patch.dict(os.environ, {"MINOS_SAGA_PORT": "2222"})
    def test_config_saga_broker(self):
        saga = self.config.saga

        broker = saga.broker
        self.assertEqual("TestHost", broker.host)
        self.assertEqual(2222, broker.port)
