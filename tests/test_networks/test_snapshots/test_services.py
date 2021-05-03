"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""

import unittest
from unittest.mock import MagicMock

from aiomisc.service.periodic import PeriodicService
from minos.common import MinosConfigException
from minos.common.testing import PostgresAsyncTestCase
from minos.networks import (
    MinosSnapshotDispatcher,
    MinosSnapshotService,
)
from tests.utils import BASE_PATH


class TestMinosSnapshotService(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def test_is_instance(self):
        with self.config:
            service = MinosSnapshotService(interval=10)
            self.assertIsInstance(service, PeriodicService)

    def test_dispatcher_config_raises(self):
        with self.assertRaises(MinosConfigException):
            MinosSnapshotService(interval=10)

    def test_dispatcher_config(self):
        service = MinosSnapshotService(interval=10, config=self.config)
        dispatcher = service.dispatcher
        self.assertIsInstance(dispatcher, MinosSnapshotDispatcher)
        self.assertFalse(dispatcher.already_setup)

    def test_dispatcher_config_context(self):
        with self.config:
            service = MinosSnapshotService(interval=10)
            self.assertIsInstance(service.dispatcher, MinosSnapshotDispatcher)

    async def test_start(self):
        service = MinosSnapshotService(interval=1, loop=None, config=self.config)
        service.dispatcher.setup = MagicMock(side_effect=service.dispatcher.setup)
        await service.start()
        self.assertTrue(1, service.dispatcher.setup.call_count)
        await service.stop()

    async def test_callback(self):
        service = MinosSnapshotService(interval=1, loop=None, config=self.config)
        service.dispatcher.dispatch = MagicMock(side_effect=service.dispatcher.dispatch)
        await service.start()
        self.assertEqual(1, service.dispatcher.dispatch.call_count)
        await service.stop()


if __name__ == "__main__":
    unittest.main()
