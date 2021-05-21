"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""

import unittest
from unittest.mock import (
    MagicMock,
)

from aiomisc.service.periodic import (
    PeriodicService,
)

from minos.common import (
    MinosConfigException,
)
from minos.common.testing import (
    PostgresAsyncTestCase,
)
from minos.networks import (
    SnapshotDispatcher,
    SnapshotService,
)
from tests.utils import (
    BASE_PATH,
)


class TestMinosSnapshotService(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def test_is_instance(self):
        service = SnapshotService(interval=0.1, config=self.config)
        self.assertIsInstance(service, PeriodicService)

    def test_dispatcher_config_raises(self):
        with self.assertRaises(MinosConfigException):
            SnapshotService(interval=0.1)

    def test_dispatcher_config(self):
        service = SnapshotService(interval=0.1, config=self.config)
        dispatcher = service.dispatcher
        self.assertIsInstance(dispatcher, SnapshotDispatcher)
        self.assertFalse(dispatcher.already_setup)

    async def test_start(self):
        service = SnapshotService(interval=0.1, loop=None, config=self.config)
        service.dispatcher.setup = MagicMock(side_effect=service.dispatcher.setup)
        await service.start()
        self.assertTrue(1, service.dispatcher.setup.call_count)
        await service.stop()

    async def test_callback(self):
        service = SnapshotService(interval=0.1, loop=None, config=self.config)
        await service.dispatcher.setup()
        service.dispatcher.dispatch = MagicMock(side_effect=service.dispatcher.dispatch)
        await service.callback()
        self.assertEqual(1, service.dispatcher.dispatch.call_count)
        await service.dispatcher.destroy()


if __name__ == "__main__":
    unittest.main()
