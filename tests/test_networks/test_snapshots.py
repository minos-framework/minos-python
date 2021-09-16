import unittest
from unittest.mock import (
    MagicMock,
)

from aiomisc.service.periodic import (
    PeriodicService,
)

from minos.common import (
    MinosConfigException,
    PostgreSqlSnapshotBuilder,
)
from minos.common.testing import (
    PostgresAsyncTestCase,
)
from minos.networks import (
    SnapshotService,
)
from tests.utils import (
    BASE_PATH,
    FakeRepository,
)


class TestSnapshotService(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def setUp(self) -> None:
        super().setUp()
        self.repository = FakeRepository()

    def test_is_instance(self):
        service = SnapshotService(interval=0.1, loop=None, config=self.config, repository=self.repository)
        self.assertIsInstance(service, PeriodicService)

    def test_dispatcher_config_raises(self):
        service = SnapshotService(interval=0.1)
        with self.assertRaises(MinosConfigException):
            # noinspection PyStatementEffect
            service.dispatcher

    def test_dispatcher_config(self):
        service = SnapshotService(interval=0.1, loop=None, config=self.config, repository=self.repository)
        dispatcher = service.dispatcher
        self.assertIsInstance(dispatcher, PostgreSqlSnapshotBuilder)
        self.assertFalse(dispatcher.already_setup)

    async def test_start(self):
        service = SnapshotService(interval=0.1, loop=None, config=self.config, repository=self.repository)
        service.dispatcher.setup = MagicMock(side_effect=service.dispatcher.setup)
        await service.start()
        self.assertTrue(1, service.dispatcher.setup.call_count)
        await service.stop()

    async def test_callback(self):
        service = SnapshotService(interval=0.1, loop=None, config=self.config, repository=self.repository)
        await service.dispatcher.setup()
        service.dispatcher.dispatch = MagicMock(side_effect=service.dispatcher.dispatch)
        await service.callback()
        self.assertEqual(1, service.dispatcher.dispatch.call_count)
        await service.dispatcher.destroy()


if __name__ == "__main__":
    unittest.main()
