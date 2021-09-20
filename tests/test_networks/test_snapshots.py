import unittest
from unittest.mock import (
    MagicMock,
)

from aiomisc.service.periodic import (
    PeriodicService,
)

from minos.common.testing import (
    PostgresAsyncTestCase,
)
from minos.networks import (
    SnapshotService,
)
from tests.utils import (
    BASE_PATH,
    FakeSnapshot,
)


class TestSnapshotService(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def setUp(self) -> None:
        super().setUp()
        self.snapshot = FakeSnapshot()

    def test_is_instance(self):
        service = SnapshotService(self.snapshot, interval=0.1, config=self.config)
        self.assertIsInstance(service, PeriodicService)

    def test_dispatcher_config(self):
        service = SnapshotService(self.snapshot, interval=0.1, config=self.config)
        snapshot = service.snapshot
        self.assertEqual(snapshot, self.snapshot)
        self.assertFalse(snapshot.already_setup)

    async def test_start(self):
        service = SnapshotService(self.snapshot, interval=0.1, loop=None, config=self.config)
        service.snapshot.setup = MagicMock(side_effect=service.snapshot.setup)
        await service.start()
        self.assertTrue(1, service.snapshot.setup.call_count)
        await service.stop()

    async def test_callback(self):
        service = SnapshotService(self.snapshot, interval=0.1, config=self.config)
        await service.snapshot.setup()
        mock = MagicMock(side_effect=service.snapshot.synchronize)
        service.snapshot.synchronize = mock
        await service.callback()
        self.assertEqual(1, mock.call_count)
        await service.snapshot.destroy()


if __name__ == "__main__":
    unittest.main()
