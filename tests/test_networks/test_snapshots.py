import unittest
from unittest.mock import (
    MagicMock,
)

from aiomisc.service.periodic import (
    PeriodicService,
)

from minos.aggregate import (
    InMemorySnapshotRepository,
)
from minos.networks import (
    SnapshotService,
)
from tests.utils import (
    MinosTestCase,
)


class TestSnapshotService(MinosTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.snapshot = InMemorySnapshotRepository()

    def test_is_instance(self):
        service = SnapshotService(self.snapshot, interval=0.1)
        self.assertIsInstance(service, PeriodicService)

    def test_dispatcher_config(self):
        snapshot = InMemorySnapshotRepository()
        service = SnapshotService(snapshot, interval=0.1)
        self.assertEqual(snapshot, service.snapshot)
        self.assertFalse(snapshot.already_setup)

    async def test_start(self):
        service = SnapshotService(self.snapshot, interval=0.1, loop=None)
        service.snapshot.setup = MagicMock(side_effect=service.snapshot.setup)
        await service.start()
        self.assertTrue(1, service.snapshot.setup.call_count)
        await service.stop()

    async def test_callback(self):
        service = SnapshotService(self.snapshot, interval=0.1)
        await service.snapshot.setup()
        mock = MagicMock(side_effect=service.snapshot.synchronize)
        service.snapshot.synchronize = mock
        await service.callback()
        self.assertEqual(1, mock.call_count)
        await service.snapshot.destroy()


if __name__ == "__main__":
    unittest.main()
