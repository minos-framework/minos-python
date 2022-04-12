import unittest

from minos.aggregate import (
    InMemorySnapshotRepository,
    SnapshotRepository,
)
from minos.aggregate.testing import (
    SnapshotRepositoryTestCase,
)
from minos.common import (
    NotProvidedException,
)
from tests.utils import (
    AggregateTestCase,
)


class TestInMemorySnapshotRepository(AggregateTestCase, SnapshotRepositoryTestCase):
    __test__ = True

    def build_snapshot_repository(self) -> SnapshotRepository:
        return InMemorySnapshotRepository()

    def test_constructor_raises(self):
        with self.assertRaises(NotProvidedException):
            # noinspection PyTypeChecker
            InMemorySnapshotRepository(event_repository=None)

        with self.assertRaises(NotProvidedException):
            # noinspection PyTypeChecker
            InMemorySnapshotRepository(transaction_repository=None)

    async def test_dispatch_with_offset(self):
        pass


if __name__ == "__main__":
    unittest.main()
