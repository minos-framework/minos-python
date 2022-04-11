import unittest

from minos.aggregate import (
    InMemorySnapshotRepository,
    SnapshotRepository,
)
from minos.aggregate.testing import (
    SnapshotRepositoryReaderTestCase,
)
from minos.common import (
    NotProvidedException,
)
from tests.utils import (
    AggregateTestCase,
)


class TestInMemorySnapshotRepositoryReader(AggregateTestCase, SnapshotRepositoryReaderTestCase):
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


if __name__ == "__main__":
    unittest.main()
