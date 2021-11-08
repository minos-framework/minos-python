import unittest

from minos.aggregate import (
    AggregateException,
    AggregateNotFoundException,
    DeletedAggregateException,
    EventRepositoryException,
    SnapshotRepositoryException,
)
from minos.common import (
    MinosException,
)


class TestExceptions(unittest.TestCase):
    def test_base(self):
        self.assertTrue(issubclass(AggregateException, MinosException))

    def test_repository(self):
        self.assertTrue(issubclass(EventRepositoryException, AggregateException))

    def test_snapshot(self):
        self.assertTrue(issubclass(SnapshotRepositoryException, AggregateException))

    def test_snapshot_aggregate_not_found(self):
        self.assertTrue(issubclass(AggregateNotFoundException, SnapshotRepositoryException))

    def test_snapshot_deleted_aggregate(self):
        self.assertTrue(issubclass(DeletedAggregateException, SnapshotRepositoryException))


if __name__ == "__main__":
    unittest.main()
