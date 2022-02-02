import unittest

from minos.aggregate import (
    AggregateException,
    AlreadyDeletedException,
    EventRepositoryException,
    NotFoundException,
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

    def test_snapshot_not_found(self):
        self.assertTrue(issubclass(NotFoundException, SnapshotRepositoryException))

    def test_snapshot_already_deleted(self):
        self.assertTrue(issubclass(AlreadyDeletedException, SnapshotRepositoryException))


if __name__ == "__main__":
    unittest.main()
