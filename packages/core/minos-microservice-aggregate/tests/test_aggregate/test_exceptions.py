import unittest

from minos.aggregate import (
    AggregateException,
    AlreadyDeletedException,
    Delta,
    DeltaRepositoryConflictException,
    DeltaRepositoryException,
    NotFoundException,
    SnapshotRepositoryConflictException,
    SnapshotRepositoryException,
)
from minos.common import (
    MinosException,
)
from tests.utils import (
    AggregateTestCase,
    Car,
)


class TestExceptions(AggregateTestCase):
    def test_base(self):
        self.assertTrue(issubclass(AggregateException, MinosException))

    def test_delta(self):
        self.assertTrue(issubclass(DeltaRepositoryException, AggregateException))

    def test_delta_conflict(self):
        message = "There was a conflict"
        offset = 56
        exception = DeltaRepositoryConflictException(message, offset)

        self.assertIsInstance(exception, DeltaRepositoryException)
        self.assertEqual(message, str(exception))
        self.assertEqual(offset, exception.offset)

    def test_snapshot(self):
        self.assertTrue(issubclass(SnapshotRepositoryException, AggregateException))

    def test_snapshot_conflict(self):
        entity = Car(3, "red")
        delta = Delta.from_entity(entity)
        exception = SnapshotRepositoryConflictException(entity, delta)

        self.assertIsInstance(exception, SnapshotRepositoryException)
        self.assertEqual(entity, exception.previous)
        self.assertEqual(delta, exception.delta)

    def test_snapshot_not_found(self):
        self.assertTrue(issubclass(NotFoundException, SnapshotRepositoryException))

    def test_snapshot_already_deleted(self):
        self.assertTrue(issubclass(AlreadyDeletedException, SnapshotRepositoryException))


if __name__ == "__main__":
    unittest.main()
