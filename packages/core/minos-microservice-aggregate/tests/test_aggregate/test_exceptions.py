import unittest

from minos.aggregate import (
    AggregateException,
    AlreadyDeletedException,
    Event,
    EventRepositoryConflictException,
    EventRepositoryException,
    NotFoundException,
    SnapshotRepositoryConflictException,
    SnapshotRepositoryException,
    TransactionRepositoryConflictException,
    TransactionRepositoryException,
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

    def test_event(self):
        self.assertTrue(issubclass(EventRepositoryException, AggregateException))

    def test_event_conflict(self):
        message = "There was a conflict"
        offset = 56
        exception = EventRepositoryConflictException(message, offset)

        self.assertIsInstance(exception, EventRepositoryException)
        self.assertEqual(message, str(exception))
        self.assertEqual(offset, exception.offset)

    def test_snapshot(self):
        self.assertTrue(issubclass(SnapshotRepositoryException, AggregateException))

    def test_snapshot_conflict(self):
        entity = Car(3, "red")
        event = Event.from_root_entity(entity)
        exception = SnapshotRepositoryConflictException(entity, event)

        self.assertIsInstance(exception, SnapshotRepositoryException)
        self.assertEqual(entity, exception.previous)
        self.assertEqual(event, exception.event)

    def test_snapshot_not_found(self):
        self.assertTrue(issubclass(NotFoundException, SnapshotRepositoryException))

    def test_snapshot_already_deleted(self):
        self.assertTrue(issubclass(AlreadyDeletedException, SnapshotRepositoryException))

    def test_transaction(self):
        self.assertTrue(issubclass(TransactionRepositoryException, AggregateException))

    def test_transaction_conflict(self):
        message = "There was a conflict"
        exception = TransactionRepositoryConflictException(message)

        self.assertIsInstance(exception, TransactionRepositoryException)
        self.assertEqual(message, str(exception))


if __name__ == "__main__":
    unittest.main()
