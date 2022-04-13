import unittest
from uuid import (
    uuid4,
)

from minos.aggregate import (
    Action,
    EventDatabaseOperationFactory,
)
from minos.common import (
    ComposedDatabaseOperation,
    current_datetime,
)
from minos.plugins.aiopg import (
    AiopgDatabaseOperation,
    AiopgEventDatabaseOperationFactory,
)


class TestAiopgEventDatabaseOperationFactory(unittest.TestCase):
    def setUp(self) -> None:
        self.factory = AiopgEventDatabaseOperationFactory()

    def test_is_subclass(self):
        self.assertTrue(issubclass(AiopgEventDatabaseOperationFactory, EventDatabaseOperationFactory))

    def test_build_table_name(self):
        self.assertEqual("aggregate_event", self.factory.build_table_name())

    def test_build_create_table(self):
        operation = self.factory.build_create_table()
        self.assertIsInstance(operation, ComposedDatabaseOperation)
        self.assertEqual(3, len(operation.operations))
        for sub in operation.operations:
            self.assertIsInstance(sub, AiopgDatabaseOperation)

    def test_build_submit_row(self):
        operation = self.factory.build_submit_row(
            transaction_uuids=[uuid4(), uuid4()],
            uuid=uuid4(),
            action=Action.CREATE,
            name="Foo",
            version=3,
            data=bytes(),
            created_at=current_datetime(),
            transaction_uuid=uuid4(),
            lock="foo",
        )
        self.assertIsInstance(operation, AiopgDatabaseOperation)

    def test_build_select_rows(self):
        operation = self.factory.build_select_rows(
            uuid=uuid4(),
            name="Foo",
            version=423453,
            version_lt=234,
            version_gt=342,
            version_le=5433,
            version_ge=897,
            id=234,
            id_lt=34,
            id_gt=543,
            id_ge=123,
            transaction_uuid=uuid4(),
            transaction_uuid_ne=uuid4(),
            transaction_uuid_in=[uuid4(), uuid4(), uuid4()],
        )
        self.assertIsInstance(operation, AiopgDatabaseOperation)

    def test_build_select_max_id(self):
        operation = self.factory.build_select_max_id()
        self.assertIsInstance(operation, AiopgDatabaseOperation)


if __name__ == "__main__":
    unittest.main()
