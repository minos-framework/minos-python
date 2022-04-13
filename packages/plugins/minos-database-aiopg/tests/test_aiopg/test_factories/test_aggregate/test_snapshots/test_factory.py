import unittest
from uuid import (
    uuid4,
)

from minos.aggregate import (
    Condition,
    Ordering,
    SnapshotDatabaseOperationFactory,
)
from minos.common import (
    ComposedDatabaseOperation,
    current_datetime,
)
from minos.plugins.aiopg import (
    AiopgDatabaseOperation,
    AiopgSnapshotDatabaseOperationFactory,
)


class TestAiopgSnapshotDatabaseOperationFactory(unittest.TestCase):
    def setUp(self) -> None:
        self.factory = AiopgSnapshotDatabaseOperationFactory()

    def test_is_subclass(self):
        self.assertTrue(issubclass(AiopgSnapshotDatabaseOperationFactory, SnapshotDatabaseOperationFactory))

    def test_build_table_name(self):
        self.assertEqual("snapshot", self.factory.build_table_name())

    def test_build_offset_table_name(self):
        self.assertEqual("snapshot_aux_offset", self.factory.build_offset_table_name())

    def test_build_create_table(self):
        operation = self.factory.build_create_table()
        self.assertIsInstance(operation, ComposedDatabaseOperation)
        self.assertEqual(3, len(operation.operations))
        for sub in operation.operations:
            self.assertIsInstance(sub, AiopgDatabaseOperation)

    def test_build_delete_by_transactions(self):
        operation = self.factory.build_delete_by_transactions({uuid4(), uuid4()})
        self.assertIsInstance(operation, AiopgDatabaseOperation)

    def test_build_insert(self):
        operation = self.factory.build_insert(
            uuid=uuid4(),
            name="Foo",
            version=34243,
            schema=bytes(),
            data={"foo": "bar"},
            created_at=current_datetime(),
            updated_at=current_datetime(),
            transaction_uuid=uuid4(),
        )
        self.assertIsInstance(operation, AiopgDatabaseOperation)

    def test_build_query(self):
        operation = self.factory.build_query(
            name="Foo",
            condition=Condition.EQUAL("foo", "bar"),
            ordering=Ordering.ASC("foobar"),
            limit=2342,
            transaction_uuids=[uuid4(), uuid4()],
            exclude_deleted=True,
        )
        self.assertIsInstance(operation, AiopgDatabaseOperation)

    def test_build_store_offset(self):
        operation = self.factory.build_store_offset(56)
        self.assertIsInstance(operation, AiopgDatabaseOperation)

    def test_build_get_offset(self):
        operation = self.factory.build_get_offset()
        self.assertIsInstance(operation, AiopgDatabaseOperation)


if __name__ == "__main__":
    unittest.main()
