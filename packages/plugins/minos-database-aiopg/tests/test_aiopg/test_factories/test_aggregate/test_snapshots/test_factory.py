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

    def test_build_create(self):
        operation = self.factory.build_create()
        self.assertIsInstance(operation, ComposedDatabaseOperation)
        self.assertEqual(3, len(operation.operations))
        for sub in operation.operations:
            self.assertIsInstance(sub, AiopgDatabaseOperation)

    def test_build_build_delete(self):
        operation = self.factory.build_delete({uuid4(), uuid4()})
        self.assertIsInstance(operation, AiopgDatabaseOperation)

    def test_build_submit(self):
        operation = self.factory.build_submit(
            uuid=uuid4(),
            type_="Foo",
            version=34243,
            schema={"type": "foo"},
            data={"foo": "bar"},
            created_at=current_datetime(),
            updated_at=current_datetime(),
            transaction_uuid=uuid4(),
        )
        self.assertIsInstance(operation, AiopgDatabaseOperation)

    def test_build_query(self):
        operation = self.factory.build_query(
            type_="Foo",
            condition=Condition.EQUAL("foo", "bar"),
            ordering=Ordering.ASC("foobar"),
            limit=2342,
            transaction_uuids=[uuid4(), uuid4()],
            exclude_deleted=True,
        )
        self.assertIsInstance(operation, AiopgDatabaseOperation)

    def test_build_submit_offset(self):
        operation = self.factory.build_submit_offset(56)
        self.assertIsInstance(operation, AiopgDatabaseOperation)

    def test_build_query_offset(self):
        operation = self.factory.build_query_offset()
        self.assertIsInstance(operation, AiopgDatabaseOperation)


if __name__ == "__main__":
    unittest.main()
