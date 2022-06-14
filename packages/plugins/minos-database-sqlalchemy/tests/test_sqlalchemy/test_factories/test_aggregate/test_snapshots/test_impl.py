import unittest
from unittest.mock import (
    MagicMock,
)
from uuid import (
    uuid4,
)

from sqlalchemy.exc import (
    IntegrityError,
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
from minos.plugins.sqlalchemy import (
    SqlAlchemyDatabaseOperation,
    SqlAlchemySnapshotDatabaseOperationFactory,
)
from tests.utils import (
    SqlAlchemyTestCase,
)


class TestSqlAlchemySnapshotDatabaseOperationFactory(SqlAlchemyTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.factory = SqlAlchemySnapshotDatabaseOperationFactory.from_config(self.config)

    def test_is_subclass(self):
        self.assertTrue(issubclass(SqlAlchemySnapshotDatabaseOperationFactory, SnapshotDatabaseOperationFactory))

    def test_build_offset_table_name(self):
        self.assertEqual("aggregate_snapshot_aux_offset", self.factory.build_offset_table_name())

    def test_build_create(self):
        operation = self.factory.build_create()
        self.assertIsInstance(operation, ComposedDatabaseOperation)
        self.assertEqual(2, len(operation.operations))
        for sub in operation.operations:
            self.assertIsInstance(sub, SqlAlchemyDatabaseOperation)

    def test_build_build_delete(self):
        operation = self.factory.build_delete({uuid4(), uuid4()})
        self.assertIsInstance(operation, ComposedDatabaseOperation)
        self.assertEqual(4, len(operation.operations))
        for sub in operation.operations:
            self.assertIsInstance(sub, SqlAlchemyDatabaseOperation)

    def test_build_submit(self):
        operation = self.factory.build_submit(
            uuid=uuid4(),
            name="Car",
            version=34243,
            schema={"type": "foo"},
            data={"owner": "bar"},
            created_at=current_datetime(),
            updated_at=current_datetime(),
            transaction_uuid=uuid4(),
        )
        self.assertIsInstance(operation, SqlAlchemyDatabaseOperation)

        mock = MagicMock()
        mock.execute.side_effect = (IntegrityError("", dict(), None), None)
        operation.statement(mock)
        self.assertEqual(2, mock.execute.call_count)

    def test_build_query(self):
        operation = self.factory.build_query(
            name="Car",
            condition=Condition.EQUAL("owner", "bar"),
            ordering=Ordering.ASC("doors"),
            limit=2342,
            transaction_uuids=(
                uuid4(),
                uuid4(),
            ),
            exclude_deleted=True,
        )
        self.assertIsInstance(operation, SqlAlchemyDatabaseOperation)

    def test_build_submit_offset(self):
        operation = self.factory.build_submit_offset(56)
        self.assertIsInstance(operation, SqlAlchemyDatabaseOperation)

        mock = MagicMock()
        mock.execute.side_effect = (IntegrityError("", dict(), None), None)
        operation.statement(mock)
        self.assertEqual(2, mock.execute.call_count)

    def test_build_query_offset(self):
        operation = self.factory.build_query_offset()
        self.assertIsInstance(operation, SqlAlchemyDatabaseOperation)


if __name__ == "__main__":
    unittest.main()
