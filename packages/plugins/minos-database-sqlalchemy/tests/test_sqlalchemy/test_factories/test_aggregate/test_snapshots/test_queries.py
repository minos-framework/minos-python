import unittest
from operator import (
    and_,
)
from unittest.mock import (
    MagicMock,
)
from uuid import (
    uuid4,
)

from sqlalchemy import (
    desc,
    literal,
    not_,
    or_,
    select,
    union_all,
)
from sqlalchemy.sql import (
    False_,
    True_,
)
from sqlalchemy.sql.operators import (
    contains_op,
    like_op,
)

from minos.aggregate import (
    IS_REPOSITORY_SERIALIZATION_CONTEXT_VAR,
    Condition,
    Ordering,
)
from minos.common import (
    NULL_UUID,
)
from minos.plugins.sqlalchemy import (
    SqlAlchemySnapshotQueryDatabaseOperationBuilder,
    SqlAlchemySnapshotTableFactory,
)
from tests.utils import (
    SqlAlchemyTestCase,
)


class TestSqlAlchemySnapshotQueryDatabaseOperationBuilder(SqlAlchemyTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.tables = SqlAlchemySnapshotTableFactory.build(*self.config.get_aggregate()["entities"]).tables
        self.name = "Car"

    def test_constructor(self):
        qb = SqlAlchemySnapshotQueryDatabaseOperationBuilder(self.tables, self.name)
        self.assertEqual(self.tables, qb.tables)
        self.assertEqual(self.name, qb.name)
        self.assertEqual(None, qb.condition)
        self.assertEqual(None, qb.ordering)
        self.assertEqual(None, qb.limit)
        self.assertFalse(qb.exclude_deleted)

    def test_constructor_full(self):
        transaction_uuids = [NULL_UUID, uuid4()]
        qb = SqlAlchemySnapshotQueryDatabaseOperationBuilder(
            self.tables, self.name, Condition.TRUE, Ordering.ASC("color"), 10, transaction_uuids, True
        )
        self.assertEqual(self.tables, qb.tables)
        self.assertEqual(self.name, qb.name)
        self.assertEqual(Condition.TRUE, qb.condition)
        self.assertEqual(Ordering.ASC("color"), qb.ordering)
        self.assertEqual(10, qb.limit)
        self.assertEqual(tuple(transaction_uuids), qb.transaction_uuids)
        self.assertTrue(qb.exclude_deleted)

    def test_build_submitting_context_var(self):
        builder = SqlAlchemySnapshotQueryDatabaseOperationBuilder(self.tables, self.name)

        def _fn():
            self.assertEqual(True, IS_REPOSITORY_SERIALIZATION_CONTEXT_VAR.get())

        mock = MagicMock(side_effect=_fn)
        builder._build = mock

        self.assertEqual(False, IS_REPOSITORY_SERIALIZATION_CONTEXT_VAR.get())
        builder.build()
        self.assertEqual(False, IS_REPOSITORY_SERIALIZATION_CONTEXT_VAR.get())

        self.assertEqual(1, mock.call_count)

    def test_build_raises(self):
        with self.assertRaises(ValueError):
            # noinspection PyTypeChecker
            SqlAlchemySnapshotQueryDatabaseOperationBuilder(self.tables, self.name, True).build()

    async def test_get_table(self):
        builder = SqlAlchemySnapshotQueryDatabaseOperationBuilder(self.tables, self.name)

        observed = builder.get_table()

        table = self.tables[self.name]
        union = union_all(
            select(table, literal(1).label("transaction_index")).filter(table.columns["transaction_uuid"] == NULL_UUID)
        ).subquery()
        expected = (
            select(union, literal(self.name).label("name"))
            .order_by(union.columns["uuid"], desc(union.columns["transaction_index"]))
            .distinct(union.columns["uuid"])
        ).subquery()

        self.assertTrue(expected.compare(observed))

    async def test_get_table_with_transactions(self):
        uuids = (NULL_UUID, uuid4())
        builder = SqlAlchemySnapshotQueryDatabaseOperationBuilder(self.tables, self.name, transaction_uuids=uuids)

        observed = builder.get_table()

        table = self.tables[self.name]
        union = union_all(
            select(table, literal(1).label("transaction_index")).filter(table.columns["transaction_uuid"] == uuids[0]),
            select(table, literal(2).label("transaction_index")).filter(table.columns["transaction_uuid"] == uuids[1]),
        ).subquery()
        expected = (
            select(union, literal(self.name).label("name"))
            .order_by(union.columns["uuid"], desc(union.columns["transaction_index"]))
            .distinct(union.columns["uuid"])
        ).subquery()

        self.assertTrue(expected.compare(observed))

    async def test_get_table_with_exclude_deleted(self):
        builder = SqlAlchemySnapshotQueryDatabaseOperationBuilder(self.tables, self.name, exclude_deleted=True)
        observed = builder.get_table()

        table = self.tables[self.name]

        union = union_all(
            select(table, literal(1).label("transaction_index")).filter(table.columns["transaction_uuid"] == NULL_UUID)
        ).subquery()

        sub = (
            select(union, literal(self.name).label("name"))
            .order_by(union.columns["uuid"], desc(union.columns["transaction_index"]))
            .distinct(union.columns["uuid"])
        ).subquery()
        expected = select(sub).filter(sub.columns["deleted"].is_(False)).subquery()

        self.assertTrue(expected.compare(observed))

    async def test_build_true(self):
        condition = Condition.TRUE
        builder = SqlAlchemySnapshotQueryDatabaseOperationBuilder(self.tables, self.name, condition)
        table = builder.get_table()

        observed = builder.build()
        expected = select(table).filter(True_())

        self.assertTrue(expected.compare(observed))

    async def test_build_false(self):
        condition = Condition.FALSE
        builder = SqlAlchemySnapshotQueryDatabaseOperationBuilder(self.tables, self.name, condition)
        table = builder.get_table()

        observed = builder.build()
        expected = select(table).filter(False_())

        self.assertTrue(expected.compare(observed))

    async def test_build_lower(self):
        condition = Condition.LOWER("doors", 1)
        builder = SqlAlchemySnapshotQueryDatabaseOperationBuilder(self.tables, self.name, condition)
        table = builder.get_table()

        observed = builder.build()
        expected = select(table).filter(table.columns["doors"] < 1)

        self.assertTrue(expected.compare(observed))

    async def test_build_lower_equal(self):
        condition = Condition.LOWER_EQUAL("doors", 1)
        builder = SqlAlchemySnapshotQueryDatabaseOperationBuilder(self.tables, self.name, condition)
        table = builder.get_table()

        observed = builder.build()
        expected = select(table).filter(table.columns["doors"] <= 1)

        self.assertTrue(expected.compare(observed))

    async def test_build_greater(self):
        condition = Condition.GREATER("doors", 1)
        builder = SqlAlchemySnapshotQueryDatabaseOperationBuilder(self.tables, self.name, condition)
        table = builder.get_table()

        observed = builder.build()
        expected = select(table).filter(table.columns["doors"] > 1)

        self.assertTrue(expected.compare(observed))

    async def test_build_greater_equal(self):
        condition = Condition.GREATER_EQUAL("doors", 1)
        builder = SqlAlchemySnapshotQueryDatabaseOperationBuilder(self.tables, self.name, condition)
        table = builder.get_table()

        observed = builder.build()
        expected = select(table).filter(table.columns["doors"] >= 1)

        self.assertTrue(expected.compare(observed))

    async def test_build_equal(self):
        condition = Condition.EQUAL("doors", 1)
        builder = SqlAlchemySnapshotQueryDatabaseOperationBuilder(self.tables, self.name, condition)
        table = builder.get_table()

        observed = builder.build()
        expected = select(table).filter(table.columns["doors"] == 1)

        self.assertTrue(expected.compare(observed))

    async def test_build_not_equal(self):
        condition = Condition.NOT_EQUAL("doors", 1)
        builder = SqlAlchemySnapshotQueryDatabaseOperationBuilder(self.tables, self.name, condition)
        table = builder.get_table()

        observed = builder.build()
        expected = select(table).filter(table.columns["doors"] != 1)

        self.assertTrue(expected.compare(observed))

    async def test_build_in(self):
        condition = Condition.IN("doors", [1, 2, 3])
        builder = SqlAlchemySnapshotQueryDatabaseOperationBuilder(self.tables, self.name, condition)
        table = builder.get_table()

        observed = builder.build()
        expected = select(table).filter(table.columns["doors"].in_([1, 2, 3]))

        self.assertTrue(expected.compare(observed))

    async def test_build_in_empty(self):
        condition = Condition.IN("doors", [])
        builder = SqlAlchemySnapshotQueryDatabaseOperationBuilder(self.tables, self.name, condition)
        table = builder.get_table()

        observed = builder.build()
        expected = select(table).filter(False_())

        self.assertTrue(expected.compare(observed))

    async def test_build_contains(self):
        condition = Condition.CONTAINS("color", "a")
        builder = SqlAlchemySnapshotQueryDatabaseOperationBuilder(self.tables, self.name, condition)
        table = builder.get_table()

        observed = builder.build()
        expected = select(table).filter(contains_op(table.columns["color"], "a"))

        self.assertTrue(expected.compare(observed))

    async def test_build_like(self):
        condition = Condition.LIKE("color", "a%")
        builder = SqlAlchemySnapshotQueryDatabaseOperationBuilder(self.tables, self.name, condition)
        table = builder.get_table()

        observed = builder.build()
        expected = select(table).filter(like_op(table.columns["color"], "a%"))

        self.assertTrue(expected.compare(observed))

    async def test_build_not(self):
        condition = Condition.NOT(Condition.LOWER("doors", 1))
        builder = SqlAlchemySnapshotQueryDatabaseOperationBuilder(self.tables, self.name, condition)
        table = builder.get_table()

        observed = builder.build()
        expected = select(table).filter(not_(table.columns["doors"] < 1))

        self.assertTrue(expected.compare(observed))

    async def test_build_and(self):
        condition = Condition.AND(Condition.GREATER("doors", 1), Condition.LOWER("doors", 3))
        builder = SqlAlchemySnapshotQueryDatabaseOperationBuilder(self.tables, self.name, condition)
        table = builder.get_table()

        observed = builder.build()
        expected = select(table).filter(and_(table.columns["doors"] > 1, table.columns["doors"] < 3))

        self.assertTrue(expected.compare(observed))

    async def test_build_or(self):
        condition = Condition.OR(Condition.LOWER("doors", 1), Condition.GREATER("doors", 3))
        builder = SqlAlchemySnapshotQueryDatabaseOperationBuilder(self.tables, self.name, condition)
        table = builder.get_table()

        observed = builder.build()
        expected = select(table).filter(or_(table.columns["doors"] < 1, table.columns["doors"] > 3))

        self.assertTrue(expected.compare(observed))

    async def test_build_ordering_asc(self):
        ordering = Ordering.ASC("color")
        builder = SqlAlchemySnapshotQueryDatabaseOperationBuilder(self.tables, self.name, ordering=ordering)
        table = builder.get_table()

        observed = builder.build()
        expected = select(table).order_by(table.columns["color"])

        self.assertTrue(expected.compare(observed))

    async def test_build_ordering_desc(self):
        ordering = Ordering.DESC("color")
        builder = SqlAlchemySnapshotQueryDatabaseOperationBuilder(self.tables, self.name, ordering=ordering)
        table = builder.get_table()

        observed = builder.build()
        expected = select(table).order_by(desc(table.columns["color"]))

        self.assertTrue(expected.compare(observed))

    async def test_build_limit(self):
        builder = SqlAlchemySnapshotQueryDatabaseOperationBuilder(self.tables, self.name, limit=10)
        table = builder.get_table()

        observed = builder.build()
        expected = select(table).limit(10)

        self.assertTrue(expected.compare(observed))

    async def test_build_complex(self):
        condition = Condition.AND(
            Condition.EQUAL("doors", 2),
            Condition.OR(Condition.EQUAL("color", "blue"), Condition.GREATER("version", 1)),
        )
        ordering = Ordering.DESC("updated_at")
        limit = 100

        builder = SqlAlchemySnapshotQueryDatabaseOperationBuilder(self.tables, self.name, condition, ordering, limit)
        table = builder.get_table()

        observed = builder.build()
        expected = (
            select(table)
            .filter(
                and_(table.columns["doors"] == 2, or_(table.columns["color"] == "blue", table.columns["version"] > 1))
            )
            .order_by(desc(table.columns["updated_at"]))
            .limit(100)
        )

        self.assertTrue(expected.compare(observed))


if __name__ == "__main__":
    unittest.main()
