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
        self.type_ = "Car"

    def test_constructor(self):
        qb = SqlAlchemySnapshotQueryDatabaseOperationBuilder(self.tables, self.type_)
        self.assertEqual(self.tables, qb.tables)
        self.assertEqual(self.type_, qb.type_)
        self.assertEqual(None, qb.condition)
        self.assertEqual(None, qb.ordering)
        self.assertEqual(None, qb.limit)
        self.assertFalse(qb.exclude_deleted)

    def test_constructor_full(self):
        transaction_uuids = [NULL_UUID, uuid4()]
        qb = SqlAlchemySnapshotQueryDatabaseOperationBuilder(
            self.tables, self.type_, Condition.TRUE, Ordering.ASC("color"), 10, transaction_uuids, True
        )
        self.assertEqual(self.tables, qb.tables)
        self.assertEqual(self.type_, qb.type_)
        self.assertEqual(Condition.TRUE, qb.condition)
        self.assertEqual(Ordering.ASC("color"), qb.ordering)
        self.assertEqual(10, qb.limit)
        self.assertEqual(tuple(transaction_uuids), qb.transaction_uuids)
        self.assertTrue(qb.exclude_deleted)

    def test_build_submitting_context_var(self):
        builder = SqlAlchemySnapshotQueryDatabaseOperationBuilder(self.tables, self.type_)

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
            SqlAlchemySnapshotQueryDatabaseOperationBuilder(self.tables, self.type_, True).build()

    async def test_get_table(self):
        builder = SqlAlchemySnapshotQueryDatabaseOperationBuilder(self.tables, self.type_)

        observed = builder.get_table()

        table = self.tables[self.type_]
        union = union_all(
            select(table, literal(1).label("_index")).filter(table.columns["_transaction_uuid"] == NULL_UUID)
        ).subquery()
        expected = (
            select(union).order_by(union.columns["uuid"], desc(union.columns["_index"])).distinct(union.columns["uuid"])
        ).subquery()

        self.assertTrue(expected.compare(observed))

    async def test_get_table_with_transactions(self):
        uuids = (NULL_UUID, uuid4())
        builder = SqlAlchemySnapshotQueryDatabaseOperationBuilder(self.tables, self.type_, transaction_uuids=uuids)

        observed = builder.get_table()

        table = self.tables[self.type_]
        union = union_all(
            select(table, literal(1).label("_index")).filter(table.columns["_transaction_uuid"] == uuids[0]),
            select(table, literal(2).label("_index")).filter(table.columns["_transaction_uuid"] == uuids[1]),
        ).subquery()
        expected = (
            select(union).order_by(union.columns["uuid"], desc(union.columns["_index"])).distinct(union.columns["uuid"])
        ).subquery()

        self.assertTrue(expected.compare(observed))

    async def test_get_table_with_exclude_deleted(self):
        builder = SqlAlchemySnapshotQueryDatabaseOperationBuilder(self.tables, self.type_, exclude_deleted=True)
        observed = builder.get_table()

        table = self.tables[self.type_]

        union = union_all(
            select(table, literal(1).label("_index")).filter(table.columns["_transaction_uuid"] == NULL_UUID)
        ).subquery()

        sub = (
            select(union).order_by(union.columns["uuid"], desc(union.columns["_index"])).distinct(union.columns["uuid"])
        ).subquery()
        expected = select(sub).filter(sub.columns["_deleted"].is_(False)).subquery()

        self.assertTrue(expected.compare(observed))

    async def test_build_true(self):
        condition = Condition.TRUE
        builder = SqlAlchemySnapshotQueryDatabaseOperationBuilder(self.tables, self.type_, condition)
        table = builder.get_table()

        observed = builder.build()
        expected = select(table, literal(self.type_).label("_type")).filter(True_())

        self.assertTrue(expected.compare(observed))

    async def test_build_false(self):
        condition = Condition.FALSE
        builder = SqlAlchemySnapshotQueryDatabaseOperationBuilder(self.tables, self.type_, condition)
        table = builder.get_table()

        observed = builder.build()
        expected = select(table, literal(self.type_).label("_type")).filter(False_())

        self.assertTrue(expected.compare(observed))

    async def test_build_lower(self):
        condition = Condition.LOWER("doors", 1)
        builder = SqlAlchemySnapshotQueryDatabaseOperationBuilder(self.tables, self.type_, condition)
        table = builder.get_table()

        observed = builder.build()
        expected = select(table, literal(self.type_).label("_type")).filter(table.columns["doors"] < 1)

        self.assertTrue(expected.compare(observed))

    async def test_build_lower_equal(self):
        condition = Condition.LOWER_EQUAL("doors", 1)
        builder = SqlAlchemySnapshotQueryDatabaseOperationBuilder(self.tables, self.type_, condition)
        table = builder.get_table()

        observed = builder.build()
        expected = select(table, literal(self.type_).label("_type")).filter(table.columns["doors"] <= 1)

        self.assertTrue(expected.compare(observed))

    async def test_build_greater(self):
        condition = Condition.GREATER("doors", 1)
        builder = SqlAlchemySnapshotQueryDatabaseOperationBuilder(self.tables, self.type_, condition)
        table = builder.get_table()

        observed = builder.build()
        expected = select(table, literal(self.type_).label("_type")).filter(table.columns["doors"] > 1)

        self.assertTrue(expected.compare(observed))

    async def test_build_greater_equal(self):
        condition = Condition.GREATER_EQUAL("doors", 1)
        builder = SqlAlchemySnapshotQueryDatabaseOperationBuilder(self.tables, self.type_, condition)
        table = builder.get_table()

        observed = builder.build()
        expected = select(table, literal(self.type_).label("_type")).filter(table.columns["doors"] >= 1)

        self.assertTrue(expected.compare(observed))

    async def test_build_equal(self):
        condition = Condition.EQUAL("doors", 1)
        builder = SqlAlchemySnapshotQueryDatabaseOperationBuilder(self.tables, self.type_, condition)
        table = builder.get_table()

        observed = builder.build()
        expected = select(table, literal(self.type_).label("_type")).filter(table.columns["doors"] == 1)

        self.assertTrue(expected.compare(observed))

    async def test_build_not_equal(self):
        condition = Condition.NOT_EQUAL("doors", 1)
        builder = SqlAlchemySnapshotQueryDatabaseOperationBuilder(self.tables, self.type_, condition)
        table = builder.get_table()

        observed = builder.build()
        expected = select(table, literal(self.type_).label("_type")).filter(table.columns["doors"] != 1)

        self.assertTrue(expected.compare(observed))

    async def test_build_in(self):
        condition = Condition.IN("doors", [1, 2, 3])
        builder = SqlAlchemySnapshotQueryDatabaseOperationBuilder(self.tables, self.type_, condition)
        table = builder.get_table()

        observed = builder.build()
        expected = select(table, literal(self.type_).label("_type")).filter(table.columns["doors"].in_([1, 2, 3]))

        self.assertTrue(expected.compare(observed))

    async def test_build_in_empty(self):
        condition = Condition.IN("doors", [])
        builder = SqlAlchemySnapshotQueryDatabaseOperationBuilder(self.tables, self.type_, condition)
        table = builder.get_table()

        observed = builder.build()
        expected = select(table, literal(self.type_).label("_type")).filter(False_())

        self.assertTrue(expected.compare(observed))

    async def test_build_contains(self):
        condition = Condition.CONTAINS("color", "a")
        builder = SqlAlchemySnapshotQueryDatabaseOperationBuilder(self.tables, self.type_, condition)
        table = builder.get_table()

        observed = builder.build()
        expected = select(table, literal(self.type_).label("_type")).filter(contains_op(table.columns["color"], "a"))

        self.assertTrue(expected.compare(observed))

    async def test_build_like(self):
        condition = Condition.LIKE("color", "a%")
        builder = SqlAlchemySnapshotQueryDatabaseOperationBuilder(self.tables, self.type_, condition)
        table = builder.get_table()

        observed = builder.build()
        expected = select(table, literal(self.type_).label("_type")).filter(like_op(table.columns["color"], "a%"))

        self.assertTrue(expected.compare(observed))

    async def test_build_not(self):
        condition = Condition.NOT(Condition.LOWER("doors", 1))
        builder = SqlAlchemySnapshotQueryDatabaseOperationBuilder(self.tables, self.type_, condition)
        table = builder.get_table()

        observed = builder.build()
        expected = select(table, literal(self.type_).label("_type")).filter(not_(table.columns["doors"] < 1))

        self.assertTrue(expected.compare(observed))

    async def test_build_and(self):
        condition = Condition.AND(Condition.GREATER("doors", 1), Condition.LOWER("doors", 3))
        builder = SqlAlchemySnapshotQueryDatabaseOperationBuilder(self.tables, self.type_, condition)
        table = builder.get_table()

        observed = builder.build()
        expected = select(table, literal(self.type_).label("_type")).filter(
            and_(table.columns["doors"] > 1, table.columns["doors"] < 3)
        )

        self.assertTrue(expected.compare(observed))

    async def test_build_or(self):
        condition = Condition.OR(Condition.LOWER("doors", 1), Condition.GREATER("doors", 3))
        builder = SqlAlchemySnapshotQueryDatabaseOperationBuilder(self.tables, self.type_, condition)
        table = builder.get_table()

        observed = builder.build()
        expected = select(table, literal(self.type_).label("_type")).filter(
            or_(table.columns["doors"] < 1, table.columns["doors"] > 3)
        )

        self.assertTrue(expected.compare(observed))

    async def test_build_ordering_asc(self):
        ordering = Ordering.ASC("color")
        builder = SqlAlchemySnapshotQueryDatabaseOperationBuilder(self.tables, self.type_, ordering=ordering)
        table = builder.get_table()

        observed = builder.build()
        expected = select(table, literal(self.type_).label("_type")).order_by(table.columns["color"])

        self.assertTrue(expected.compare(observed))

    async def test_build_ordering_desc(self):
        ordering = Ordering.DESC("color")
        builder = SqlAlchemySnapshotQueryDatabaseOperationBuilder(self.tables, self.type_, ordering=ordering)
        table = builder.get_table()

        observed = builder.build()
        expected = select(table, literal(self.type_).label("_type")).order_by(desc(table.columns["color"]))

        self.assertTrue(expected.compare(observed))

    async def test_build_limit(self):
        builder = SqlAlchemySnapshotQueryDatabaseOperationBuilder(self.tables, self.type_, limit=10)
        table = builder.get_table()

        observed = builder.build()
        expected = select(table, literal(self.type_).label("_type")).limit(10)

        self.assertTrue(expected.compare(observed))

    async def test_build_complex(self):
        condition = Condition.AND(
            Condition.EQUAL("doors", 2),
            Condition.OR(Condition.EQUAL("color", "blue"), Condition.GREATER("version", 1)),
        )
        ordering = Ordering.DESC("updated_at")
        limit = 100

        builder = SqlAlchemySnapshotQueryDatabaseOperationBuilder(self.tables, self.type_, condition, ordering, limit)
        table = builder.get_table()

        observed = builder.build()
        expected = (
            select(table, literal(self.type_).label("_type"))
            .filter(
                and_(table.columns["doors"] == 2, or_(table.columns["color"] == "blue", table.columns["version"] > 1))
            )
            .order_by(desc(table.columns["updated_at"]))
            .limit(100)
        )

        self.assertTrue(expected.compare(observed))


if __name__ == "__main__":
    unittest.main()
