import unittest
from unittest.mock import (
    MagicMock,
    patch,
)
from uuid import (
    uuid4,
)

import aiopg
from psycopg2.extras import (
    Json,
)
from psycopg2.sql import (
    SQL,
    Literal,
    Placeholder,
)

from minos.aggregate import (
    IS_REPOSITORY_SERIALIZATION_CONTEXT_VAR,
    Condition,
    Ordering,
    PostgreSqlSnapshotQueryBuilder,
)
from minos.aggregate.snapshots.pg.queries import (
    _SELECT_ENTRIES_QUERY,
    _SELECT_TRANSACTION_CHUNK,
)
from minos.common import (
    NULL_UUID,
)
from minos.common.testing import (
    PostgresAsyncTestCase,
)
from tests.utils import (
    BASE_PATH,
)


class TestPostgreSqlSnapshotQueryBuilder(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def setUp(self) -> None:
        super().setUp()
        self.base_parameters = {
            "aggregate_name": "path.to.Aggregate",
            "transaction_uuid_1": NULL_UUID,
        }
        self.base_select = _SELECT_ENTRIES_QUERY.format(
            from_parts=_SELECT_TRANSACTION_CHUNK.format(
                index=Literal(1), transaction_uuid=Placeholder("transaction_uuid_1")
            )
        )

    def test_constructor(self):
        qb = PostgreSqlSnapshotQueryBuilder("path.to.Aggregate", Condition.TRUE)
        self.assertEqual("path.to.Aggregate", qb.aggregate_name)
        self.assertEqual(Condition.TRUE, qb.condition)
        self.assertEqual(None, qb.ordering)
        self.assertEqual(None, qb.limit)
        self.assertFalse(qb.exclude_deleted)

    def test_constructor_full(self):
        transaction_uuids = (NULL_UUID, uuid4())
        qb = PostgreSqlSnapshotQueryBuilder(
            "path.to.Aggregate", Condition.TRUE, Ordering.ASC("name"), 10, transaction_uuids, True
        )
        self.assertEqual("path.to.Aggregate", qb.aggregate_name)
        self.assertEqual(Condition.TRUE, qb.condition)
        self.assertEqual(Ordering.ASC("name"), qb.ordering)
        self.assertEqual(10, qb.limit)
        self.assertEqual(transaction_uuids, qb.transaction_uuids)
        self.assertTrue(qb.exclude_deleted)

    def test_build_submitting_context_var(self):
        builder = PostgreSqlSnapshotQueryBuilder("path.to.Aggregate", Condition.TRUE)

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
            PostgreSqlSnapshotQueryBuilder("path.to.Aggregate", True).build()

    async def test_build_with_transactions(self):
        transaction_uuids = (NULL_UUID, uuid4())
        observed = PostgreSqlSnapshotQueryBuilder(
            "path.to.Aggregate", Condition.TRUE, transaction_uuids=transaction_uuids
        ).build()

        expected_query = SQL(" WHERE ").join(
            [
                _SELECT_ENTRIES_QUERY.format(
                    from_parts=SQL(" UNION ALL ").join(
                        [
                            _SELECT_TRANSACTION_CHUNK.format(
                                index=Literal(1), transaction_uuid=Placeholder("transaction_uuid_1")
                            ),
                            _SELECT_TRANSACTION_CHUNK.format(
                                index=Literal(2), transaction_uuid=Placeholder("transaction_uuid_2")
                            ),
                        ]
                    )
                ),
                SQL("TRUE"),
            ]
        )
        expected_parameters = self.base_parameters | {"transaction_uuid_2": transaction_uuids[1]}

        self.assertEqual(await self._flatten_query(expected_query), await self._flatten_query(observed[0]))
        self.assertEqual(self._flatten_parameters(expected_parameters), self._flatten_parameters(observed[1]))

    async def test_build_true(self):
        condition = Condition.TRUE
        observed = PostgreSqlSnapshotQueryBuilder("path.to.Aggregate", condition).build()

        expected_query = SQL(" WHERE ").join([self.base_select, SQL("TRUE")])
        expected_parameters = self.base_parameters

        self.assertEqual(await self._flatten_query(expected_query), await self._flatten_query(observed[0]))
        self.assertEqual(self._flatten_parameters(expected_parameters), self._flatten_parameters(observed[1]))

    async def test_build_false(self):
        condition = Condition.FALSE
        observed = PostgreSqlSnapshotQueryBuilder("path.to.Aggregate", condition).build()
        expected_query = SQL(" WHERE ").join([self.base_select, SQL("FALSE")])
        expected_parameters = self.base_parameters
        self.assertEqual(await self._flatten_query(expected_query), await self._flatten_query(observed[0]))
        self.assertEqual(self._flatten_parameters(expected_parameters), self._flatten_parameters(observed[1]))

    async def test_build_fixed_uuid(self):
        uuid = uuid4()
        condition = Condition.EQUAL("uuid", uuid)
        with patch("minos.aggregate.PostgreSqlSnapshotQueryBuilder.generate_random_str", side_effect=["hello"]):
            observed = PostgreSqlSnapshotQueryBuilder("path.to.Aggregate", condition).build()

        expected_query = SQL(" WHERE ").join([self.base_select, SQL('("aggregate_uuid" = %(hello)s)')])
        expected_parameters = {"hello": str(uuid)} | self.base_parameters

        self.assertEqual(await self._flatten_query(expected_query), await self._flatten_query(observed[0]))
        self.assertEqual(self._flatten_parameters(expected_parameters), self._flatten_parameters(observed[1]))

    async def test_build_fixed_version(self):
        condition = Condition.EQUAL("version", 1)
        with patch("minos.aggregate.PostgreSqlSnapshotQueryBuilder.generate_random_str", side_effect=["hello"]):
            observed = PostgreSqlSnapshotQueryBuilder("path.to.Aggregate", condition).build()

        expected_query = SQL(" WHERE ").join([self.base_select, SQL('("version" = %(hello)s)')])
        expected_parameters = {"hello": 1} | self.base_parameters

        self.assertEqual(await self._flatten_query(expected_query), await self._flatten_query(observed[0]))
        self.assertEqual(self._flatten_parameters(expected_parameters), self._flatten_parameters(observed[1]))

    async def test_build_fixed_created_at(self):
        condition = Condition.EQUAL("created_at", 1)
        with patch("minos.aggregate.PostgreSqlSnapshotQueryBuilder.generate_random_str", side_effect=["hello"]):
            observed = PostgreSqlSnapshotQueryBuilder("path.to.Aggregate", condition).build()

        expected_query = SQL(" WHERE ").join([self.base_select, SQL('("created_at" = %(hello)s)')])
        expected_parameters = {"hello": 1} | self.base_parameters

        self.assertEqual(await self._flatten_query(expected_query), await self._flatten_query(observed[0]))
        self.assertEqual(self._flatten_parameters(expected_parameters), self._flatten_parameters(observed[1]))

    async def test_build_fixed_updated_at(self):
        condition = Condition.EQUAL("updated_at", 1)
        with patch("minos.aggregate.PostgreSqlSnapshotQueryBuilder.generate_random_str", side_effect=["hello"]):
            observed = PostgreSqlSnapshotQueryBuilder("path.to.Aggregate", condition).build()

        expected_query = SQL(" WHERE ").join([self.base_select, SQL('("updated_at" = %(hello)s)')])
        expected_parameters = {"hello": 1} | self.base_parameters

        self.assertEqual(await self._flatten_query(expected_query), await self._flatten_query(observed[0]))
        self.assertEqual(self._flatten_parameters(expected_parameters), self._flatten_parameters(observed[1]))

    async def test_build_lower(self):
        condition = Condition.LOWER("age", 1)
        with patch("minos.aggregate.PostgreSqlSnapshotQueryBuilder.generate_random_str", side_effect=["hello"]):
            observed = PostgreSqlSnapshotQueryBuilder("path.to.Aggregate", condition).build()

        expected_query = SQL(" WHERE ").join([self.base_select, SQL("(data#>'{age}' < %(hello)s::jsonb)")])
        expected_parameters = {"hello": 1} | self.base_parameters

        self.assertEqual(await self._flatten_query(expected_query), await self._flatten_query(observed[0]))
        self.assertEqual(self._flatten_parameters(expected_parameters), self._flatten_parameters(observed[1]))

    async def test_build_lower_equal(self):
        condition = Condition.LOWER_EQUAL("age", 1)
        with patch("minos.aggregate.PostgreSqlSnapshotQueryBuilder.generate_random_str", side_effect=["hello"]):
            observed = PostgreSqlSnapshotQueryBuilder("path.to.Aggregate", condition).build()

        expected_query = SQL(" WHERE ").join([self.base_select, SQL("(data#>'{age}' <= %(hello)s::jsonb)")])
        expected_parameters = {"hello": 1} | self.base_parameters

        self.assertEqual(await self._flatten_query(expected_query), await self._flatten_query(observed[0]))
        self.assertEqual(self._flatten_parameters(expected_parameters), self._flatten_parameters(observed[1]))

    async def test_build_greater(self):
        condition = Condition.GREATER("age", 1)
        with patch("minos.aggregate.PostgreSqlSnapshotQueryBuilder.generate_random_str", side_effect=["hello"]):
            observed = PostgreSqlSnapshotQueryBuilder("path.to.Aggregate", condition).build()

        expected_query = SQL(" WHERE ").join([self.base_select, SQL("(data#>'{age}' > %(hello)s::jsonb)")])
        expected_parameters = {"hello": 1} | self.base_parameters

        self.assertEqual(await self._flatten_query(expected_query), await self._flatten_query(observed[0]))
        self.assertEqual(self._flatten_parameters(expected_parameters), self._flatten_parameters(observed[1]))

    async def test_build_greater_equal(self):
        condition = Condition.GREATER_EQUAL("age", 1)
        with patch("minos.aggregate.PostgreSqlSnapshotQueryBuilder.generate_random_str", side_effect=["hello"]):
            observed = PostgreSqlSnapshotQueryBuilder("path.to.Aggregate", condition).build()

        expected_query = SQL(" WHERE ").join([self.base_select, SQL("(data#>'{age}' >= %(hello)s::jsonb)")])
        expected_parameters = {"hello": 1} | self.base_parameters

        self.assertEqual(await self._flatten_query(expected_query), await self._flatten_query(observed[0]))
        self.assertEqual(self._flatten_parameters(expected_parameters), self._flatten_parameters(observed[1]))

    async def test_build_equal(self):
        condition = Condition.EQUAL("age", 1)
        with patch("minos.aggregate.PostgreSqlSnapshotQueryBuilder.generate_random_str", side_effect=["hello"]):
            observed = PostgreSqlSnapshotQueryBuilder("path.to.Aggregate", condition).build()

        expected_query = SQL(" WHERE ").join([self.base_select, SQL("(data#>'{age}' = %(hello)s::jsonb)")])
        expected_parameters = {"hello": 1} | self.base_parameters

        self.assertEqual(await self._flatten_query(expected_query), await self._flatten_query(observed[0]))
        self.assertEqual(self._flatten_parameters(expected_parameters), self._flatten_parameters(observed[1]))

    async def test_build_not_equal(self):
        condition = Condition.NOT_EQUAL("age", 1)
        with patch("minos.aggregate.PostgreSqlSnapshotQueryBuilder.generate_random_str", side_effect=["hello"]):
            observed = PostgreSqlSnapshotQueryBuilder("path.to.Aggregate", condition).build()

        expected_query = SQL(" WHERE ").join([self.base_select, SQL("(data#>'{age}' <> %(hello)s::jsonb)")])
        expected_parameters = {"hello": 1} | self.base_parameters

        self.assertEqual(await self._flatten_query(expected_query), await self._flatten_query(observed[0]))
        self.assertEqual(self._flatten_parameters(expected_parameters), self._flatten_parameters(observed[1]))

    async def test_build_in(self):
        condition = Condition.IN("age", [1, 2, 3])
        with patch("minos.aggregate.PostgreSqlSnapshotQueryBuilder.generate_random_str", side_effect=["hello"]):
            observed = PostgreSqlSnapshotQueryBuilder("path.to.Aggregate", condition).build()

        expected_query = SQL(" WHERE ").join([self.base_select, SQL("(data#>'{age}' IN %(hello)s::jsonb)")])
        expected_parameters = {"hello": (1, 2, 3)} | self.base_parameters

        self.assertEqual(await self._flatten_query(expected_query), await self._flatten_query(observed[0]))
        self.assertEqual(self._flatten_parameters(expected_parameters), self._flatten_parameters(observed[1]))

    async def test_build_in_empty(self):
        condition = Condition.IN("age", [])
        with patch("minos.aggregate.PostgreSqlSnapshotQueryBuilder.generate_random_str", side_effect=["hello"]):
            observed = PostgreSqlSnapshotQueryBuilder("path.to.Aggregate", condition).build()

        expected_query = SQL(" WHERE ").join([self.base_select, SQL("FALSE")])
        expected_parameters = self.base_parameters

        self.assertEqual(await self._flatten_query(expected_query), await self._flatten_query(observed[0]))
        self.assertEqual(self._flatten_parameters(expected_parameters), self._flatten_parameters(observed[1]))

    async def test_build_not(self):
        condition = Condition.NOT(Condition.LOWER("age", 1))
        with patch("minos.aggregate.PostgreSqlSnapshotQueryBuilder.generate_random_str", side_effect=["hello"]):
            observed = PostgreSqlSnapshotQueryBuilder("path.to.Aggregate", condition).build()

        expected_query = SQL(" WHERE ").join([self.base_select, SQL("(NOT (data#>'{age}' < %(hello)s::jsonb))")])
        expected_parameters = {"hello": 1} | self.base_parameters

        self.assertEqual(await self._flatten_query(expected_query), await self._flatten_query(observed[0]))
        self.assertEqual(self._flatten_parameters(expected_parameters), self._flatten_parameters(observed[1]))

    async def test_build_and(self):
        condition = Condition.AND(Condition.LOWER("age", 1), Condition.LOWER("level", 3))
        with patch(
            "minos.aggregate.PostgreSqlSnapshotQueryBuilder.generate_random_str", side_effect=["hello", "goodbye"]
        ):
            observed = PostgreSqlSnapshotQueryBuilder("path.to.Aggregate", condition).build()

        expected_query = SQL(" WHERE ").join(
            [self.base_select, SQL("((data#>'{age}' < %(hello)s::jsonb) AND (data#>'{level}' < %(goodbye)s::jsonb))")]
        )
        expected_parameters = {"hello": 1, "goodbye": 3} | self.base_parameters

        self.assertEqual(await self._flatten_query(expected_query), await self._flatten_query(observed[0]))
        self.assertEqual(self._flatten_parameters(expected_parameters), self._flatten_parameters(observed[1]))

    async def test_build_or(self):
        condition = Condition.OR(Condition.LOWER("age", 1), Condition.LOWER("level", 3))
        with patch(
            "minos.aggregate.PostgreSqlSnapshotQueryBuilder.generate_random_str", side_effect=["hello", "goodbye"]
        ):
            observed = PostgreSqlSnapshotQueryBuilder("path.to.Aggregate", condition).build()

        expected_query = SQL(" WHERE ").join(
            [self.base_select, SQL("((data#>'{age}' < %(hello)s::jsonb) OR (data#>'{level}' < %(goodbye)s::jsonb))")]
        )
        expected_parameters = {"hello": 1, "goodbye": 3} | self.base_parameters

        self.assertEqual(await self._flatten_query(expected_query), await self._flatten_query(observed[0]))
        self.assertEqual(self._flatten_parameters(expected_parameters), self._flatten_parameters(observed[1]))

    async def test_build_exclude_deleted(self):
        observed = PostgreSqlSnapshotQueryBuilder("path.to.Aggregate", Condition.TRUE, exclude_deleted=True).build()

        expected_query = SQL(" WHERE ").join([self.base_select, SQL("TRUE AND (data IS NOT NULL)")])
        expected_parameters = self.base_parameters

        self.assertEqual(await self._flatten_query(expected_query), await self._flatten_query(observed[0]))
        self.assertEqual(self._flatten_parameters(expected_parameters), self._flatten_parameters(observed[1]))

    async def test_build_fixed_ordering_asc(self):
        ordering = Ordering.ASC("created_at")
        observed = PostgreSqlSnapshotQueryBuilder("path.to.Aggregate", Condition.TRUE, ordering).build()

        expected_query = SQL(" WHERE ").join([self.base_select, SQL('TRUE ORDER BY "created_at" ASC')])

        expected_parameters = self.base_parameters

        self.assertEqual(await self._flatten_query(expected_query), await self._flatten_query(observed[0]))
        self.assertEqual(self._flatten_parameters(expected_parameters), self._flatten_parameters(observed[1]))

    async def test_build_fixed_ordering_desc(self):
        ordering = Ordering.DESC("created_at")
        observed = PostgreSqlSnapshotQueryBuilder("path.to.Aggregate", Condition.TRUE, ordering).build()

        expected_query = SQL(" WHERE ").join([self.base_select, SQL('TRUE ORDER BY "created_at" DESC')])
        expected_parameters = self.base_parameters

        self.assertEqual(await self._flatten_query(expected_query), await self._flatten_query(observed[0]))
        self.assertEqual(self._flatten_parameters(expected_parameters), self._flatten_parameters(observed[1]))

    async def test_build_ordering_asc(self):
        ordering = Ordering.ASC("name")
        observed = PostgreSqlSnapshotQueryBuilder("path.to.Aggregate", Condition.TRUE, ordering).build()

        expected_query = SQL(" WHERE ").join([self.base_select, SQL("TRUE ORDER BY data#>'{name}' ASC")])
        expected_parameters = self.base_parameters

        self.assertEqual(await self._flatten_query(expected_query), await self._flatten_query(observed[0]))
        self.assertEqual(self._flatten_parameters(expected_parameters), self._flatten_parameters(observed[1]))

    async def test_build_ordering_desc(self):
        ordering = Ordering.DESC("name")
        observed = PostgreSqlSnapshotQueryBuilder("path.to.Aggregate", Condition.TRUE, ordering).build()

        expected_query = SQL(" WHERE ").join([self.base_select, SQL("TRUE ORDER BY data#>'{name}' DESC")])

        expected_parameters = self.base_parameters

        self.assertEqual(await self._flatten_query(expected_query), await self._flatten_query(observed[0]))
        self.assertEqual(self._flatten_parameters(expected_parameters), self._flatten_parameters(observed[1]))

    async def test_build_limit(self):
        observed = PostgreSqlSnapshotQueryBuilder("path.to.Aggregate", Condition.TRUE, limit=10).build()

        expected_query = SQL(" WHERE ").join([self.base_select, SQL("TRUE LIMIT 10")])

        expected_parameters = self.base_parameters

        self.assertEqual(await self._flatten_query(expected_query), await self._flatten_query(observed[0]))
        self.assertEqual(self._flatten_parameters(expected_parameters), self._flatten_parameters(observed[1]))

    async def test_build_complex(self):
        condition = Condition.AND(
            Condition.EQUAL("inventory.amount", 0),
            Condition.OR(Condition.EQUAL("title", "Fanta Zero"), Condition.GREATER("version", 1)),
        )
        ordering = Ordering.DESC("updated_at")
        limit = 100

        with patch(
            "minos.aggregate.PostgreSqlSnapshotQueryBuilder.generate_random_str", side_effect=["one", "two", "three"]
        ):
            observed = PostgreSqlSnapshotQueryBuilder("path.to.Aggregate", condition, ordering, limit).build()

        expected_query = SQL(" WHERE ").join(
            [
                self.base_select,
                SQL(
                    "((data#>'{inventory,amount}' = %(one)s::jsonb) AND ((data#>'{title}' = %(two)s::jsonb) OR "
                    '("version" > %(three)s))) '
                    'ORDER BY "updated_at" DESC '
                    "LIMIT 100"
                ),
            ]
        )

        expected_parameters = {"one": 0, "three": 1, "two": "Fanta Zero"} | self.base_parameters

        self.assertEqual(await self._flatten_query(expected_query), await self._flatten_query(observed[0]))
        self.assertEqual(self._flatten_parameters(expected_parameters), self._flatten_parameters(observed[1]))

    async def _flatten_query(self, query) -> str:
        async with aiopg.connect(**self.snapshot_db) as connection:
            return query.as_string(connection.raw)

    @staticmethod
    def _flatten_parameters(parameters) -> dict:
        return {k: (v if not isinstance(v, Json) else v.adapted) for k, v in parameters.items()}


if __name__ == "__main__":
    unittest.main()
