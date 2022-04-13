from __future__ import (
    annotations,
)

from typing import (
    Any,
    Iterable,
    Optional,
)
from uuid import (
    UUID,
    uuid4,
)

from psycopg2.extras import (
    Json,
)
from psycopg2.sql import (
    SQL,
    Composable,
    Identifier,
    Literal,
    Placeholder,
)

from minos.aggregate import (
    IS_REPOSITORY_SERIALIZATION_CONTEXT_VAR,
)
from minos.aggregate.queries import (
    _FALSE_CONDITION,
    _AndCondition,
    _ComposedCondition,
    _Condition,
    _EqualCondition,
    _FalseCondition,
    _GreaterCondition,
    _GreaterEqualCondition,
    _InCondition,
    _LikeCondition,
    _LowerCondition,
    _LowerEqualCondition,
    _NotCondition,
    _NotEqualCondition,
    _OrCondition,
    _Ordering,
    _SimpleCondition,
    _TrueCondition,
)
from minos.common import (
    NULL_UUID,
    AvroDataEncoder,
)


# noinspection SqlResolve,SqlNoDataSourceInspection
class AiopgSnapshotQueryDatabaseOperationBuilder:
    """Aiopg Snapshot Query Database Operation Builder class.

    This class build postgres-compatible database queries over fields based on a condition, ordering, etc.
    """

    def __init__(
        self,
        name: str,
        condition: _Condition,
        ordering: Optional[_Ordering] = None,
        limit: Optional[int] = None,
        transaction_uuids: Iterable[UUID, ...] = (NULL_UUID,),
        exclude_deleted: bool = False,
        table_name: Optional[str] = None,
    ):
        if not isinstance(transaction_uuids, tuple):
            transaction_uuids = tuple(transaction_uuids)
        if table_name is None:
            table_name = "snapshot"
        self.name = name
        self.condition = condition
        self.ordering = ordering
        self.limit = limit
        self.transaction_uuids = transaction_uuids
        self.exclude_deleted = exclude_deleted
        self.table_name = table_name
        self._parameters = None

    def build(self) -> tuple[Composable, dict[str, Any]]:
        """Build a query.

        :return: A tuple in which the first value is the sql sentence and the second one is a dictionary containing the
            query parameters.
        """
        self._parameters = dict()

        token = IS_REPOSITORY_SERIALIZATION_CONTEXT_VAR.set(True)
        try:
            query = self._build()
        finally:
            IS_REPOSITORY_SERIALIZATION_CONTEXT_VAR.reset(token)

        parameters = self._parameters

        return query, parameters

    def _build(self) -> Composable:
        self._parameters["name"] = self.name

        query = SQL(" WHERE ").join([self._build_select_from(), self._build_condition(self.condition)])

        if self.exclude_deleted:
            query = SQL(" AND ").join([query, self._EXCLUDE_DELETED_CONDITION])

        if self.ordering is not None:
            query = SQL(" ").join([query, self._build_ordering(self.ordering)])

        if self.limit is not None:
            query = SQL(" ").join([query, self._build_limit(self.limit)])

        return query

    def _build_select_from(self) -> Composable:
        from_query_parts = list()
        for index, transaction_uuid in enumerate(self.transaction_uuids, start=1):
            name = f"transaction_uuid_{index}"
            self._parameters[name] = transaction_uuid

            from_query_parts.append(
                self._SELECT_TRANSACTION_CHUNK.format(
                    index=Literal(index), transaction_uuid=Placeholder(name), table_name=Identifier(self.table_name)
                )
            )

        from_query = SQL(" UNION ALL ").join(from_query_parts)

        query = self._SELECT_ENTRIES_QUERY.format(from_parts=from_query)
        return query

    def _build_condition(self, condition: _Condition) -> Composable:
        if isinstance(condition, _NotCondition):
            return self._build_condition_not(condition)
        if isinstance(condition, _ComposedCondition):
            return self._build_condition_composed(condition)
        if isinstance(condition, _TrueCondition):
            return SQL("TRUE")
        if isinstance(condition, _FalseCondition):
            return SQL("FALSE")
        if isinstance(condition, _LikeCondition):
            return self._build_condition_like(condition)
        if isinstance(condition, _SimpleCondition):
            return self._build_condition_simple(condition)

        raise ValueError(f"Given condition is not supported. Obtained: {condition}")

    def _build_condition_not(self, condition: _NotCondition) -> Composable:
        return SQL("(NOT {})").format(self._build_condition(condition.inner))

    def _build_condition_composed(self, condition: _ComposedCondition) -> Composable:
        # noinspection PyTypeChecker
        operator = self._COMPOSED_MAPPER[type(condition)]
        parts = (self._build_condition(c) for c in condition)
        return SQL("({composed})").format(composed=operator.join(parts))

    def _build_condition_simple(self, condition: _SimpleCondition) -> Composable:
        field = condition.field
        # noinspection PyTypeChecker
        operator = self._SIMPLE_MAPPER[type(condition)]

        parameter = AvroDataEncoder(condition.parameter).build()
        if isinstance(parameter, list):
            if not len(parameter):
                return self._build_condition(_FALSE_CONDITION)
            parameter = tuple(parameter)

        if field in self._FIXED_FIELDS_MAPPER:
            name = self.generate_random_str()
            self._parameters[name] = parameter

            field = self._FIXED_FIELDS_MAPPER[field]
            name = Placeholder(name)
            return SQL("({field} {operator} {name})").format(field=field, operator=operator, name=name)
        else:
            name = self.generate_random_str()
            self._parameters[name] = Json(parameter)

            field = Literal("{{{}}}".format(field.replace(".", ",")))
            name = Placeholder(name)
            return SQL("(data#>{field} {operator} {name}::jsonb)").format(field=field, operator=operator, name=name)

    def _build_condition_like(self, condition: _SimpleCondition) -> Composable:
        field = condition.field

        parameter = AvroDataEncoder(condition.parameter).build()

        if field in self._FIXED_FIELDS_MAPPER:
            name = self.generate_random_str()
            self._parameters[name] = parameter

            field = self._FIXED_FIELDS_MAPPER[field]
            name = Placeholder(name)
            return SQL("({field}::text LIKE {name})").format(field=field, name=name)
        else:
            name = self.generate_random_str()
            self._parameters[name] = parameter

            field = Literal("{{{}}}".format(field.replace(".", ",")))
            name = Placeholder(name)
            return SQL("(data#>>{field} LIKE {name})").format(field=field, name=name)

    def _build_ordering(self, ordering: _Ordering) -> Composable:
        field = ordering.by
        direction = self._ORDERING_MAPPER[ordering.reverse]

        if field in self._FIXED_FIELDS_MAPPER:
            field = Identifier(field)
            order_by = SQL("ORDER BY {field} {direction}").format(field=field, direction=direction)
        else:
            field = Literal("{{{}}}".format(field.replace(".", ",")))
            order_by = SQL("ORDER BY data#>{field} {direction}").format(field=field, direction=direction)

        return order_by

    @staticmethod
    def _build_limit(value: int) -> Composable:
        limit = SQL("LIMIT {limit}").format(limit=Literal(value))
        return limit

    @staticmethod
    def generate_random_str() -> str:
        """Generate a random string

        :return: A random string value.
        """
        return str(uuid4())

    _COMPOSED_MAPPER = {_AndCondition: SQL(" AND "), _OrCondition: SQL(" OR ")}

    _SIMPLE_MAPPER = {
        _LowerCondition: SQL("<"),
        _LowerEqualCondition: SQL("<="),
        _GreaterCondition: SQL(">"),
        _GreaterEqualCondition: SQL(">="),
        _EqualCondition: SQL("="),
        _NotEqualCondition: SQL("<>"),
        _InCondition: SQL("IN"),
    }

    _FIXED_FIELDS_MAPPER = {
        "uuid": Identifier("uuid"),
        "version": Identifier("version"),
        "created_at": Identifier("created_at"),
        "updated_at": Identifier("updated_at"),
    }

    _ORDERING_MAPPER = {
        True: SQL("DESC"),
        False: SQL("ASC"),
    }

    _SELECT_ENTRIES_QUERY = SQL(
        "SELECT "
        "   t2.uuid, "
        "   t2.name, "
        "   t2.version, "
        "   t2.schema, "
        "   t2.data, "
        "   t2.created_at, "
        "   t2.updated_at, "
        "   t2.transaction_uuid "
        "FROM ("
        "   SELECT DISTINCT ON (uuid) t1.* "
        "   FROM ( {from_parts} ) AS t1 "
        "   ORDER BY uuid, transaction_index DESC "
        ") AS t2"
    )

    _SELECT_TRANSACTION_CHUNK = SQL(
        "SELECT {index} AS transaction_index, * "
        "FROM {table_name} "
        "WHERE name = %(name)s AND transaction_uuid = {transaction_uuid} "
    )

    _EXCLUDE_DELETED_CONDITION = SQL("(data IS NOT NULL)")
