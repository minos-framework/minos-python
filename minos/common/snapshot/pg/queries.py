"""minos.common.snapshot.pg.queries module."""

from __future__ import (
    annotations,
)

import json
from typing import (
    Any,
    Optional,
)
from uuid import (
    uuid4,
)

from psycopg2.sql import (
    SQL,
    Composable,
    Identifier,
    Literal,
    Placeholder,
)

from ..queries import (
    ComposedCondition,
    ComposedOperator,
    Condition,
    FALSECondition,
    Ordering,
    SimpleCondition,
    SimpleOperator,
    TRUECondition,
)


class PostgreSqlSnapshotQueryBuilder:
    """TODO"""

    def __init__(self, aggregate_name: str, condition: Condition, ordering: Optional[Ordering], limit: Optional[int]):
        self.aggregate_name = aggregate_name
        self.condition = condition
        self.ordering = ordering
        self.limit = limit
        self._parameters = None

    def build(self) -> tuple[str, dict[str, Any]]:
        """TODO

        :return: TODO
        """
        self._parameters = dict()

        query = self._build()
        parameters = self._parameters

        return query, parameters

    def _build(self) -> str:
        self._parameters["aggregate_name"] = self.aggregate_name

        query = SQL(" AND ").join([_SELECT_MULTIPLE_ENTRIES_QUERY, self._build_query(self.condition)])

        if self.ordering is not None:
            order_by = SQL("ORDER BY {key} {direction}").format(
                key=Identifier(self.ordering.key), direction=_ORDERING_MAPPER[self.ordering.reverse]
            )
            query = SQL(" ").join([query, order_by])

        if self.limit is not None:
            limit = SQL(" LIMIT {limit}").format(limit=Literal(self.limit))
            query = SQL(" ").join([query, limit])

        return query

    def _build_query(self, condition: Condition) -> Composable:
        if isinstance(condition, ComposedCondition):
            return self._build_composed_query(condition)
        elif isinstance(condition, SimpleCondition):
            return self._build_simple_query(condition)
        elif isinstance(condition, TRUECondition):
            return SQL("TRUE")
        elif isinstance(condition, FALSECondition):
            return SQL("FALSE")
        else:
            raise Exception

    def _build_composed_query(self, condition: ComposedCondition) -> Composable:
        parts = (self._build_query(c) for c in condition.conditions)
        operator = _COMPOSED_MAPPER[condition.operator]

        return SQL("({composed})").format(composed=operator.join(parts))

    def _build_simple_query(self, condition: SimpleCondition) -> Composable:
        field = condition.field
        operator = _SIMPLE_MAPPER[condition.operator]

        value = condition.value
        if isinstance(value, (list, tuple, set)):
            value = tuple(value)
            if value == tuple():
                return self._build_query(FALSECondition())

        if field in _DIRECT_FIELDS_MAPPER:
            self._parameters[field] = value
            return SQL("({field} {operator} {name})").format(
                field=_DIRECT_FIELDS_MAPPER[field], operator=operator, name=Placeholder(field)
            )
        else:
            name = str(uuid4())
            self._parameters[name] = json.dumps(value)
            return SQL("((data#>{field}) {operator} {name}::jsonb)").format(
                field=Literal("{{{}}}".format(field.replace(".", ","))), operator=operator, name=Placeholder(name)
            )


_COMPOSED_MAPPER = {ComposedOperator.AND: SQL(" AND "), ComposedOperator.OR: SQL(" OR ")}

_SIMPLE_MAPPER = {
    SimpleOperator.LOWER: SQL("<"),
    SimpleOperator.LOWER_EQUAL: SQL("<="),
    SimpleOperator.GREATER: SQL(">"),
    SimpleOperator.GREATER_EQUAL: SQL(">="),
    SimpleOperator.EQUAL: SQL("="),
    SimpleOperator.NOT_EQUAL: SQL("<>"),
    SimpleOperator.IN: SQL("IN"),
}

_DIRECT_FIELDS_MAPPER = {
    "uuid": Identifier("aggregate_uuid"),
    "version": Identifier("version"),
    "created_at": Identifier("created_at"),
    "updated_at": Identifier("updated_at"),
}

_ORDERING_MAPPER = {
    True: SQL("DESC"),
    False: SQL("ASC"),
}

_SELECT_MULTIPLE_ENTRIES_QUERY = SQL(
    """
SELECT aggregate_uuid, aggregate_name, version, schema, data, created_at, updated_at
FROM snapshot
WHERE aggregate_name = %(aggregate_name)s
    """.strip()
)
