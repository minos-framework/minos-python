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

from ...queries import (
    _FALSE_CONDITION,
    _AndCondition,
    _ComposedCondition,
    _Condition,
    _EqualCondition,
    _FalseCondition,
    _GreaterCondition,
    _GreaterEqualCondition,
    _InCondition,
    _LowerCondition,
    _LowerEqualCondition,
    _NotCondition,
    _NotEqualCondition,
    _OrCondition,
    _Ordering,
    _SimpleCondition,
    _TrueCondition,
)


class PostgreSqlSnapshotQueryBuilder:
    """TODO"""

    def __init__(self, aggregate_name: str, condition: _Condition, ordering: Optional[_Ordering], limit: Optional[int]):
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
                key=Identifier(self.ordering.by), direction=_ORDERING_MAPPER[self.ordering.reverse]
            )
            query = SQL(" ").join([query, order_by])

        if self.limit is not None:
            limit = SQL(" LIMIT {limit}").format(limit=Literal(self.limit))
            query = SQL(" ").join([query, limit])

        return query

    def _build_query(self, condition: _Condition) -> Composable:
        if isinstance(condition, _NotCondition):
            return self._build_not_query(condition)
        if isinstance(condition, _ComposedCondition):
            return self._build_composed_query(condition)
        elif isinstance(condition, _SimpleCondition):
            return self._build_simple_query(condition)
        elif isinstance(condition, _TrueCondition):
            return SQL("TRUE")
        elif isinstance(condition, _FalseCondition):
            return SQL("FALSE")
        else:
            raise Exception

    def _build_not_query(self, condition: _NotCondition) -> Composable:
        return SQL("(NOT {})").format(self._build_query(condition.inner))

    def _build_composed_query(self, condition: _ComposedCondition) -> Composable:
        if not len(condition.parts):
            return self._build_query(_FALSE_CONDITION)

        # noinspection PyTypeChecker
        operator = _COMPOSED_MAPPER[type(condition)]
        parts = (self._build_query(c) for c in condition)
        return SQL("({composed})").format(composed=operator.join(parts))

    def _build_simple_query(self, condition: _SimpleCondition) -> Composable:
        field = condition.field
        # noinspection PyTypeChecker
        operator = _SIMPLE_MAPPER[type(condition)]

        value = condition.value
        if isinstance(value, (list, tuple, set)):
            value = tuple(value)
            if value == tuple():
                return self._build_query(_FALSE_CONDITION)

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
