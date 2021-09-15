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

from ..conditions import (
    ComposedCondition,
    ComposedOperator,
    Condition,
    FALSECondition,
    SimpleCondition,
    SimpleOperator,
    TRUECondition,
)


class PostgreSqlSnapshotQueryBuilder:
    def __init__(self, aggregate_name: str, condition: Condition, ordering: Optional[str], limit: Optional[int]):
        self.aggregate_name = aggregate_name
        self.condition = condition
        self.ordering = ordering
        self.limit = limit
        self._parameters = None

    def build(self) -> tuple[str, dict[str, Any]]:
        self._parameters = dict()

        query = self._build()
        parameters = self._parameters

        return query, parameters

    def _build(self) -> str:
        self._parameters["aggregate_name"] = self.aggregate_name
        query = f"{_SELECT_MULTIPLE_ENTRIES_QUERY} AND "

        query += self._build_query(self.condition)

        if self.ordering is not None:
            query += f" ORDER BY {self.ordering}"

        if self.limit is not None:
            query += f" LIMIT {self.limit}"

        return query

    def _build_query(self, condition: Condition) -> str:
        if isinstance(condition, ComposedCondition):
            return self._build_composed_query(condition)
        elif isinstance(condition, SimpleCondition):
            return self._build_simple_query(condition)
        elif isinstance(condition, TRUECondition):
            return "TRUE"
        elif isinstance(condition, FALSECondition):
            return "FALSE"
        else:
            raise Exception

    def _build_composed_query(self, condition: ComposedCondition) -> str:
        parts = (self._build_query(c) for c in condition.conditions)
        operator = _COMPOSED_MAPPER[condition.operator]

        return "(" + f" {operator} ".join(parts) + ")"

    def _build_simple_query(self, condition: SimpleCondition) -> str:
        field = condition.field.replace(".", ",")
        operator = _SIMPLE_MAPPER[condition.operator]

        value = condition.value
        if isinstance(value, (list, tuple, set)):
            value = tuple(value)
            if value == tuple():
                return self._build_query(FALSECondition())

        if field in _DIRECT_FIELDS_MAPPER:
            self._parameters[field] = value
            return f"({_DIRECT_FIELDS_MAPPER[field]} {operator} %({field})s)"
        else:
            name = str(uuid4())
            self._parameters[name] = json.dumps(value)
            return f"((data#>'{{{field}}}') {operator} %({name})s::jsonb)"


_COMPOSED_MAPPER = {ComposedOperator.AND: "AND", ComposedOperator.OR: "OR"}

_SIMPLE_MAPPER = {
    SimpleOperator.LOWER: "<",
    SimpleOperator.LOWER_EQUAL: "<=",
    SimpleOperator.GREATER: ">",
    SimpleOperator.GREATER_EQUAL: ">=",
    SimpleOperator.EQUAL: "=",
    SimpleOperator.NOT_EQUAL: "<>",
    SimpleOperator.IN: "IN",
}

_DIRECT_FIELDS_MAPPER = {
    "uuid": "aggregate_uuid",
    "version": "version",
    "created_at": "created_at",
    "updated_at": "updated_at",
}

_SELECT_MULTIPLE_ENTRIES_QUERY = """
SELECT aggregate_uuid, aggregate_name, version, schema, data, created_at, updated_at
FROM snapshot
WHERE aggregate_name = %(aggregate_name)s
""".strip()
