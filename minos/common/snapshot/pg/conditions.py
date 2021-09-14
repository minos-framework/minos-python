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


def build_query(
    aggregate_name: str, condition: Condition, ordering: Optional[str], limit: Optional[int]
) -> tuple[str, dict[str, Any]]:

    parameters = {"aggregate_name": aggregate_name}
    query = f"{_SELECT_MULTIPLE_ENTRIES_QUERY} AND "

    query += _build_query(parameters, condition)

    if ordering is not None:
        query += f" ORDER BY {ordering}"

    if limit is not None:
        query += f" LIMIT {limit}"

    return query, parameters


def _build_query(parameters: dict[str, Any], condition: Condition) -> str:
    if isinstance(condition, ComposedCondition):
        return _build_composed_query(parameters, condition)
    elif isinstance(condition, SimpleCondition):
        return _build_simple_query(parameters, condition)
    elif isinstance(condition, TRUECondition):
        return "TRUE"
    elif isinstance(condition, FALSECondition):
        return "FALSE"
    else:
        raise Exception


def _build_composed_query(parameters: dict[str, Any], condition: ComposedCondition) -> str:
    parts = (_build_query(parameters, c) for c in condition.conditions)
    operator = composed_mapper[condition.operator]

    return "(" + f" {operator} ".join(parts) + ")"


def _build_simple_query(parameters: dict[str, Any], condition: SimpleCondition) -> str:

    field = condition.first.replace(".", ",")
    operator = simple_mapper[condition.operator]

    value = condition.value
    if isinstance(value, (list, tuple, set)):
        value = tuple(value)
        if value == tuple():
            return _build_query(parameters, FALSECondition())

    if field in direct_fields:
        parameters[field] = value
        return f"({direct_fields[field]} {operator} %({field})s)"

    name = str(uuid4())
    parameters[name] = json.dumps(value)
    return f"((data#>'{{{field}}}') {operator} %({name})s::jsonb)"


composed_mapper = {ComposedOperator.AND: "AND", ComposedOperator.OR: "OR"}

simple_mapper = {
    SimpleOperator.LOWER: "<",
    SimpleOperator.LOWER_EQUAL: "<=",
    SimpleOperator.GREATER: ">",
    SimpleOperator.GREATER_EQUAL: ">=",
    SimpleOperator.EQUAL: "=",
    SimpleOperator.NOT_EQUAL: "<>",
    SimpleOperator.IN: "IN",
}

direct_fields = {
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
