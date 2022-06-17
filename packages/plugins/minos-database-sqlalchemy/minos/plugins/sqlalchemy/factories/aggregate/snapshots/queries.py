from __future__ import (
    annotations,
)

from operator import (
    eq,
    ge,
    gt,
    le,
    lt,
    ne,
)
from typing import (
    Iterable,
    Optional,
)
from uuid import (
    UUID,
)

from sqlalchemy import (
    Table,
    and_,
    desc,
    literal,
    not_,
    or_,
    select,
    union_all,
)
from sqlalchemy.sql import (
    ClauseElement,
    Executable,
    False_,
    Subquery,
    True_,
)
from sqlalchemy.sql.operators import (
    contains_op,
    in_op,
    like_op,
)

from minos.aggregate import (
    IS_REPOSITORY_SERIALIZATION_CONTEXT_VAR,
)
# noinspection PyProtectedMember
from minos.aggregate.queries import (
    _FALSE_CONDITION,
    _AndCondition,
    _ComposedCondition,
    _Condition,
    _ContainsCondition,
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
)


# noinspection SqlResolve,SqlNoDataSourceInspection
class SqlAlchemySnapshotQueryDatabaseOperationBuilder:
    """SqlAlchemy Snapshot Query Database Operation Builder class."""

    def __init__(
        self,
        tables: dict[str, Table],
        type_: str,
        condition: Optional[_Condition] = None,
        ordering: Optional[_Ordering] = None,
        limit: Optional[int] = None,
        transaction_uuids: Iterable[UUID, ...] = (NULL_UUID,),
        exclude_deleted: bool = False,
    ):
        if not isinstance(transaction_uuids, tuple):
            transaction_uuids = tuple(transaction_uuids)
        self.type_ = type_
        self.condition = condition
        self.ordering = ordering
        self.limit = limit
        self.transaction_uuids = transaction_uuids
        self.exclude_deleted = exclude_deleted
        self.tables = tables

    def build(self) -> Executable:
        """Build a query.

        :return: An ``Executable`` instance..
        """

        token = IS_REPOSITORY_SERIALIZATION_CONTEXT_VAR.set(True)
        try:
            query = self._build()
        finally:
            IS_REPOSITORY_SERIALIZATION_CONTEXT_VAR.reset(token)

        return query

    def _build(self) -> Executable:
        table = self.get_table()

        statement = select(table, literal(self.type_).label("_type"))

        if self.condition is not None:
            statement = statement.filter(self._build_condition(self.condition, table))

        if self.ordering is not None:
            statement = statement.order_by(self._build_ordering(self.ordering, table))

        if self.limit is not None:
            statement = statement.limit(self._build_limit(self.limit, table))

        return statement

    def get_table(self) -> Subquery:
        """Get a table relative to the given ``Entity``.

        :return: A pre-filtered table as a ``Subquery`` instance.
        """

        simplified_name = self.type_.rsplit(".", 1)[-1]
        table = self.tables[simplified_name]

        parts = list()
        for index, uuid in enumerate(self.transaction_uuids, start=1):
            part = select(table, literal(index).label("_index")).filter(table.columns["_transaction_uuid"] == uuid)
            parts.append(part)

        union = union_all(*parts).subquery()
        statement = (
            select(union).order_by(union.columns["uuid"], desc(union.columns["_index"])).distinct(union.columns["uuid"])
        )

        if self.exclude_deleted:
            subquery = statement.subquery()
            statement = select(subquery).filter(subquery.columns["_deleted"].is_(False))

        return statement.subquery()

    def _build_condition(self, condition: _Condition, table: Subquery) -> ClauseElement:
        if isinstance(condition, _NotCondition):
            return self._build_condition_not(condition, table)
        if isinstance(condition, _ComposedCondition):
            return self._build_condition_composed(condition, table)
        if isinstance(condition, _TrueCondition):
            return True_()
        if isinstance(condition, _FalseCondition):
            return False_()
        if isinstance(condition, _SimpleCondition):
            return self._build_condition_simple(condition, table)

        raise ValueError(f"Given condition is not supported. Obtained: {condition}")

    def _build_condition_not(self, condition: _NotCondition, table: Subquery) -> ClauseElement:
        return not_(self._build_condition(condition.inner, table))

    def _build_condition_composed(self, condition: _ComposedCondition, table: Subquery) -> ClauseElement:
        # noinspection PyTypeChecker
        operator = self._COMPOSED_MAPPER[type(condition)]
        parts = (self._build_condition(c, table) for c in condition)
        return operator(*parts)

    def _build_condition_simple(self, condition: _SimpleCondition, table: Subquery) -> ClauseElement:
        field = condition.field
        # noinspection PyTypeChecker
        operator = self._SIMPLE_MAPPER[type(condition)]

        parameter = condition.parameter
        if isinstance(parameter, list):
            if not len(parameter):
                return self._build_condition(_FALSE_CONDITION, table)
            parameter = tuple(parameter)

        return operator(table.columns[field], parameter)

    @staticmethod
    def _build_ordering(ordering: _Ordering, table: Subquery) -> ClauseElement:
        clause = table.columns[ordering.by]

        if ordering.reverse:
            clause = desc(clause)

        return clause

    # noinspection PyUnusedLocal
    @staticmethod
    def _build_limit(value: int, table: Subquery) -> select:
        return value

    _COMPOSED_MAPPER = {
        _AndCondition: and_,
        _OrCondition: or_,
    }

    _SIMPLE_MAPPER = {
        _LowerCondition: lt,
        _LowerEqualCondition: le,
        _GreaterCondition: gt,
        _GreaterEqualCondition: ge,
        _EqualCondition: eq,
        _NotEqualCondition: ne,
        _InCondition: in_op,
        _ContainsCondition: contains_op,
        _LikeCondition: like_op,
    }
