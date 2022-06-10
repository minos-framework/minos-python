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
    """SqlAlchemy Snapshot Query Database Operation Builder class.

    This class build postgres-compatible database queries over fields based on a condition, ordering, etc.
    """

    def __init__(
        self,
        tables: dict[str, Table],
        name: str,
        condition: _Condition,
        ordering: Optional[_Ordering] = None,
        limit: Optional[int] = None,
        transaction_uuids: Iterable[UUID, ...] = (NULL_UUID,),
        exclude_deleted: bool = False,
    ):
        if not isinstance(transaction_uuids, tuple):
            transaction_uuids = tuple(transaction_uuids)
        self.name = name
        self.condition = condition
        self.ordering = ordering
        self.limit = limit
        self.transaction_uuids = transaction_uuids
        self.exclude_deleted = exclude_deleted
        self.tables = tables

    def build(self) -> Executable:
        """Build a query.

        :return: A tuple in which the first value is the sql sentence and the second one is a dictionary containing the
            query parameters.
        """

        token = IS_REPOSITORY_SERIALIZATION_CONTEXT_VAR.set(True)
        try:
            query = self._build()
        finally:
            IS_REPOSITORY_SERIALIZATION_CONTEXT_VAR.reset(token)

        return query

    def _build(self) -> Executable:
        # if not (isinstance(self.condition, _EqualCondition)):
        #     raise ValueError("TODO")

        table = self._build_select_from()

        statement = select(table)

        statement = statement.filter(self._build_condition(self.condition, table))
        # statement = statement.filter(table.c[self.condition.field] == self.condition.parameter)

        if self.exclude_deleted:
            statement = statement.filter(table.c.deleted.is_(False))

        if self.ordering is not None:
            statement = statement.order_by(self._build_ordering(self.ordering, table))

        if self.limit is not None:
            statement = statement.limit(self._build_limit(self.limit, table))

        return statement

    def _build_select_from(self) -> Subquery:

        simplified_name = self.name.rsplit(".", 1)[-1]
        table = self.tables[simplified_name]

        parts = list()
        for index, transaction_uuid in enumerate(self.transaction_uuids, start=1):
            part = select(table, literal(index).label("transaction_index")).filter(
                table.c.transaction_uuid == transaction_uuid
            )
            parts.append(part)

        union = union_all(*parts).subquery()
        statement = (
            select(union, literal(self.name).label("name"))
            .order_by(union.c.uuid, desc(union.c.transaction_index))
            .distinct(union.c.uuid)
        ).subquery()
        return statement

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
