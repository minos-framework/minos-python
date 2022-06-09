from __future__ import (
    annotations,
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
    literal,
    select,
)
from sqlalchemy.sql import (
    Executable,
)

from minos.aggregate import (
    IS_REPOSITORY_SERIALIZATION_CONTEXT_VAR,
)
from minos.aggregate.queries import (
    _Condition,
    _EqualCondition,
    _Ordering,
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
        if not (isinstance(self.condition, _EqualCondition)):
            raise ValueError("TODO")

        simplified_name = self.name.rsplit(".", 1)[-1]
        table = self.tables[simplified_name]

        statement = select(table, literal(self.name).label("name")).where(
            table.c[self.condition.field] == self.condition.parameter
        )
        return statement
