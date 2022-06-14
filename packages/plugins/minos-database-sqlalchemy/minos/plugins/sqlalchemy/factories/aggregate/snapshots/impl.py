from collections.abc import (
    Iterable,
)
from datetime import (
    datetime,
)
from typing import (
    Any,
    Optional,
    Union,
)
from uuid import (
    UUID,
)

from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    MetaData,
    Table,
    case,
    delete,
    insert,
    select,
    update,
)
from sqlalchemy.engine import (
    Connection,
    Result,
)
from sqlalchemy.exc import (
    IntegrityError,
)
from sqlalchemy.sql import (
    Subquery,
)

from minos.aggregate import (
    SnapshotDatabaseOperationFactory,
)
from minos.aggregate.queries import (
    Condition,
    _Condition,
    _Ordering,
)
from minos.common import (
    ComposedDatabaseOperation,
    Config,
    DatabaseOperation,
    Inject,
)

from ....clients import (
    SqlAlchemyDatabaseClient,
)
from ....operations import (
    SqlAlchemyDatabaseOperation,
)
from .queries import (
    SqlAlchemySnapshotQueryDatabaseOperationBuilder,
)
from .tables import (
    SqlAlchemySnapshotTableFactory,
)


# noinspection SqlNoDataSourceInspection,SqlDialectInspection
class SqlAlchemySnapshotDatabaseOperationFactory(SnapshotDatabaseOperationFactory):
    """TODO"""

    def __init__(
        self, entities_metadata: Optional[MetaData] = None, offset_metadata: Optional[MetaData] = None, **kwargs
    ):
        super().__init__(**kwargs)
        if entities_metadata is None:
            entities_metadata = self._get_entities_metadata(**kwargs)
        if offset_metadata is None:
            offset_metadata = self._get_offset_metadata(**kwargs)

        self.entities_metadata = entities_metadata
        self.offset_metadata = offset_metadata

    @staticmethod
    @Inject()
    def _get_entities_metadata(config: Config, **kwargs) -> MetaData:
        entities = config.get_aggregate()["entities"]
        metadata = SqlAlchemySnapshotTableFactory.build(*entities)
        return metadata

    @classmethod
    def _get_offset_metadata(cls, **kwargs) -> MetaData:
        metadata = MetaData()

        Table(
            cls.build_offset_table_name(),
            metadata,
            Column("id", Boolean(), primary_key=True, default=True, unique=True),
            Column("value", BigInteger(), nullable=False),
        )

        return metadata

    @staticmethod
    def build_offset_table_name() -> str:
        """Get the offset table name.

        :return: A ``str`` value.
        """
        return "aggregate_snapshot_aux_offset"

    def build_create(self) -> DatabaseOperation:
        """TODO"""

        return ComposedDatabaseOperation(
            [
                SqlAlchemyDatabaseOperation(self.offset_metadata.create_all),
                SqlAlchemyDatabaseOperation(self.entities_metadata.create_all),
            ]
        )

    def build_delete(self, transaction_uuids: Iterable[UUID]) -> DatabaseOperation:
        """TODO"""
        transaction_uuids = set(transaction_uuids)

        operations = list()
        for table in self.entities_metadata.tables.values():
            statement = delete(table).filter(table.c.transaction_uuid.in_(transaction_uuids))
            operation = SqlAlchemyDatabaseOperation(statement)
            operations.append(operation)

        return ComposedDatabaseOperation(operations)

    def build_submit(
        self,
        uuid: UUID,
        name: str,
        version: int,
        schema: Optional[Union[list[dict[str, Any]], dict[str, Any]]],
        data: Optional[dict[str, Any]],
        created_at: datetime,
        updated_at: datetime,
        transaction_uuid: UUID,
    ) -> DatabaseOperation:
        """TODO"""

        simplified_name = name.rsplit(".", 1)[-1]
        table = self.entities_metadata.tables[simplified_name]

        if data is not None:
            values = data | {
                "uuid": uuid,
                "version": version,
                "created_at": created_at,
                "updated_at": updated_at,
                "transaction_uuid": transaction_uuid,
            }
        else:
            values = {
                "uuid": uuid,
                "version": version,
                "created_at": created_at,
                "updated_at": updated_at,
                "transaction_uuid": transaction_uuid,
                "deleted": True,
            }

        def _statement(conn: Connection) -> Result:
            try:
                statement = insert(table).values(values).returning(table.c.created_at, table.c.updated_at)
                with conn.begin_nested():
                    return conn.execute(statement)
            except IntegrityError:
                statement = (
                    update(table)
                    .filter(table.c.uuid == uuid, table.c.transaction_uuid == transaction_uuid)
                    .values(values)
                    .returning(table.c.created_at, table.c.updated_at)
                )
                return conn.execute(statement)

        return SqlAlchemyDatabaseOperation(_statement)

    def build_query(
        self,
        name: str,
        condition: _Condition,
        ordering: Optional[_Ordering],
        limit: Optional[int],
        transaction_uuids: tuple[UUID, ...],
        exclude_deleted: bool,
    ) -> DatabaseOperation:
        """TODO"""

        builder = SqlAlchemySnapshotQueryDatabaseOperationBuilder(
            name=name,
            condition=condition,
            ordering=ordering,
            limit=limit,
            transaction_uuids=transaction_uuids,
            exclude_deleted=exclude_deleted,
            tables=self.entities_metadata.tables,
        )
        statement = builder.build()

        operation = SqlAlchemyDatabaseOperation(statement)

        return operation

    def get_table(self, name: str, transaction_uuids: tuple[UUID, ...], exclude_deleted: bool) -> Subquery:
        """TODO"""
        builder = SqlAlchemySnapshotQueryDatabaseOperationBuilder(
            name=name,
            condition=Condition.TRUE,
            transaction_uuids=transaction_uuids,
            exclude_deleted=exclude_deleted,
            tables=self.entities_metadata.tables,
        )
        return builder.get_table()

    def build_submit_offset(self, value: int) -> DatabaseOperation:
        """TODO"""
        table = self.offset_metadata.tables[self.build_offset_table_name()]

        def _statement(conn: Connection) -> Result:
            try:
                statement = insert(table).values(id=True, value=value)
                with conn.begin_nested():
                    return conn.execute(statement)
            except IntegrityError:
                statement = (
                    update(table)
                    .filter(table.c.id.is_(True))
                    .values(
                        {
                            "value": (
                                select(
                                    case(
                                        (table.c.value > value, table.c.value),
                                        else_=value,
                                    )
                                )
                                .filter(table.c.id.is_(True))
                                .scalar_subquery()
                            )
                        }
                    )
                )
                return conn.execute(statement)

        return SqlAlchemyDatabaseOperation(_statement)

    def build_query_offset(self) -> DatabaseOperation:
        """TODO"""
        table = self.offset_metadata.tables[self.build_offset_table_name()]
        statement = select(table.c.value).filter(table.c.id.is_(True))
        return SqlAlchemyDatabaseOperation(statement)


SqlAlchemyDatabaseClient.set_factory(SnapshotDatabaseOperationFactory, SqlAlchemySnapshotDatabaseOperationFactory)
