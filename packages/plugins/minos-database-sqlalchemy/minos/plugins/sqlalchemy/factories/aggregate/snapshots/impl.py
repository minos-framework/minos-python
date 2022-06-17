from collections.abc import (
    Iterable,
)
from datetime import (
    datetime,
)
from functools import (
    partial,
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
    """SqlAlchemy Snapshot Database Operation Factory class."""

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
        """Build the database operation to create the snapshot table.

        :return: A ``DatabaseOperation`` instance.
        """

        return ComposedDatabaseOperation(
            [
                SqlAlchemyDatabaseOperation(self.offset_metadata.create_all),
                SqlAlchemyDatabaseOperation(self.entities_metadata.create_all),
            ]
        )

    def build_delete(self, transaction_uuids: Iterable[UUID]) -> DatabaseOperation:
        """Build the database operation to delete rows by transaction identifiers.

        :param transaction_uuids: The transaction identifiers.
        :return: A ``DatabaseOperation`` instance.
        """
        transaction_uuids = set(transaction_uuids)

        operations = list()
        for table in self.entities_metadata.tables.values():
            statement = delete(table).filter(table.columns["_transaction_uuid"].in_(transaction_uuids))
            operation = SqlAlchemyDatabaseOperation(statement)
            operations.append(operation)

        return ComposedDatabaseOperation(operations)

    def build_submit(
        self,
        uuid: UUID,
        type_: str,
        version: int,
        schema: Optional[Union[list[dict[str, Any]], dict[str, Any]]],
        data: Optional[dict[str, Any]],
        created_at: datetime,
        updated_at: datetime,
        transaction_uuid: UUID,
    ) -> DatabaseOperation:
        """Build the insert database operation.

        :param uuid: The identifier of the entity.
        :param name: The name of the entity.
        :param version: The version of the entity.
        :param schema: The schema of the entity.
        :param data: The data of the entity.
        :param created_at: The creation datetime.
        :param updated_at: The last update datetime.
        :param transaction_uuid: The transaction identifier.
        :return: A ``DatabaseOperation`` instance.
        """

        simplified_name = type_.rsplit(".", 1)[-1]
        table = self.entities_metadata.tables[simplified_name]

        if data is not None:
            values = data | {
                "uuid": uuid,
                "version": version,
                "created_at": created_at,
                "updated_at": updated_at,
                "_transaction_uuid": transaction_uuid,
            }
        else:
            values = {
                "uuid": uuid,
                "version": version,
                "created_at": created_at,
                "updated_at": updated_at,
                "_transaction_uuid": transaction_uuid,
                "_deleted": True,
            }

        fn = partial(self._submit, table=table, values=values, uuid=uuid, transaction_uuid=transaction_uuid)
        return SqlAlchemyDatabaseOperation(fn)

    @staticmethod
    def _submit(conn: Connection, table: Table, values: dict[str, Any], uuid: UUID, transaction_uuid: UUID) -> Result:
        try:
            statement = insert(table).values(values).returning(table.columns["created_at"], table.columns["updated_at"])
            with conn.begin_nested():
                return conn.execute(statement)
        except IntegrityError:
            statement = (
                update(table)
                .filter(table.columns["uuid"] == uuid, table.columns["_transaction_uuid"] == transaction_uuid)
                .values(values)
                .returning(table.columns["created_at"], table.columns["updated_at"])
            )
            return conn.execute(statement)

    def build_query(
        self,
        type_: str,
        condition: _Condition,
        ordering: Optional[_Ordering],
        limit: Optional[int],
        transaction_uuids: tuple[UUID, ...],
        exclude_deleted: bool,
    ) -> DatabaseOperation:
        """Build the query database operation.

        :param name: Class name of the ``Entity``.
        :param condition: The condition that must be satisfied by the ``Entity`` instances.
        :param ordering: Optional argument to return the instance with specific ordering strategy. The default behaviour
            is to retrieve them without any order pattern.
        :param limit: Optional argument to return only a subset of instances. The default behaviour is to return all the
            instances that meet the given condition.
        :param transaction_uuids: The transaction within the operation is performed. If not any value is provided, then
            the transaction is extracted from the context var. If not any transaction is being scoped then the query is
            performed to the global snapshot.
        :param exclude_deleted: If ``True``, deleted ``Entity`` entries are included, otherwise deleted
            ``Entity`` entries are filtered.
        :return: A ``DatabaseOperation`` instance.
        """

        builder = SqlAlchemySnapshotQueryDatabaseOperationBuilder(
            type_=type_,
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
        """Get a table relative to the given ``Entity``.

        :return: A pre-filtered table as a ``Subquery`` instance.
        """
        builder = SqlAlchemySnapshotQueryDatabaseOperationBuilder(
            type_=name,
            condition=Condition.TRUE,
            transaction_uuids=transaction_uuids,
            exclude_deleted=exclude_deleted,
            tables=self.entities_metadata.tables,
        )
        return builder.get_table()

    def build_submit_offset(self, value: int) -> DatabaseOperation:
        """Build the database operation to store the offset.

        :param value: The value to be stored as the new offset.
        :return: A ``DatabaseOperation`` instance.
        """
        table = self.offset_metadata.tables[self.build_offset_table_name()]

        fn = partial(self._submit_offset, table=table, value=value)
        return SqlAlchemyDatabaseOperation(fn)

    @staticmethod
    def _submit_offset(conn: Connection, table: Table, value: int) -> Result:
        try:
            statement = insert(table).values(id=True, value=value)
            with conn.begin_nested():
                return conn.execute(statement)
        except IntegrityError:
            statement = (
                update(table)
                .filter(table.columns["id"].is_(True))
                .values(
                    {
                        "value": (
                            select(
                                case(
                                    (table.columns["value"] > value, table.columns["value"]),
                                    else_=value,
                                )
                            )
                            .filter(table.columns["id"].is_(True))
                            .scalar_subquery()
                        )
                    }
                )
            )
            return conn.execute(statement)

    def build_query_offset(self) -> DatabaseOperation:
        """Build the database operation to get the current offset.

        :return: A ``DatabaseOperation`` instance.
        """
        table = self.offset_metadata.tables[self.build_offset_table_name()]
        statement = select(table.columns["value"]).filter(table.columns["id"].is_(True))
        return SqlAlchemyDatabaseOperation(statement)


SqlAlchemyDatabaseClient.set_factory(SnapshotDatabaseOperationFactory, SqlAlchemySnapshotDatabaseOperationFactory)
