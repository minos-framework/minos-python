from collections.abc import (
    Iterable,
)
from datetime import (
    datetime,
)
from typing import (
    Any,
    Optional,
)
from uuid import (
    UUID,
)

from minos.common import (
    AiopgDatabaseClient,
    AiopgDatabaseOperation,
    ComposedDatabaseOperation,
    DatabaseOperation,
)

from .....queries import (
    _Condition,
    _Ordering,
)
from ..abc import (
    SnapshotDatabaseOperationFactory,
)
from .queries import (
    AiopgSnapshotQueryDatabaseOperationBuilder,
)


# noinspection SqlNoDataSourceInspection,SqlResolve
class AiopgSnapshotDatabaseOperationFactory(SnapshotDatabaseOperationFactory):
    """Aiopg Snapshot Database Operation Factory class."""

    def build_create_table(self) -> DatabaseOperation:
        """Build the database operation to create the snapshot table.

        :return: A ``DatabaseOperation`` instance.
        """
        return ComposedDatabaseOperation(
            [
                AiopgDatabaseOperation(
                    'CREATE EXTENSION IF NOT EXISTS "uuid-ossp";',
                    lock="uuid-ossp",
                ),
                AiopgDatabaseOperation(
                    """
                    CREATE TABLE IF NOT EXISTS snapshot (
                        uuid UUID NOT NULL,
                        name TEXT NOT NULL,
                        version INT NOT NULL,
                        schema BYTEA,
                        data JSONB,
                        created_at TIMESTAMPTZ NOT NULL,
                        updated_at TIMESTAMPTZ NOT NULL,
                        transaction_uuid UUID NOT NULL DEFAULT uuid_nil(),
                        PRIMARY KEY (uuid, transaction_uuid)
                    );
                    """,
                    lock="snapshot",
                ),
                AiopgDatabaseOperation(
                    """
                    CREATE TABLE IF NOT EXISTS snapshot_aux_offset (
                        id bool PRIMARY KEY DEFAULT TRUE,
                        value BIGINT NOT NULL,
                        CONSTRAINT id_uni CHECK (id)
                    );
                    """,
                    lock="snapshot_aux_offset",
                ),
            ]
        )

    def build_delete_by_transactions(self, transaction_uuids: Iterable[UUID]) -> DatabaseOperation:
        """Build the database operation to delete rows by transaction identifiers.

        :param transaction_uuids: The transaction identifiers.
        :return: A ``DatabaseOperation`` instance.
        """
        return AiopgDatabaseOperation(
            """
            DELETE FROM snapshot
            WHERE transaction_uuid IN %(transaction_uuids)s;
            """,
            {"transaction_uuids": tuple(transaction_uuids)},
        )

    def build_insert(
        self,
        uuid: UUID,
        name: str,
        version: int,
        schema: bytes,
        data: dict[str, Any],
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

        return AiopgDatabaseOperation(
            """
            INSERT INTO snapshot (uuid, name, version, schema, data, created_at, updated_at, transaction_uuid)
            VALUES (
                %(uuid)s,
                %(name)s,
                %(version)s,
                %(schema)s,
                %(data)s,
                %(created_at)s,
                %(updated_at)s,
                %(transaction_uuid)s
            )
            ON CONFLICT (uuid, transaction_uuid)
            DO
               UPDATE SET version = %(version)s, schema = %(schema)s, data = %(data)s, updated_at = %(updated_at)s
            RETURNING created_at, updated_at;
            """.strip(),
            {
                "uuid": uuid,
                "name": name,
                "version": version,
                "schema": schema,
                "data": data,
                "created_at": created_at,
                "updated_at": updated_at,
                "transaction_uuid": transaction_uuid,
            },
        )

    def build_query(
        self,
        name: str,
        condition: _Condition,
        ordering: Optional[_Ordering],
        limit: Optional[int],
        transaction_uuids: tuple[UUID, ...],
        exclude_deleted: bool,
    ) -> DatabaseOperation:
        """Build the query database operation.

        :param name: Class name of the ``RootEntity``.
        :param condition: The condition that must be satisfied by the ``RootEntity`` instances.
        :param ordering: Optional argument to return the instance with specific ordering strategy. The default behaviour
            is to retrieve them without any order pattern.
        :param limit: Optional argument to return only a subset of instances. The default behaviour is to return all the
            instances that meet the given condition.
        :param transaction_uuids: The transaction within the operation is performed. If not any value is provided, then
            the transaction is extracted from the context var. If not any transaction is being scoped then the query is
            performed to the global snapshot.
        :param exclude_deleted: If ``True``, deleted ``RootEntity`` entries are included, otherwise deleted
            ``RootEntity`` entries are filtered.
        :return: A ``DatabaseOperation`` instance.
        """
        builder = AiopgSnapshotQueryDatabaseOperationBuilder(
            name, condition, ordering, limit, transaction_uuids, exclude_deleted
        )
        query, parameters = builder.build()

        return AiopgDatabaseOperation(query, parameters)

    def build_store_offset(self, value: int) -> DatabaseOperation:
        """Build the database operation to store the offset.

        :param value: The value to be stored as the new offset.
        :return: A ``DatabaseOperation`` instance.
        """
        return AiopgDatabaseOperation(
            """
            INSERT INTO snapshot_aux_offset (id, value)
            VALUES (TRUE, %(value)s)
            ON CONFLICT (id)
            DO UPDATE SET value = GREATEST(%(value)s, (SELECT value FROM snapshot_aux_offset WHERE id = TRUE));
            """.strip(),
            {"value": value},
            lock="insert_snapshot_aux_offset",
        )

    def build_get_offset(self) -> DatabaseOperation:
        """Build the database operation to get the current offset.

        :return: A ``DatabaseOperation`` instance.
        """
        return AiopgDatabaseOperation(
            """
            SELECT value
            FROM snapshot_aux_offset
            WHERE id = TRUE;
            """
        )


AiopgDatabaseClient.register_factory(SnapshotDatabaseOperationFactory, AiopgSnapshotDatabaseOperationFactory)
