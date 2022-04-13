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

from psycopg2.sql import (
    SQL,
    Composable,
    Identifier,
    Literal,
    Placeholder,
)

from minos.aggregate import (
    Action,
    EventDatabaseOperationFactory,
)
from minos.common import (
    ComposedDatabaseOperation,
    DatabaseOperation,
)

from ...clients import (
    AiopgDatabaseClient,
)
from ...operations import (
    AiopgDatabaseOperation,
)


# noinspection SqlNoDataSourceInspection,SqlResolve,PyMethodMayBeStatic
class AiopgEventDatabaseOperationFactory(EventDatabaseOperationFactory):
    """Aiopg Event Database Operation Factory class."""

    def build_table_name(self) -> str:
        """Get the table name.

        :return: A ``str`` value.
        """
        return "aggregate_event"

    def build_create(self) -> DatabaseOperation:
        """Build the database operation to create the event table.

        :return: A ``DatabaseOperation`` instance.s
        """
        return ComposedDatabaseOperation(
            [
                AiopgDatabaseOperation(
                    'CREATE EXTENSION IF NOT EXISTS "uuid-ossp";',
                    lock="uuid-ossp",
                ),
                AiopgDatabaseOperation(
                    """
                    DO
                    $$
                        BEGIN
                            IF NOT EXISTS(SELECT *
                                          FROM pg_type typ
                                                   INNER JOIN pg_namespace nsp
                                                              ON nsp.oid = typ.typnamespace
                                          WHERE nsp.nspname = current_schema()
                                            AND typ.typname = 'action_type') THEN
                                CREATE TYPE action_type AS ENUM ('create', 'update', 'delete');
                            END IF;
                        END;
                    $$
                    LANGUAGE plpgsql;
                    """,
                    lock=self.build_table_name(),
                ),
                AiopgDatabaseOperation(
                    f"""
                    CREATE TABLE IF NOT EXISTS {self.build_table_name()} (
                        id BIGSERIAL PRIMARY KEY,
                        action ACTION_TYPE NOT NULL,
                        uuid UUID NOT NULL,
                        name TEXT NOT NULL,
                        version INT NOT NULL,
                        data BYTEA NOT NULL,
                        created_at TIMESTAMPTZ NOT NULL,
                        transaction_uuid UUID NOT NULL DEFAULT uuid_nil(),
                        UNIQUE (uuid, version, transaction_uuid)
                    );
                    """,
                    lock=self.build_table_name(),
                ),
            ]
        )

    def build_submit(
        self,
        transaction_uuids: Iterable[UUID],
        uuid: UUID,
        action: Action,
        name: str,
        version: int,
        data: bytes,
        created_at: datetime,
        transaction_uuid: UUID,
        lock: Optional[str],
        **kwargs,
    ) -> DatabaseOperation:
        """Build the database operation to submit a row into the event table.

        :param transaction_uuids: The sequence of nested transaction in on top of the current event's transaction.
        :param uuid: The identifier of the entity.
        :param action: The action of the event.
        :param name: The name of the entity.
        :param version: The version of the entity
        :param data: The data of the event.
        :param created_at: The creation datetime.
        :param transaction_uuid: The identifier of the transaction.
        :param lock: The lock identifier.
        :param kwargs: Additional named arguments.
        :return: A ``DatabaseOperation`` instance.
        """
        insert_values = SQL(
            """
            INSERT INTO {table_name} (id, action, uuid, name, version, data, created_at, transaction_uuid)
            VALUES (
                default,
                %(action)s,
                CASE %(uuid)s WHEN uuid_nil() THEN uuid_generate_v4() ELSE %(uuid)s END,
                %(name)s,
                (
                    SELECT (CASE WHEN %(version)s IS NULL THEN 1 + COALESCE(MAX(t2.version), 0) ELSE %(version)s END)
                    FROM (
                             SELECT DISTINCT ON (t1.uuid) t1.version
                             FROM ( {from_parts} ) AS t1
                             ORDER BY t1.uuid, t1.transaction_index DESC
                    ) AS t2
                ),
                %(data)s,
                (CASE WHEN %(created_at)s IS NULL THEN NOW() ELSE %(created_at)s END),
                %(transaction_uuid)s
            )
            RETURNING id, uuid, version, created_at;
            """
        )
        insert_parameters = {
            "uuid": uuid,
            "action": action,
            "name": name,
            "version": version,
            "data": data,
            "created_at": created_at,
            "transaction_uuid": transaction_uuid,
        }

        from_sql, from_parameters = self._build_submit_from(transaction_uuids)

        query = insert_values.format(from_parts=from_sql, table_name=Identifier(self.build_table_name()))
        parameters = from_parameters | insert_parameters

        return AiopgDatabaseOperation(query, parameters, lock)

    def _build_submit_from(self, transaction_uuids: Iterable[UUID]) -> tuple[Composable, dict[str, Any]]:
        select_transaction = SQL(
            """
            SELECT {index} AS transaction_index, uuid, MAX(version) AS version
            FROM {table_name}
            WHERE uuid = %(uuid)s AND transaction_uuid = {transaction_uuid}
            GROUP BY uuid
            """
        )
        from_query_parts = list()
        parameters = dict()
        for index, transaction_uuid in enumerate(transaction_uuids, start=1):
            name = f"transaction_uuid_{index}"
            parameters[name] = transaction_uuid

            from_query_parts.append(
                select_transaction.format(
                    index=Literal(index),
                    transaction_uuid=Placeholder(name),
                    table_name=Identifier(self.build_table_name()),
                ),
            )

        query = SQL(" UNION ALL ").join(from_query_parts)
        return query, parameters

    # noinspection PyShadowingBuiltins
    def build_query(
        self,
        uuid: Optional[UUID] = None,
        name: Optional[str] = None,
        version: Optional[int] = None,
        version_lt: Optional[int] = None,
        version_gt: Optional[int] = None,
        version_le: Optional[int] = None,
        version_ge: Optional[int] = None,
        id: Optional[int] = None,
        id_lt: Optional[int] = None,
        id_gt: Optional[int] = None,
        id_le: Optional[int] = None,
        id_ge: Optional[int] = None,
        transaction_uuid: Optional[UUID] = None,
        transaction_uuid_ne: Optional[UUID] = None,
        transaction_uuid_in: Optional[Iterable[UUID, ...]] = None,
        **kwargs,
    ) -> DatabaseOperation:
        """Build the database operation to select rows.

        :param uuid: The identifier must be equal to the given value.
        :param name: The classname must be equal to the given value.
        :param version: The version must be equal to the given value.
        :param version_lt: The version must be lower than the given value.
        :param version_gt: The version must be greater than the given value.
        :param version_le: The version must be lower or equal to the given value.
        :param version_ge: The version must be greater or equal to the given value.
        :param id: The entry identifier must be equal to the given value.
        :param id_lt: The entry identifier must be lower than the given value.
        :param id_gt: The entry identifier must be greater than the given value.
        :param id_le: The entry identifier must be lower or equal to the given value.
        :param id_ge: The entry identifier must be greater or equal to the given value.
        :param transaction_uuid: The transaction identifier must be equal to the given value.
        :param transaction_uuid_ne: The transaction identifier must be distinct of the given value.
        :param transaction_uuid_in: The destination transaction identifier must be equal to one of the given values.

        :return: A ``DatabaseOperation`` instance.
        """
        if transaction_uuid_in is not None:
            transaction_uuid_in = tuple(transaction_uuid_in)

        _select_all = f"""
            SELECT uuid, name, version, data, id, action, created_at, transaction_uuid
            FROM {self.build_table_name()}
            """

        conditions = list()

        if uuid is not None:
            conditions.append("uuid = %(uuid)s")
        if name is not None:
            conditions.append("name = %(name)s")
        if version is not None:
            conditions.append("version = %(version)s")
        if version_lt is not None:
            conditions.append("version < %(version_lt)s")
        if version_gt is not None:
            conditions.append("version > %(version_gt)s")
        if version_le is not None:
            conditions.append("version <= %(version_le)s")
        if version_ge is not None:
            conditions.append("version >= %(version_ge)s")
        if id is not None:
            conditions.append("id = %(id)s")
        if id_lt is not None:
            conditions.append("id < %(id_lt)s")
        if id_gt is not None:
            conditions.append("id > %(id_gt)s")
        if id_le is not None:
            conditions.append("id <= %(id_le)s")
        if id_ge is not None:
            conditions.append("id >= %(id_ge)s")
        if transaction_uuid is not None:
            conditions.append("transaction_uuid = %(transaction_uuid)s")
        if transaction_uuid_ne is not None:
            conditions.append("transaction_uuid <> %(transaction_uuid_ne)s")
        if transaction_uuid_in is not None:
            conditions.append("transaction_uuid IN %(transaction_uuid_in)s")

        if not conditions:
            return AiopgDatabaseOperation(f"{_select_all} ORDER BY id;")

        return AiopgDatabaseOperation(
            f"{_select_all} WHERE {' AND '.join(conditions)} ORDER BY id;",
            {
                "uuid": uuid,
                "name": name,
                "version": version,
                "version_lt": version_lt,
                "version_gt": version_gt,
                "version_le": version_le,
                "version_ge": version_ge,
                "id": id,
                "id_lt": id_lt,
                "id_gt": id_gt,
                "id_le": id_le,
                "id_ge": id_ge,
                "transaction_uuid": transaction_uuid,
                "transaction_uuid_ne": transaction_uuid_ne,
                "transaction_uuid_in": transaction_uuid_in,
            },
        )

    def build_query_offset(self) -> DatabaseOperation:
        """Build the database operation to get the maximum identifier.

        :return: A ``DatabaseOperation`` instance.
        """
        return AiopgDatabaseOperation(f"SELECT MAX(id) FROM {self.build_table_name()};".strip())


AiopgDatabaseClient.set_factory(EventDatabaseOperationFactory, AiopgEventDatabaseOperationFactory)
