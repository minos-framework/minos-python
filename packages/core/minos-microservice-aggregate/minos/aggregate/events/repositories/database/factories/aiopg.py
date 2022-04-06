from typing import (
    Optional,
)
from uuid import (
    UUID,
)

from psycopg2.sql import (
    SQL,
    Literal,
    Placeholder,
)

from minos.common import (
    AiopgDatabaseOperation,
    ComposedDatabaseOperation,
    DatabaseOperation,
    datetime,
)

from .....actions import (
    Action,
)
from .abc import (
    EventRepositoryOperationFactory,
)


# noinspection SqlNoDataSourceInspection,SqlResolve,PyMethodMayBeStatic
class AiopgEventRepositoryOperationFactory(EventRepositoryOperationFactory):
    """TODO"""

    def build_create_table(self) -> DatabaseOperation:
        """TODO"""
        return ComposedDatabaseOperation(
            [
                AiopgDatabaseOperation(
                    'CREATE EXTENSION IF NOT EXISTS "uuid-ossp";',
                    lock="aggregate_event",
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
                    lock="aggregate_event",
                ),
                AiopgDatabaseOperation(
                    """
                    CREATE TABLE IF NOT EXISTS aggregate_event (
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
                    lock="aggregate_event",
                ),
            ]
        )

    def build_submit_row(
        self,
        transaction_uuids: tuple[UUID],
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
        """TODO"""
        insert_values = SQL(
            """
            INSERT INTO aggregate_event (id, action, uuid, name, version, data, created_at, transaction_uuid)
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

        select_transaction = SQL(
            """
            SELECT {index} AS transaction_index, uuid, MAX(version) AS version
            FROM aggregate_event
            WHERE uuid = %(uuid)s AND transaction_uuid = {transaction_uuid}
            GROUP BY uuid
            """
        )

        from_query_parts = list()
        parameters = dict()
        for index, transaction_uuid in enumerate(transaction_uuids, start=1):
            transaction_name = f"transaction_uuid_{index}"
            parameters[transaction_name] = transaction_uuid

            from_query_parts.append(
                select_transaction.format(index=Literal(index), transaction_uuid=Placeholder(transaction_name))
            )

        from_query = SQL(" UNION ALL ").join(from_query_parts)

        query = insert_values.format(from_parts=from_query)

        parameters |= {
            "uuid": uuid,
            "action": action,
            "name": name,
            "version": version,
            "data": data,
            "created_at": created_at,
            "transaction_uuid": transaction_uuid,
        }

        return AiopgDatabaseOperation(query, parameters, lock)

    def build_select_rows(
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
        transaction_uuid_in: Optional[tuple[UUID, ...]] = None,
        **kwargs,
    ) -> DatabaseOperation:
        """TODO"""

        _select_all = """
            SELECT uuid, name, version, data, id, action, created_at, transaction_uuid
            FROM aggregate_event
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
            return AiopgDatabaseOperation("{_select_all} ORDER BY id;")

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
                "id_lt": id_lt,
                "id_gt": id_gt,
                "id_le": id_le,
                "id_ge": id_ge,
                "transaction_uuid": transaction_uuid,
                "transaction_uuid_ne": transaction_uuid_ne,
                "transaction_uuid_in": transaction_uuid_in,
            },
        )

    def build_select_max_id(self) -> DatabaseOperation:
        """TODO"""
        return AiopgDatabaseOperation("SELECT MAX(id) FROM aggregate_event;".strip())
