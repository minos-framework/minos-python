from __future__ import (
    annotations,
)

from datetime import (
    datetime,
)
from typing import (
    TYPE_CHECKING,
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

from .abc import (
    TransactionDatabaseOperationFactory,
)

if TYPE_CHECKING:
    from ....entries import (
        TransactionStatus,
    )


# noinspection SqlNoDataSourceInspection,SqlResolve,PyMethodMayBeStatic
class AiopgTransactionDatabaseOperationFactory(TransactionDatabaseOperationFactory):
    """Aiopg Transaction Database Operation Factory class."""

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
                    DO
                    $$
                        BEGIN
                            IF NOT EXISTS(SELECT *
                                          FROM pg_type typ
                                                   INNER JOIN pg_namespace nsp
                                                              ON nsp.oid = typ.typnamespace
                                          WHERE nsp.nspname = current_schema()
                                            AND typ.typname = 'transaction_status') THEN
                                CREATE TYPE transaction_status AS ENUM (
                                    'pending', 'reserving', 'reserved', 'committing', 'committed', 'rejected'
                                );
                            END IF;
                        END;
                    $$
                    LANGUAGE plpgsql;
                    """,
                    lock="aggregate_transaction_enum",
                ),
                AiopgDatabaseOperation(
                    """
                    CREATE TABLE IF NOT EXISTS aggregate_transaction (
                        uuid UUID PRIMARY KEY,
                        destination_uuid UUID NOT NULL,
                        status TRANSACTION_STATUS NOT NULL,
                        event_offset INTEGER,
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    );
                    """,
                    lock="aggregate_transaction",
                ),
            ]
        )

    def build_submit_row(
        self,
        uuid: UUID,
        destination_uuid: UUID,
        status: TransactionStatus,
        event_offset: int,
    ) -> DatabaseOperation:
        """Build the database operation to submit a row.

        :param uuid: The identifier of the transaction.
        :param destination_uuid: The identifier of the destination transaction.
        :param status: The status of the transaction.
        :param event_offset: The event offset of the transaction.
        :return: A ``DatabaseOperation`` instance.
        """

        params = {
            "uuid": uuid,
            "destination_uuid": destination_uuid,
            "status": status,
            "event_offset": event_offset,
        }

        return AiopgDatabaseOperation(
            """
            INSERT INTO aggregate_transaction AS t (uuid, destination_uuid, status, event_offset)
            VALUES (%(uuid)s, %(destination_uuid)s, %(status)s, %(event_offset)s)
            ON CONFLICT (uuid)
            DO
               UPDATE SET status = %(status)s, event_offset = %(event_offset)s, updated_at = NOW()
            WHERE (t.destination_uuid = %(destination_uuid)s)
              AND (NOT (t.status = 'pending' AND %(status)s NOT IN ('pending', 'reserving', 'rejected')))
              AND (NOT (t.status = 'reserving' AND %(status)s NOT IN ('reserved', 'rejected')))
              AND (NOT (t.status = 'reserved' AND %(status)s NOT IN ('committing', 'rejected')))
              AND (NOT (t.status = 'committing' AND %(status)s NOT IN ('committed')))
              AND (NOT (t.status = 'committed'))
              AND (NOT (t.status = 'rejected'))
            RETURNING updated_at;
            """,
            params,
            lock=uuid.int & (1 << 32) - 1,
        )

    def build_select_rows(
        self,
        uuid: Optional[UUID] = None,
        uuid_ne: Optional[UUID] = None,
        uuid_in: Optional[tuple[UUID]] = None,
        destination_uuid: Optional[UUID] = None,
        status: Optional[str] = None,
        status_in: Optional[tuple[str]] = None,
        event_offset: Optional[int] = None,
        event_offset_lt: Optional[int] = None,
        event_offset_gt: Optional[int] = None,
        event_offset_le: Optional[int] = None,
        event_offset_ge: Optional[int] = None,
        updated_at: Optional[datetime] = None,
        updated_at_lt: Optional[datetime] = None,
        updated_at_gt: Optional[datetime] = None,
        updated_at_le: Optional[datetime] = None,
        updated_at_ge: Optional[datetime] = None,
        **kwargs,
    ) -> DatabaseOperation:
        """Build the database operation to select rows.

        :param uuid: Transaction identifier equal to the given value.
        :param uuid_ne: Transaction identifier not equal to the given value
        :param uuid_in: Transaction identifier within the given values.
        :param destination_uuid: Destination Transaction identifier equal to the given value.
        :param status: Transaction status equal to the given value.
        :param status_in: Transaction status within the given values
        :param event_offset: Event offset equal to the given value.
        :param event_offset_lt: Event Offset lower than the given value
        :param event_offset_gt: Event Offset greater than the given value
        :param event_offset_le: Event Offset lower or equal to the given value
        :param event_offset_ge: Event Offset greater or equal to the given value
        :param updated_at: Updated at equal to the given value.
        :param updated_at_lt: Updated at lower than the given value.
        :param updated_at_gt: Updated at greater than the given value.
        :param updated_at_le: Updated at lower or equal to the given value.
        :param updated_at_ge: Updated at greater or equal to the given value.
        :param kwargs: Additional named arguments.
        :return: A ``DatabaseOperation`` instance.
        """

        conditions = list()

        if uuid is not None:
            conditions.append("uuid = %(uuid)s")
        if uuid_ne is not None:
            conditions.append("uuid <> %(uuid_ne)s")
        if uuid_in is not None:
            conditions.append("uuid IN %(uuid_in)s")
        if destination_uuid is not None:
            conditions.append("destination_uuid = %(destination_uuid)s")
        if status is not None:
            conditions.append("status = %(status)s")
        if status_in is not None:
            conditions.append("status IN %(status_in)s")
        if event_offset is not None:
            conditions.append("event_offset = %(event_offset)s")
        if event_offset_lt is not None:
            conditions.append("event_offset < %(event_offset_lt)s")
        if event_offset_gt is not None:
            conditions.append("event_offset > %(event_offset_gt)s")
        if event_offset_le is not None:
            conditions.append("event_offset <= %(event_offset_le)s")
        if event_offset_ge is not None:
            conditions.append("event_offset >= %(event_offset_ge)s")
        if updated_at is not None:
            conditions.append("updated_at = %(updated_at)s")
        if updated_at_lt is not None:
            conditions.append("updated_at < %(updated_at_lt)s")
        if updated_at_gt is not None:
            conditions.append("updated_at > %(updated_at_gt)s")
        if updated_at_le is not None:
            conditions.append("updated_at <= %(updated_at_le)s")
        if updated_at_ge is not None:
            conditions.append("updated_at >= %(updated_at_ge)s")

        select_all = """
        SELECT uuid, status, event_offset, destination_uuid, updated_at
        FROM aggregate_transaction
        """.strip()

        if not conditions:
            return AiopgDatabaseOperation(f"{select_all} ORDER BY event_offset;")

        return AiopgDatabaseOperation(
            f"{select_all} WHERE {' AND '.join(conditions)} ORDER BY event_offset;",
            {
                "uuid": uuid,
                "uuid_ne": uuid_ne,
                "uuid_in": uuid_in,
                "destination_uuid": destination_uuid,
                "status": status,
                "status_in": status_in,
                "event_offset": event_offset,
                "event_offset_lt": event_offset_lt,
                "event_offset_gt": event_offset_gt,
                "event_offset_le": event_offset_le,
                "event_offset_ge": event_offset_ge,
                "updated_at": updated_at,
                "updated_at_lt": updated_at_lt,
                "updated_at_gt": updated_at_gt,
                "updated_at_le": updated_at_le,
                "updated_at_ge": updated_at_ge,
            },
        )


AiopgDatabaseClient.register_factory(TransactionDatabaseOperationFactory, AiopgTransactionDatabaseOperationFactory)
