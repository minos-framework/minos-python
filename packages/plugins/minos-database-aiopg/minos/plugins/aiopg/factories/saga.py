from __future__ import (
    annotations,
)

import json
from typing import (
    Any,
    Optional,
)
from uuid import (
    UUID,
)

from psycopg2.sql import (
    SQL,
)

from minos.common import (
    ComposedDatabaseOperation,
    DatabaseOperation,
)
from minos.saga import (
    SagaExecutionDatabaseOperationFactory,
)

from ..clients import (
    AiopgDatabaseClient,
)
from ..operations import (
    AiopgDatabaseOperation,
)


# noinspection SqlNoDataSourceInspection,SqlResolve,PyMethodMayBeStatic,SqlDialectInspection
class AiopgSagaExecutionDatabaseOperationFactory(SagaExecutionDatabaseOperationFactory):
    """Aiopg Saga Execution Database Operation Factory class."""

    def build_table_name(self) -> str:
        """Get the table name.

        :return: A ``str`` value.
        """
        return "saga_execution"

    def build_create(self) -> DatabaseOperation:
        """Build the database operation to create the delta table.

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
                                            AND typ.typname = 'saga_execution_status_type') THEN
                                CREATE TYPE saga_execution_status_type AS ENUM (
                                    'created', 'running', 'paused', 'finished', 'errored'
                                );
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
                        "uuid" UUID PRIMARY KEY,
                        "definition" JSONB NOT NULL,
                        "status" saga_execution_status_type NOT NULL,
                        "executed_steps" JSONB NOT NULL,
                        "paused_step" JSONB NOT NULL,
                        "context" TEXT NOT NULL,
                        "already_rollback" BOOL NOT NULL,
                        "user" UUID
                    );
                    """,
                    lock=self.build_table_name(),
                ),
            ]
        )

    def build_store(
        self,
        uuid: UUID,
        definition: dict[str, Any],
        status: str,
        executed_steps: list[dict[str, Any]],
        paused_step: Optional[dict[str, Any]],
        context: str,
        already_rollback: bool,
        user: Optional[UUID],
        **kwargs,
    ) -> DatabaseOperation:
        """Build the database operation to store a saga execution.

        :param uuid: The identifier of the saga execution.
        :param definition: The ``Saga`` definition.
        :param context: The execution context.
        :param status: The status of the execution.
        :param executed_steps: The executed steps of the execution.
        :param paused_step: The paused step of the execution.
        :param already_rollback: ``True`` if already rollback of ``False`` otherwise.
        :param user: The user that launched the execution.
        :param kwargs: The attributes of the saga execution.
        :return: A ``DatabaseOperation`` instance.
        """
        query = SQL(
            f"""
            INSERT INTO {self.build_table_name()} (
                "uuid", "definition", "status", "executed_steps", "paused_step", "context", "already_rollback", "user"
            )
            VALUES (
                %(uuid)s,
                %(definition)s,
                %(status)s,
                %(executed_steps)s,
                %(paused_step)s,
                %(context)s,
                %(already_rollback)s,
                %(user)s
            )
            ON CONFLICT (uuid)
            DO
               UPDATE SET
                "definition" = %(definition)s,
                "status" = %(status)s,
                "executed_steps" = %(executed_steps)s,
                "paused_step" = %(paused_step)s,
                "context" = %(context)s,
                "already_rollback" = %(already_rollback)s,
                "user" = %(user)s
            ;
            """
        )
        parameters = {
            "uuid": uuid,
            "definition": json.dumps(definition),
            "status": status,
            "executed_steps": json.dumps(executed_steps),
            "paused_step": json.dumps(paused_step),
            "context": context,
            "already_rollback": already_rollback,
            "user": user,
        }
        return AiopgDatabaseOperation(query, parameters)

    def build_load(self, uuid: UUID) -> DatabaseOperation:
        """Build the database operation to load a saga execution.

        :param uuid: The identifier of the saga execution.
        :return: A ``DatabaseOperation`` instance.
        """
        return AiopgDatabaseOperation(
            SQL(f"SELECT * FROM {self.build_table_name()} WHERE uuid = %(uuid)s"), {"uuid": uuid}
        )

    def build_delete(self, uuid: UUID) -> DatabaseOperation:
        """Build the database operation to delete a saga execution.

        :param uuid: The identifier of the saga execution.
        :return: A ``DatabaseOperation`` instance.
        """
        return AiopgDatabaseOperation(
            SQL(f"DELETE FROM {self.build_table_name()} WHERE uuid = %(uuid)s"),
            {"uuid": uuid},
        )


AiopgDatabaseClient.set_factory(SagaExecutionDatabaseOperationFactory, AiopgSagaExecutionDatabaseOperationFactory)
