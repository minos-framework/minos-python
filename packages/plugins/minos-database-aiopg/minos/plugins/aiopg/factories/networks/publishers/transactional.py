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
from minos.networks import (
    BrokerPublisherTransactionDatabaseOperationFactory,
)

from ....clients import (
    AiopgDatabaseClient,
)
from ....operations import (
    AiopgDatabaseOperation,
)


# noinspection SqlDialectInspection,SqlNoDataSourceInspection
class AiopgBrokerPublisherTransactionDatabaseOperationFactory(BrokerPublisherTransactionDatabaseOperationFactory):
    """TODO"""

    @staticmethod
    def build_table_name() -> str:
        """Build the table name.

        :return: A ``str`` instance.
        """
        return "broker_publisher_transactional_messages"

    def build_create(self) -> DatabaseOperation:
        """TODO"""
        return ComposedDatabaseOperation(
            [
                AiopgDatabaseOperation(
                    SQL('CREATE EXTENSION IF NOT EXISTS "uuid-ossp";'),
                    lock="uuid-ossp",
                ),
                AiopgDatabaseOperation(
                    SQL(
                        f"""
                        CREATE TABLE IF NOT EXISTS {self.build_table_name()} (
                           message BYTEA NOT NULL,
                           transaction_uuid UUID NOT NULL
                        )
                        """
                    ),
                    lock=self.build_table_name(),
                ),
            ]
        )

    def build_query(self, transaction_uuid: UUID) -> DatabaseOperation:
        """TODO"""
        return AiopgDatabaseOperation(
            SQL(
                f"""
                SELECT message, transaction_uuid
                FROM {self.build_table_name()}
                WHERE transaction_uuid =  %(transaction_uuid)s
                """
            ),
            {"transaction_uuid": transaction_uuid},
        )

    def build_submit(self, message: bytes, transaction_uuid: UUID) -> DatabaseOperation:
        """TODO"""
        return AiopgDatabaseOperation(
            SQL(
                f"""
                INSERT INTO {self.build_table_name()}(message, transaction_uuid)
                VALUES(%(message)s, %(transaction_uuid)s)
                """
            ),
            {
                "message": message,
                "transaction_uuid": transaction_uuid,
            },
        )

    def build_delete_batch(self, transaction_uuid: UUID) -> DatabaseOperation:
        """TODO"""
        return AiopgDatabaseOperation(
            SQL(f"DELETE FROM {self.build_table_name()} WHERE transaction_uuid = %(transaction_uuid)s"),
            {"transaction_uuid": transaction_uuid},
        )


AiopgDatabaseClient.set_factory(
    BrokerPublisherTransactionDatabaseOperationFactory, AiopgBrokerPublisherTransactionDatabaseOperationFactory
)
