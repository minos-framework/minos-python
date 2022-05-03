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
    BrokerSubscriberDuplicateValidatorDatabaseOperationFactory,
)

from ....clients import (
    AiopgDatabaseClient,
)
from ....operations import (
    AiopgDatabaseOperation,
)


# noinspection SqlNoDataSourceInspection,SqlResolve
class AiopgBrokerSubscriberDuplicateValidatorDatabaseOperationFactory(
    BrokerSubscriberDuplicateValidatorDatabaseOperationFactory
):
    """Aiopg Broker Subscriber Duplicate Detector Database Operation class."""

    @staticmethod
    def build_table_name() -> str:
        """Build the table name.

        :return: A ``str`` instance.
        """
        return "broker_subscriber_processed_messages"

    def build_create(self) -> DatabaseOperation:
        """Build the "create table" query.

        :return: A ``SQL`` instance.
        """
        return ComposedDatabaseOperation(
            [
                AiopgDatabaseOperation(
                    SQL('CREATE EXTENSION IF NOT EXISTS "uuid-ossp";'),
                    lock="uuid-ossp",
                ),
                AiopgDatabaseOperation(
                    SQL(
                        f"CREATE TABLE IF NOT EXISTS {self.build_table_name()} ("
                        "   topic VARCHAR(255) NOT NULL, "
                        "   uuid UUID NOT NULL, "
                        "   created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),"
                        "   PRIMARY KEY (topic, uuid)"
                        ")"
                    ),
                    lock=self.build_table_name(),
                ),
            ]
        )

    def build_submit(self, topic: str, uuid: UUID) -> DatabaseOperation:
        """Build the "insert row" query.

        :return: A ``SQL`` instance.
        """
        return AiopgDatabaseOperation(
            SQL(f"INSERT INTO {self.build_table_name()}(topic, uuid) VALUES(%(topic)s, %(uuid)s)"),
            {
                "topic": topic,
                "uuid": uuid,
            },
        )


AiopgDatabaseClient.set_factory(
    BrokerSubscriberDuplicateValidatorDatabaseOperationFactory,
    AiopgBrokerSubscriberDuplicateValidatorDatabaseOperationFactory,
)
