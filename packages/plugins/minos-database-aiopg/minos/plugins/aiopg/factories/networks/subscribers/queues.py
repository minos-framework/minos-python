from collections.abc import (
    Iterable,
)

from psycopg2.sql import (
    SQL,
)

from minos.common import (
    DatabaseOperation,
)
from minos.networks import (
    BrokerSubscriberQueueDatabaseOperationFactory,
)

from ....clients import (
    AiopgDatabaseClient,
)
from ....operations import (
    AiopgDatabaseOperation,
)
from ..collections import (
    AiopgBrokerQueueDatabaseOperationFactory,
)


# noinspection SqlNoDataSourceInspection,SqlResolve,PyTypeChecker,PyArgumentList
class AiopgBrokerSubscriberQueueDatabaseOperationFactory(
    BrokerSubscriberQueueDatabaseOperationFactory, AiopgBrokerQueueDatabaseOperationFactory
):
    """Aiopg Broker Subscriber Queue Database Operation Factory class."""

    def build_table_name(self) -> str:
        """Get the table name.

        :return: A ``str`` value.
        """
        return "broker_subscriber_queue"

    def build_count(self, retry: int, topics: Iterable[str] = tuple(), *args, **kwargs) -> DatabaseOperation:
        """Build the "count not processed" query.

        :return:
        """
        return AiopgDatabaseOperation(
            SQL(
                f"SELECT COUNT(*) FROM (SELECT id FROM {self.build_table_name()} "
                "WHERE NOT processing AND retry < %(retry)s AND topic IN %(topics)s FOR UPDATE SKIP LOCKED) s"
            ),
            {"retry": retry, "topics": tuple(topics)},
        )

    def build_query(
        self, retry: int, records: int, topics: Iterable[str] = tuple(), *args, **kwargs
    ) -> DatabaseOperation:
        """Build the "select not processed" query.

        :return: A ``SQL`` instance.
        """
        return AiopgDatabaseOperation(
            SQL(
                "SELECT id, data "
                f"FROM {self.build_table_name()} "
                "WHERE NOT processing AND retry < %(retry)s AND topic IN %(topics)s "
                "ORDER BY created_at "
                "LIMIT %(records)s "
                "FOR UPDATE SKIP LOCKED"
            ),
            {"retry": retry, "topics": tuple(topics), "records": records},
        )


AiopgDatabaseClient.set_factory(
    BrokerSubscriberQueueDatabaseOperationFactory,
    AiopgBrokerSubscriberQueueDatabaseOperationFactory,
)
