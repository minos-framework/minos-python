from abc import (
    ABC,
)
from collections.abc import (
    Iterable,
)

from minos.common import (
    DatabaseOperation,
)

from ......collections import (
    BrokerQueueDatabaseOperationFactory,
)


# noinspection SqlNoDataSourceInspection,SqlResolve,PyTypeChecker,PyArgumentList
class BrokerSubscriberQueueDatabaseOperationFactory(BrokerQueueDatabaseOperationFactory, ABC):
    """PostgreSql Broker Subscriber Queue Query Factory class."""

    def build_table_name(self) -> str:
        """Get the table name.

        :return: A ``str`` value.
        """
        return "broker_subscriber_queue"

    def build_count_not_processed(
        self,
        retry: int,
        topics: Iterable[str] = tuple(),
        *args,
        **kwargs,
    ) -> DatabaseOperation:
        """Build the "count not processed" query.

        :return:
        """

    def build_select_not_processed(
        self,
        retry: int,
        records: int,
        topics: Iterable[str] = tuple(),
        *args,
        **kwargs,
    ) -> DatabaseOperation:
        """Build the "select not processed" query.

        :return: A ``SQL`` instance.
        """
