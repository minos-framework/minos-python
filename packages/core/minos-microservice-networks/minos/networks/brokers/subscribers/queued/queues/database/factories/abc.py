from abc import (
    ABC,
    abstractmethod,
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
    """Broker Subscriber Queue Database Operation Factory class."""

    @abstractmethod
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

    @abstractmethod
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
