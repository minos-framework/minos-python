from __future__ import (
    annotations,
)

import logging
from typing import (
    Optional,
)

from ....collections import (
    PostgreSqlBrokerQueue,
    PostgreSqlBrokerQueueQueryFactory,
)
from .abc import (
    BrokerPublisherQueue,
)

logger = logging.getLogger(__name__)


class PostgreSqlBrokerPublisherQueue(PostgreSqlBrokerQueue, BrokerPublisherQueue):
    """PostgreSql Broker Publisher Queue class."""

    def __init__(self, *args, query_factory: Optional[PostgreSqlBrokerQueueQueryFactory] = None, **kwargs):
        if query_factory is None:
            query_factory = PostgreSqlBrokerPublisherQueueQueryFactory()
        super().__init__(*args, query_factory=query_factory, **kwargs)


class PostgreSqlBrokerPublisherQueueQueryFactory(PostgreSqlBrokerQueueQueryFactory):
    """PostgreSql Broker Publisher Queue Query Factory class."""

    def build_table_name(self) -> str:
        """Get the table name.

        :return: A ``str`` value.
        """
        return "broker_publisher_queue"
