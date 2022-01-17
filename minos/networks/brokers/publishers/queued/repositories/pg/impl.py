from .....messages import (
    BrokerMessage,
)
from ..abc import (
    BrokerPublisherRepository,
)


class PostgreSqlBrokerPublisherRepository(BrokerPublisherRepository):
    """PostgreSql Broker Publisher Repository class."""

    async def enqueue(self, message: BrokerMessage) -> None:
        """Enqueue method."""
        # TODO

    async def dequeue(self) -> BrokerMessage:
        """Enqueue method."""
        # TODO
