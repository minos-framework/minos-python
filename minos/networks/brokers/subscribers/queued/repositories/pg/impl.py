import logging

from minos.networks import (
    BrokerMessage,
)

from ..abc import (
    BrokerSubscriberRepository,
)

logger = logging.getLogger(__name__)


class PostgreSqlBrokerSubscriberRepository(BrokerSubscriberRepository):
    """TODO"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    async def enqueue(self, message: BrokerMessage) -> None:
        """TODO

        :param message: TODO
        :return: TODO
        """
        raise NotImplementedError

    async def dequeue(self) -> BrokerMessage:
        """TODO

        :return: TODO
        """
        raise NotImplementedError
