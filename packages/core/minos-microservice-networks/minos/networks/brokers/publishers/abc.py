from __future__ import (
    annotations,
)

import logging
from abc import (
    ABC,
    abstractmethod,
)
from typing import (
    TYPE_CHECKING,
    Any,
    Generic,
    Optional,
    TypeVar,
    Union,
)

from minos.common import (
    BuildableMixin,
    Builder,
    Config,
    Injectable,
    MinosConfigException,
)

from ..messages import (
    BrokerMessage,
)

if TYPE_CHECKING:
    from .queued import (
        BrokerPublisherQueue,
        QueuedBrokerPublisher,
    )

logger = logging.getLogger(__name__)


@Injectable("broker_publisher")
class BrokerPublisher(ABC, BuildableMixin):
    """Broker Publisher class."""

    async def send(self, message: BrokerMessage) -> None:
        """Send a message.

        :param message: The message to be sent.
        :return: This method does not return anything.
        """
        logger.debug(f"Sending {message!r} message...")
        await self._send(message)

    @abstractmethod
    async def _send(self, message: BrokerMessage) -> None:
        raise NotImplementedError


BrokerPublisherCls = TypeVar("BrokerPublisherCls", bound=BrokerPublisher)


class BrokerPublisherBuilder(Builder[BrokerPublisher], Generic[BrokerPublisherCls]):
    """Broker Publisher Builder class."""

    def __init__(
        self,
        *args,
        queue_builder: Optional[Builder] = None,
        queued_cls: Optional[type[QueuedBrokerPublisher]] = None,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)

        if queued_cls is None:
            from .queued import (
                QueuedBrokerPublisher,
            )

            queued_cls = QueuedBrokerPublisher

        self.queue_builder = queue_builder

        self.queued_cls = queued_cls

    def with_queued_cls(self, queued_cls: type[QueuedBrokerPublisher]):
        """Set the queued class.

        :param queued_cls: A subclass of ``QueuedBrokerPublisher``.
        :return: This method return the builder instance.
        """
        self.queued_cls = queued_cls

        return self

    def with_config(self, config: Config):
        """Set config.

        :param config: The config to be set.
        :return: This method return the builder instance.
        """
        self._with_builders_from_config(config)

        if self.queue_builder is not None:
            self.queue_builder.with_config(config)
        return super().with_config(config)

    def _with_builders_from_config(self, config):
        try:
            broker_config = config.get_interface_by_name("broker")
        except MinosConfigException:
            return

        broker_publisher_config = broker_config["publisher"]

        if "queue" in broker_publisher_config:
            self.with_queue(broker_publisher_config["queue"])

    def with_queue(self, queue: Union[type[BrokerPublisherQueue], Builder[BrokerPublisherQueue]]):
        """Set the queue builder.

        :param queue: The queue builder to be set.
        :return: This method return the builder instance.
        """
        if not isinstance(queue, Builder):
            queue = queue.get_builder()
        self.queue_builder = queue.copy()
        return self

    def with_kwargs(self, kwargs: dict[str, Any]):
        """Set kwargs.

        :param kwargs: The kwargs to be set.
        :return: This method return the builder instance.
        """
        if self.queue_builder is not None:
            self.queue_builder.with_kwargs(kwargs)

        return super().with_kwargs(kwargs)

    def build(self) -> BrokerPublisher:
        """Build the instance.

        :return: A ``QueuedBrokerSubscriber`` instance.
        """
        impl = super().build()

        if self.queue_builder is not None:
            queue = self.queue_builder.build()
            impl = self.queued_cls(impl=impl, queue=queue, **self.kwargs)

        return impl


BrokerPublisher.set_builder(BrokerPublisherBuilder)
