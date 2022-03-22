from __future__ import (
    annotations,
)

import logging
from abc import (
    ABC,
    abstractmethod,
)
from collections.abc import (
    AsyncIterator,
    Iterable,
)
from typing import (
    TYPE_CHECKING,
    Any,
    Optional,
)

from minos.common import (
    BuildableMixin,
    Builder,
    Config,
    Injectable,
)

from ..messages import (
    BrokerMessage,
)

if TYPE_CHECKING:
    from .idempotent import (
        BrokerSubscriberDuplicateDetectorBuilder,
        IdempotentBrokerSubscriber,
    )
    from .queued import (
        BrokerSubscriberQueueBuilder,
        QueuedBrokerSubscriber,
    )

logger = logging.getLogger(__name__)


class BrokerSubscriber(ABC, BuildableMixin):
    """Broker Subscriber class."""

    def __init__(self, topics: Iterable[str], **kwargs):
        super().__init__(**kwargs)
        self._topics = set(topics)

    @property
    def topics(self) -> set[str]:
        """Topics getter.

        :return: A list of string values.
        """
        return self._topics

    def __aiter__(self) -> AsyncIterator[BrokerMessage]:
        return self

    async def __anext__(self) -> BrokerMessage:
        if self.already_destroyed:
            raise StopAsyncIteration
        return await self.receive()

    async def receive(self) -> BrokerMessage:
        """Receive a new message.

        :return: A ``BrokerMessage`` instance.
        """
        message = await self._receive()
        logger.debug(f"Receiving {message!r} message...")
        return message

    @abstractmethod
    async def _receive(self) -> BrokerMessage:
        raise NotImplementedError


@Injectable("broker_subscriber_builder")
class BrokerSubscriberBuilder(Builder[BrokerSubscriber]):
    """Broker Subscriber Builder class."""

    impl_cls: type[BrokerSubscriber]
    idempotent_cls: type[IdempotentBrokerSubscriber]
    queued_cls: type[QueuedBrokerSubscriber]

    def __init__(
        self,
        *args,
        idempotent_builder: Optional[BrokerSubscriberDuplicateDetectorBuilder] = None,
        queue_builder: Optional[BrokerSubscriberQueueBuilder] = None,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.duplicate_detector_builder = idempotent_builder
        self.queue_builder = queue_builder

    def with_config(self, config: Config):
        """Set config.

        :param config: The config to be set.
        :return: This method return the builder instance.
        """
        broker_config = config.get_interface_by_name("broker")
        broker_subscriber_config = broker_config.get("subscriber", None)
        if broker_subscriber_config is not None and broker_subscriber_config.get("idempotent", None) is not None:
            self.duplicate_detector_builder = (
                broker_subscriber_config.get("idempotent").get_builder().new().with_config(config)
            )
        if broker_subscriber_config is not None and broker_subscriber_config.get("queue", None) is not None:
            self.queue_builder = broker_subscriber_config.get("queue").get_builder().new().with_config(config)
        return super().with_config(config)

    def with_kwargs(self, kwargs: dict[str, Any]):
        """Set kwargs.

        :param kwargs: The kwargs to be set.
        :return: This method return the builder instance.
        """
        if self.duplicate_detector_builder is not None:
            self.duplicate_detector_builder.with_kwargs(kwargs)
        if self.queue_builder is not None:
            self.queue_builder.with_kwargs(kwargs)
        return super().with_kwargs(kwargs)

    def with_group_id(self, group_id: Optional[str]):
        """Set group_id.

        :param group_id: The group_id to be set.
        :return: This method return the builder instance.
        """
        self.kwargs["group_id"] = group_id
        return self

    def with_remove_topics_on_destroy(self, remove_topics_on_destroy: bool):
        """Set remove_topics_on_destroy.

        :param remove_topics_on_destroy: The remove_topics_on_destroy flag to be set.
        :return: This method return the builder instance.
        """
        self.kwargs["remove_topics_on_destroy"] = remove_topics_on_destroy
        return self

    def with_topics(self, topics: Iterable[str]):
        """Set topics.

        :param topics: The topics to be set.
        :return: This method return the builder instance.
        """
        topics = set(topics)
        self.kwargs["topics"] = set(topics)
        if self.queue_builder is not None:
            self.queue_builder.with_topics(topics)
        return self

    def build(self) -> BrokerSubscriber:
        """Build the instance.

        :return: A ``QueuedBrokerSubscriber`` instance.
        """
        impl = self.impl_cls(**self.kwargs)

        if self.duplicate_detector_builder is not None:
            from .idempotent import (
                IdempotentBrokerSubscriber,
            )

            duplicate_detector = self.duplicate_detector_builder.build()
            impl = IdempotentBrokerSubscriber(impl=impl, duplicate_detector=duplicate_detector, **self.kwargs)

        if self.queue_builder is not None:
            from .queued import (
                QueuedBrokerSubscriber,
            )

            queue = self.queue_builder.build()
            impl = QueuedBrokerSubscriber(impl=impl, queue=queue, **self.kwargs)

        return impl


BrokerSubscriber.set_builder(BrokerSubscriberBuilder)
