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
    from .filtered import (
        BrokerSubscriberValidator,
        FilteredBrokerSubscriber,
    )
    from .queued import (
        BrokerSubscriberQueue,
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


BrokerSubscriberCls = TypeVar("BrokerSubscriberCls", bound=BrokerSubscriber)


@Injectable("broker_subscriber_builder")
class BrokerSubscriberBuilder(Builder[BrokerSubscriberCls], Generic[BrokerSubscriberCls]):
    """Broker Subscriber Builder class."""

    def __init__(
        self,
        *args,
        validator_builder: Optional[Builder] = None,
        queue_builder: Optional[BrokerSubscriberQueueBuilder] = None,
        filtered_cls: Optional[type[FilteredBrokerSubscriber]] = None,
        queued_cls: Optional[type[QueuedBrokerSubscriber]] = None,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)

        if filtered_cls is None:
            from .filtered import (
                FilteredBrokerSubscriber,
            )

            filtered_cls = FilteredBrokerSubscriber

        if queued_cls is None:
            from .queued import (
                QueuedBrokerSubscriber,
            )

            queued_cls = QueuedBrokerSubscriber

        self.validator_builder = validator_builder
        self.queue_builder = queue_builder

        self.filtered_cls = filtered_cls
        self.queued_cls = queued_cls

    def with_filtered_cls(self, filtered_cls: type[FilteredBrokerSubscriber]):
        """Set the filtered class.

        :param filtered_cls: A subclass of ``FilteredBrokerSubscriber``.
        :return: This method return the builder instance.
        """
        self.filtered_cls = filtered_cls

        return self

    def with_queued_cls(self, queued_cls: type[QueuedBrokerSubscriber]):
        """Set the queued class.

        :param queued_cls: A subclass of ``QueuedBrokerSubscriber``.
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

        if self.validator_builder is not None:
            self.validator_builder.with_config(config)
        if self.queue_builder is not None:
            self.queue_builder.with_config(config)
        return super().with_config(config)

    def _with_builders_from_config(self, config):
        try:
            broker_config = config.get_interface_by_name("broker")
        except MinosConfigException:
            return

        broker_subscriber_config = broker_config["subscriber"]

        if "validator" in broker_subscriber_config:
            self.with_validator(broker_subscriber_config["validator"])

        if "queue" in broker_subscriber_config:
            self.with_queue(broker_subscriber_config["queue"])

    def with_validator(
        self,
        validator: Union[type[BrokerSubscriberValidator], Builder[BrokerSubscriberValidator]],
    ):
        """Set the duplicate detector.

        :param validator: The duplicate detector to be set.
        :return: This method return the builder instance.
        """
        if not isinstance(validator, Builder):
            validator = validator.get_builder()
        self.validator_builder = validator.copy()
        return self

    def with_queue(self, queue: Union[type[BrokerSubscriberQueue], BrokerSubscriberQueueBuilder]):
        """Set the queue builder.

        :param queue: The queue to be set.
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
        if self.validator_builder is not None:
            self.validator_builder.with_kwargs(kwargs)

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
        impl = super().build()

        if self.validator_builder is not None:
            validator = self.validator_builder.build()
            impl = self.filtered_cls(impl=impl, validator=validator, **self.kwargs)

        if self.queue_builder is not None:
            queue = self.queue_builder.build()
            impl = self.queued_cls(impl=impl, queue=queue, **self.kwargs)

        return impl


BrokerSubscriber.set_builder(BrokerSubscriberBuilder)
