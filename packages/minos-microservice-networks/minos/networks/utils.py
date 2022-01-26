from __future__ import (
    annotations,
)

import re
import socket
from abc import (
    ABC,
    abstractmethod,
)
from asyncio import (
    QueueEmpty,
)
from typing import (
    Any,
    Generic,
    TypeVar,
)

from minos.common import (
    MinosConfig,
    MinosSetup,
)


def get_host_ip() -> str:
    """Get the host ip.

    :return: A string value.
    """
    name = get_host_name()
    return get_ip(name)


def get_host_name() -> str:
    """Get the host name.

    :return: A string value.
    """
    return re.sub(r"\.(?:local|lan)", "", socket.gethostname())


def get_ip(name: str) -> str:
    """Get the ip address.

    :param name: The name to be converted to an ip.
    :return: A string value.
    """
    return socket.gethostbyname(name)


async def consume_queue(queue, max_count: int) -> None:
    """Consume ``count`` at least ``1`` and at most ``max_count`` elements from the given queue.

    :param queue: The queue to be consumed.
    :param max_count: The max count of elements to be consumed.
    :return: This function does not return anything.
    """
    await queue.get()

    c = 1
    while c < max_count:
        c += 1
        try:
            queue.get_nowait()
        except QueueEmpty:
            break


Instance = TypeVar("Instance")


class Builder(MinosSetup, ABC, Generic[Instance]):  # FIXME: This class should be part of `minos.common` or similar.
    """Builder class."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.kwargs = dict()

    def copy(self: type[B]) -> B:
        """Get a copy of the instance.

        :return: A ``BrokerSubscriberBuilder`` instance.
        """
        return self.new().with_kwargs(self.kwargs)

    @classmethod
    def new(cls: type[B]) -> B:
        """Get a new instance.

        :return: A ``BrokerSubscriberBuilder`` instance.
        """
        return cls()

    def with_kwargs(self: B, kwargs: dict[str, Any]) -> B:
        """Set kwargs.

        :param kwargs: The kwargs to be set.
        :return: This method return the builder instance.
        """
        self.kwargs |= kwargs
        return self

    def with_config(self: B, config: MinosConfig) -> B:
        """Set config.

        :param config: The config to be set.
        :return: This method return the builder instance.
        """
        return self

    @abstractmethod
    def build(self) -> Instance:
        """Build the instance.

        :return: A ``BrokerSubscriber`` instance.
        """


B = TypeVar("B", bound=Builder)
