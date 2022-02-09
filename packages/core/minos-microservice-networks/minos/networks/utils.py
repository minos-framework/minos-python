from __future__ import (
    annotations,
)

import re
import socket
from asyncio import (
    QueueEmpty,
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
