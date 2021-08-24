"""minos.networks.utils module."""

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


async def consume_queue(queue, count: int) -> None:
    """TODO

    :param queue: TODO
    :param count: TODO
    :return: TODO
    """
    await queue.get()

    c = 1
    while c < count:
        c += 1
        try:
            queue.get_nowait()
        except QueueEmpty:
            break
