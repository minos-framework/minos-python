"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import re
import socket


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
