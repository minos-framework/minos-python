"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import re
import socket


def get_ip_address() -> str:
    """Get the host ip address.

    :return: A string value.
    """
    hostname = re.sub(r"\.(?:local|lan)", "", socket.getfqdn().rstrip(".local"))
    return socket.gethostbyname(hostname)
