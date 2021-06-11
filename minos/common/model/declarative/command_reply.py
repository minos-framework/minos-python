"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""

from .event import (
    Event,
)


class CommandReply(Event):
    """Base Command class."""

    saga_uuid: str
