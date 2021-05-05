"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""

from minos.common.model import (
    Aggregate,
)

from .event import (
    Event,
)


class Command(Event):
    """Base Command class."""

    reply_on: str

    def __init__(self, topic: str, items: list[Aggregate], reply_on: str, *args, **kwargs):
        super().__init__(topic, items, *args, reply_on=reply_on, **kwargs)
