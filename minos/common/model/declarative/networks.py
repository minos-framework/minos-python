"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""

from __future__ import (
    annotations,
)

from enum import (
    IntEnum,
)

from ..abc import (
    Model,
)
from .abc import (
    DeclarativeModel,
)


class Command(DeclarativeModel):
    """Base Command class."""

    topic: str
    items: list[Model]
    saga_uuid: str
    reply_topic: str


class CommandReply(DeclarativeModel):
    """Base Command class."""

    topic: str
    items: list[Model]
    saga_uuid: str
    status: CommandStatus


class CommandStatus(IntEnum):
    """Command Status class."""

    SUCCESS = 200
    ERROR = 400
    SYSTEM_ERROR = 500


class Event(DeclarativeModel):
    """Base Event class."""

    topic: str
    items: list[Model]
