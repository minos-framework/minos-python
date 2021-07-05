"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from typing import (
    Optional,
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
    reply_topic: Optional[str]
