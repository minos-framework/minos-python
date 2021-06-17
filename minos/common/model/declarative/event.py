"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""

from __future__ import (
    annotations,
)

from ..abc import (
    Model,
)
from .abc import (
    DeclarativeModel,
)


class Event(DeclarativeModel):
    """Base Event class."""

    topic: str
    items: list[Model]
