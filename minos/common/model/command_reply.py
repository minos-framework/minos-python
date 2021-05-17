"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""

from .abc import (
    MinosModel,
)
from .event import (
    Event,
)


class CommandReply(Event):
    """Base Command class."""

    saga_id: str
    task_id: str

    def __init__(self, topic: str, items: list[MinosModel], saga_id: str, task_id: str, *args, **kwargs):
        super().__init__(topic, items, *args, saga_id=saga_id, task_id=task_id, **kwargs)
