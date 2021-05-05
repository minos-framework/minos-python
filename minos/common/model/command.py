"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""

from minos.common.model import (
    Aggregate,
)

from .command_reply import (
    CommandReply,
)


class Command(CommandReply):
    """Base Command class."""

    reply_on: str

    def __init__(self, topic: str, items: list[Aggregate], saga_id: str, task_id: str, reply_on: str, *args, **kwargs):
        super().__init__(topic, items, *args, saga_id=saga_id, task_id=task_id, reply_on=reply_on, **kwargs)
