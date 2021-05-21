"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from pathlib import (
    Path,
)
from typing import (
    NoReturn,
)

from minos.common import (
    Aggregate,
    CommandReply,
    MinosBroker,
    MinosModel,
    MinosSagaManager,
)

BASE_PATH = Path(__file__).parent


class NaiveAggregate(Aggregate):
    """Naive aggregate class to be used for testing purposes."""

    test: int


class FakeDispatcher:
    """For testing purposes"""

    def __init__(self):
        self.setup_count = 0
        self.setup_dispatch = 0
        self.setup_destroy = 0

    async def setup(self):
        """For testing purposes."""
        self.setup_count += 1

    async def dispatch(self):
        """For testing purposes."""
        self.setup_dispatch += 1

    async def destroy(self):
        """For testing purposes."""
        self.setup_destroy += 1


class FakeSagaManager(MinosSagaManager):
    """For testing purposes."""

    def __init__(self):
        super().__init__()
        self.name = None
        self.reply = None

    def _run_new(self, name: str, **kwargs) -> NoReturn:
        self.name = name

    def _load_and_run(self, reply: CommandReply, **kwargs) -> NoReturn:
        self.reply = reply


class FakeBroker(MinosBroker):
    """For testing purposes."""

    def __init__(self):
        super().__init__()
        self.items = None
        self.topic = None
        self.saga_id = None
        self.task_id = None

    async def send(
        self, items: list[MinosModel], topic: str = None, saga_id: str = None, task_id: str = None, **kwargs
    ) -> NoReturn:
        """For testing purposes."""
        self.items = items
        self.topic = topic
        self.saga_id = saga_id
        self.task_id = task_id
