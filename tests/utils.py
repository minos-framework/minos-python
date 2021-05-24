"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from collections import (
    namedtuple,
)
from pathlib import (
    Path,
)

from minos.common import (
    Aggregate,
)

BASE_PATH = Path(__file__).parent


class NaiveAggregate(Aggregate):
    """Naive aggregate class to be used for testing purposes."""

    test: int


Message = namedtuple("Message", ["topic", "partition", "value"])


class FakeConsumer:
    """For testing purposes."""

    def __init__(self):
        self.i = 0
        self.length = 3

    async def start(self):
        """For testing purposes."""

    async def stop(self):
        """For testing purposes."""

    async def __aiter__(self):
        yield Message(topic="TicketAdded", partition=0, value=bytes())


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
