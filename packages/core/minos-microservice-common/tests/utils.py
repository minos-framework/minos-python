import sys
import unittest
from pathlib import (
    Path,
)

from dependency_injector import (
    containers,
    providers,
)

from minos.common import (
    Lock,
    MinosPool,
)

BASE_PATH = Path(__file__).parent


class MinosTestCase(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        super().setUp()

        self.lock_pool = FakeLockPool()
        self.container = containers.DynamicContainer()
        self.container.lock_pool = providers.Object(self.lock_pool)
        self.container.wire(modules=[sys.modules[__name__]])

    async def asyncSetUp(self):
        await super().asyncSetUp()

        await self.lock_pool.setup()

    async def asyncTearDown(self):
        await self.lock_pool.destroy()

        await super().asyncTearDown()

    def tearDown(self) -> None:
        self.container.unwire()
        super().tearDown()


class FakeEntrypoint:
    """For testing purposes."""

    def __init__(self, *args, **kwargs):
        """For testing purposes."""

    async def __aenter__(self):
        """For testing purposes."""

    async def graceful_shutdown(*args, **kwargs):
        """For testing purposes."""


class FakeLoop:
    """For testing purposes."""

    def __init__(self):
        """For testing purposes."""

    def run_forever(self):
        """For testing purposes."""

    def run_until_complete(self, *args, **kwargs):
        """For testing purposes."""


class FakeAsyncIterator:
    """For testing purposes."""

    def __init__(self, seq):
        self.iter = iter(seq)

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return next(self.iter)
        except StopIteration:
            raise StopAsyncIteration


class FakeLock(Lock):
    """For testing purposes."""

    def __init__(self, key=None, *args, **kwargs):
        if key is None:
            key = "fake"
        super().__init__(key, *args, **kwargs)

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return


class FakeLockPool(MinosPool):
    """For testing purposes."""

    async def _create_instance(self):
        return FakeLock()

    async def _destroy_instance(self, instance) -> None:
        """For testing purposes."""
