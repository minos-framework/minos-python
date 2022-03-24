from pathlib import (
    Path,
)

from minos.common import (
    Lock,
    LockPool,
    Port,
)

BASE_PATH = Path(__file__).parent
CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"


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


class FakeLockPool(LockPool):
    """For testing purposes."""

    async def _create_instance(self):
        return FakeLock()

    async def _destroy_instance(self, instance) -> None:
        """For testing purposes."""


class FakePeriodicPort(Port):
    """For testing purposes."""

    async def _start(self) -> None:
        """For testing purposes."""

    async def _stop(self, err: Exception = None) -> None:
        """For testing purposes."""


class FakeHttpPort(Port):
    """For testing purposes."""

    async def _start(self) -> None:
        """For testing purposes."""

    async def _stop(self, err: Exception = None) -> None:
        """For testing purposes."""


class FakeBrokerPort(Port):
    """For testing purposes."""

    async def _start(self) -> None:
        """For testing purposes."""

    async def _stop(self, err: Exception = None) -> None:
        """For testing purposes."""
