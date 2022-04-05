from pathlib import (
    Path,
)

from minos.common import (
    BuildableMixin,
    Builder,
    Injectable,
    Lock,
    LockPool,
    Port,
)
from minos.common.testing import (
    MinosTestCase,
)

BASE_PATH = Path(__file__).parent
CONFIG_FILE_PATH = BASE_PATH / "config" / "v2.yml"


class CommonTestCase(MinosTestCase):
    def get_config_file_path(self) -> Path:
        return CONFIG_FILE_PATH


class FakeEntrypoint:
    """For testing purposes."""

    def __init__(self, *args, **kwargs):
        """For testing purposes."""

    async def __aenter__(self):
        """For testing purposes."""

    async def __aexit__(self, exc_type, exc_val, exc_tb):
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

    async def acquire(self) -> None:
        """For testing purposes."""

    async def release(self):
        """For testing purposes."""


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


@Injectable("custom")
class FakeCustomInjection:
    """For testing purposes."""


@Injectable("serializer")
class FakeSerializer:
    """For testing purposes."""


@Injectable("http_connector")
class FakeHttpConnector:
    """For testing purposes."""


@Injectable("broker_publisher")
class FakeBrokerPublisher(BuildableMixin):
    """For testing purposes."""


class FakeBrokerPublisherBuilder(Builder[FakeBrokerPublisher]):
    """For testing purposes."""


FakeBrokerPublisher.set_builder(FakeBrokerPublisherBuilder)


class FakeBrokerSubscriber(BuildableMixin):
    """For testing purposes."""


@Injectable("broker_subscriber_builder")
class FakeBrokerSubscriberBuilder(Builder[FakeBrokerSubscriber]):
    """For testing purposes."""


FakeBrokerSubscriber.set_builder(FakeBrokerSubscriberBuilder)


@Injectable("database_pool")
class FakeDatabasePool:
    """For testing purposes."""


@Injectable("broker_pool")
class FakeBrokerClientPool:
    """For testing purposes."""


@Injectable("discovery_connector")
class FakeDiscoveryConnector:
    """For testing purposes."""


@Injectable("saga_manager")
class FakeSagaManager:
    """For testing purposes."""


@Injectable("event_repository")
class FakeEventRepository:
    """For testing purposes."""


@Injectable("snapshot_repository")
class FakeSnapshotRepository:
    """For testing purposes."""


@Injectable("transaction_repository")
class FakeTransactionRepository:
    """For testing purposes."""
