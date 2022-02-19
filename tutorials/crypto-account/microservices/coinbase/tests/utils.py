from __future__ import (
    annotations,
)

from pathlib import (
    Path,
)

from minos.aggregate import (
    InMemoryEventRepository,
    InMemorySnapshotRepository,
    InMemoryTransactionRepository,
)
from minos.common import (
    DependencyInjector,
    MinosConfig,
)
from minos.networks import (
    InMemoryBrokerPublisher,
    InMemoryBrokerSubscriberBuilder,
)


def build_dependency_injector() -> DependencyInjector:
    """For testing purposes"""

    return DependencyInjector(
        build_config(),
        broker_publisher=InMemoryBrokerPublisher,
        transaction_repository=InMemoryTransactionRepository,
        event_repository=InMemoryEventRepository,
        snapshot_snapshot=InMemorySnapshotRepository,
    )


def build_config() -> MinosConfig:
    """For testing purposes"""

    return MinosConfig(DEFAULT_CONFIG_FILE_PATH)


DEFAULT_CONFIG_FILE_PATH = Path(__file__).parents[1] / "config.yml"
