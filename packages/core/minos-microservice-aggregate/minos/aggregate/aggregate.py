from __future__ import (
    annotations,
)

from typing import (
    Generic,
    TypeVar,
    get_args,
)

from minos.common import (
    Config,
    Inject,
    NotProvidedException,
    SetupMixin, Injectable,
)
from minos.transactions import (
    TransactionRepository,
)

from .entities import (
    RootEntity,
)
from .events import (
    EventRepository,
)
from .snapshots import (
    SnapshotRepository,
)
from minos.networks import (
    TransactionalBrokerPublisher,
)

RT = TypeVar("RT", bound=RootEntity)


@Injectable("aggregate")
class Aggregate(Generic[RT], SetupMixin):
    """Base Service class"""

    transaction_repository: TransactionRepository
    event_repository: EventRepository
    snapshot_repository: SnapshotRepository
    broker_publisher: TransactionalBrokerPublisher

    @Inject()
    def __init__(
        self,
        transaction_repository: TransactionRepository,
        event_repository: EventRepository,
        snapshot_repository: SnapshotRepository,
        broker_publisher: TransactionalBrokerPublisher,
        *args,
        **kwargs,
    ):
        if transaction_repository is None:
            raise NotProvidedException(f"A {TransactionRepository!r} object must be provided.")
        if event_repository is None:
            raise NotProvidedException(f"A {EventRepository!r} object must be provided.")
        if snapshot_repository is None:
            raise NotProvidedException(f"A {SnapshotRepository!r} object must be provided.")
        if broker_publisher is None:
            raise NotProvidedException(f"A {TransactionalBrokerPublisher!r} object must be provided.")

        super().__init__(*args, **kwargs)

        self._check_root()

        self.transaction_repository = transaction_repository
        self.event_repository = event_repository
        self.snapshot_repository = snapshot_repository
        self.broker_publisher = broker_publisher

    @classmethod
    def _from_config(cls, config: Config, **kwargs) -> Aggregate:
        kwargs["broker_publisher"] = TransactionalBrokerPublisher.from_config(config, **kwargs)
        return cls(**kwargs)

    def _check_root(self):
        self.root  # If root is not valid it will raise an exception.

    @property
    def root(self) -> type[RootEntity]:
        """Get the root entity of the aggregate.

        :return: A ``RootEntity`` type.
        """
        # noinspection PyUnresolvedReferences
        bases = self.__orig_bases__
        root = get_args(bases[0])[0]
        if not isinstance(root, type) or not issubclass(root, RootEntity):
            raise TypeError(f"{type(self)!r} must contain a {RootEntity!r} as generic value.")
        return root
