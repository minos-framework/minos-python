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
    Injectable,
    NotProvidedException,
    SetupMixin,
)
from minos.networks import (
    BrokerPublisher,
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

RT = TypeVar("RT", bound=RootEntity)


@Injectable("aggregate")
class Aggregate(Generic[RT], SetupMixin):
    """Base Service class"""

    transaction_repository: TransactionRepository
    event_repository: EventRepository
    snapshot_repository: SnapshotRepository
    broker_publisher: BrokerPublisher

    @Inject()
    def __init__(
        self,
        transaction_repository: TransactionRepository,
        event_repository: EventRepository,
        snapshot_repository: SnapshotRepository,
        broker_publisher: BrokerPublisher,
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
            raise NotProvidedException(f"A {BrokerPublisher!r} object must be provided.")

        super().__init__(*args, **kwargs)

        self._check_root()

        self.transaction_repository = transaction_repository
        self.event_repository = event_repository
        self.snapshot_repository = snapshot_repository
        self.broker_publisher = broker_publisher

    @classmethod
    def _from_config(cls, config: Config, **kwargs) -> Aggregate:
        kwargs["broker_publisher"] = cls._get_broker_publisher_from_config(config, **kwargs)
        return cls(**kwargs)

    @staticmethod
    @Inject()
    def _get_broker_publisher_from_config(
        config: Config, broker_publisher: BrokerPublisher, **kwargs
    ) -> BrokerPublisher:
        aggregate_config = config.get_aggregate()

        if "publisher" in aggregate_config:
            publisher_config = aggregate_config["publisher"]
            client_cls = publisher_config.pop("client")
            publisher_kwargs = publisher_config | {"broker_publisher": broker_publisher} | kwargs
            broker_publisher = client_cls.from_config(config, **publisher_kwargs)

        return broker_publisher

    async def _setup(self) -> None:
        await super()._setup()
        await self.broker_publisher.setup()

    async def _destroy(self) -> None:
        await self.broker_publisher.destroy()
        await super()._destroy()

    def _check_root(self):
        self.root  # If root is not valid it will raise an exception.

    @property
    def root(self) -> type[RootEntity]:
        """Get the root entity of the aggregate.

        :return: A ``RootEntity`` type.
        """
        # noinspection PyUnresolvedReferences
        bases = self.__orig_bases__
        root = get_args(next((base for base in bases if len(get_args(base))), None))[0]
        if not isinstance(root, type) or not issubclass(root, RootEntity):
            raise TypeError(f"{type(self)!r} must contain a {RootEntity!r} as generic value.")
        return root
