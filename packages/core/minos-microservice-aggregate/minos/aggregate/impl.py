from __future__ import (
    annotations,
)

from asyncio import (
    gather,
)
from typing import (
    Generic,
    Optional,
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
    BrokerMessageV1,
    BrokerMessageV1Payload,
    BrokerPublisher,
)
from minos.transactions import (
    TransactionRepository,
)

from .actions import (
    Action,
)
from .entities import (
    EntityRepository,
    RootEntity,
)
from .events import (
    Event,
    EventRepository,
    IncrementalFieldDiff,
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
    repository: EntityRepository
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

        self.repository = EntityRepository(event_repository, snapshot_repository)

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

    async def publish_domain_event(self, delta: Optional[Event]) -> None:
        """Send a domain event message containing a delta.

        :param delta: The delta to be sent.
        :return: This method does not return anything.
        """

        if delta is None:
            return

        suffix_mapper = {
            Action.CREATE: "Created",
            Action.UPDATE: "Updated",
            Action.DELETE: "Deleted",
        }
        topic = f"{delta.simplified_name}{suffix_mapper[delta.action]}"
        message = BrokerMessageV1(topic, BrokerMessageV1Payload(delta))
        futures = [self.broker_publisher.send(message)]

        if delta.action == Action.UPDATE:
            for decomposed_event in delta.decompose():
                diff = next(iter(decomposed_event.fields_diff.flatten_values()))
                composed_topic = f"{topic}.{diff.name}"
                if isinstance(diff, IncrementalFieldDiff):
                    composed_topic += f".{diff.action.value}"

                message = BrokerMessageV1(composed_topic, BrokerMessageV1Payload(decomposed_event))
                futures.append(self.broker_publisher.send(message))

        await gather(*futures)
