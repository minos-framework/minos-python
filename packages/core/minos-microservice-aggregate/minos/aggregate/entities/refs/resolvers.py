import logging
from asyncio import (
    gather,
)
from collections.abc import (
    Iterable,
)
from functools import (
    reduce,
)
from itertools import (
    chain,
)
from operator import (
    or_,
)
from typing import (
    Any,
    Optional,
    Union,
)
from uuid import (
    UUID,
)

from minos.common import (
    Inject,
    Model,
    ModelType,
    NotProvidedException,
    PoolFactory,
)
from minos.networks import (
    BrokerClient,
    BrokerClientPool,
    BrokerMessageV1,
    BrokerMessageV1Payload,
)

from ...entities import (
    Entity,
)
from ...exceptions import (
    RefException,
)
from ...snapshots import (
    SnapshotRepository,
)
from .extractors import (
    RefExtractor,
)
from .injectors import (
    RefInjector,
)

logger = logging.getLogger(__name__)


class RefResolver:
    """Ref Resolver class."""

    # noinspection PyUnusedLocal
    @Inject()
    def __init__(
        self,
        snapshot_repository: SnapshotRepository,
        broker_pool: Optional[BrokerClientPool] = None,
        pool_factory: Optional[PoolFactory] = None,
        **kwargs,
    ):
        if broker_pool is None and pool_factory is not None:
            broker_pool = pool_factory.get_pool("broker")

        if broker_pool is None:
            raise NotProvidedException(f"A {BrokerClientPool!r} instance is required.")

        if snapshot_repository is None:
            raise NotProvidedException(f"A {SnapshotRepository!r} instance is required.")

        self._broker_pool = broker_pool
        self._snapshot_repository = snapshot_repository

    @property
    def broker_pool(self) -> BrokerClientPool:
        """Get the broker pool.

        :return: A ``BrokerClientPool`` instance.
        """
        return self._broker_pool

    @property
    def snapshot_repository(self) -> SnapshotRepository:
        """Get the snapshot repository.

        :return: A ``SnapshotRepository`` instance.
        """
        return self._snapshot_repository

    # noinspection PyUnusedLocal
    async def resolve(self, data: Any, **kwargs) -> Any:
        """Resolve Ref instances.

        :param data: The data to be resolved.
        :param kwargs: Additional named arguments.
        :return: The data instance with model references already resolved.
        """
        missing = RefExtractor(data).build()

        if not len(missing):
            return data

        recovered = await self._query(missing)

        return RefInjector(data, recovered).build()

    async def _query(self, references: dict[type, set[UUID]]) -> dict[UUID, Model]:
        snapshot, broker = dict(), dict()
        for type_, uuids in references.items():
            if issubclass(type_, Entity) or (isinstance(type_, ModelType) and issubclass(type_.model_cls, Entity)):
                snapshot[type_] = uuids
            else:
                broker[type_] = uuids

        parts = await gather(self._query_broker(broker), self._query_snapshot(snapshot))
        ans = reduce(or_, parts)
        return ans

    async def _query_broker(self, references: dict[type, set[UUID]]) -> dict[UUID, Model]:
        if not len(references):
            return dict()

        messages = (
            BrokerMessageV1(self.build_topic_name(type_), BrokerMessageV1Payload({"uuids": refs}))
            for type_, refs in references.items()
        )
        async with self._broker_pool.acquire() as broker:
            futures = (broker.send(message) for message in messages)
            await gather(*futures)

            return {model.uuid: model for model in await self._get_response(broker, len(references))}

    async def _query_snapshot(self, references: dict[type, set[UUID]]) -> dict[UUID, Model]:
        if not len(references):
            return dict()

        futures = list()
        for type_, uuids in references.items():
            for uuid in uuids:
                # noinspection PyTypeChecker
                futures.append(self._snapshot_repository.get(type_, uuid))
        return {model.uuid: model for model in await gather(*futures)}

    @staticmethod
    async def _get_response(broker: BrokerClient, count: int, **kwargs) -> Iterable[Model]:
        messages = list()
        async for message in broker.receive_many(count, **kwargs):
            if not message.ok:
                raise RefException(f"The received message is not ok: {message!r}")
            messages.append(message)

        return chain(*(message.content for message in messages))

    @staticmethod
    def build_topic_name(entity: Union[type, str]) -> str:
        """Build the topic name based on the name of the entity.

        :param entity: The name of the entity to be resolved.
        :return: The topic name.
        """
        if isinstance(entity, type):
            entity = entity.__name__
        return f"_Get{entity}Snapshots"
