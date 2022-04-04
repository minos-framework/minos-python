import logging
from asyncio import (
    gather,
)
from collections.abc import (
    Iterable,
)
from itertools import (
    chain,
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
    NotProvidedException,
    PoolFactory,
)
from minos.networks import (
    BrokerClient,
    BrokerClientPool,
    BrokerMessageV1,
    BrokerMessageV1Payload,
)

from ...exceptions import (
    RefException,
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
        broker_pool: Optional[BrokerClientPool] = None,
        pool_factory: Optional[PoolFactory] = None,
        **kwargs,
    ):
        if broker_pool is None and pool_factory is not None:
            broker_pool = pool_factory.get_pool("broker")

        if not isinstance(broker_pool, BrokerClientPool):
            raise NotProvidedException(f"A {BrokerClientPool!r} instance is required. Obtained: {broker_pool}")

        self.broker_pool = broker_pool

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

    async def _query(self, references: dict[str, set[UUID]]) -> dict[UUID, Model]:
        messages = (
            BrokerMessageV1(self.build_topic_name(name), BrokerMessageV1Payload({"uuids": uuids}))
            for name, uuids in references.items()
        )
        async with self.broker_pool.acquire() as broker:
            futures = (broker.send(message) for message in messages)
            await gather(*futures)

            return {model.uuid: model for model in await self._get_response(broker, len(references))}

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
