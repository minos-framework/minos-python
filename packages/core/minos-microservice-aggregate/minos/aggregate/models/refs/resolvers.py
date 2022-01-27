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
)
from uuid import (
    UUID,
)

from dependency_injector.wiring import (
    Provide,
    inject,
)

from minos.common import (
    Model,
)
from minos.networks import (
    BrokerClient,
    BrokerClientPool,
    BrokerMessageV1,
    BrokerMessageV1Payload,
)

from .extractors import (
    ModelRefExtractor,
)
from .injectors import (
    ModelRefInjector,
)


class ModelRefResolver:
    """ModelRef Resolver class."""

    # noinspection PyUnusedLocal
    @inject
    def __init__(self, broker_pool: BrokerClientPool = Provide["broker_pool"], **kwargs):
        self.broker_pool = broker_pool

    # noinspection PyUnusedLocal
    async def resolve(self, data: Any, **kwargs) -> Any:
        """Resolve ModelRef instances.

        :param data: The data to be resolved.
        :param kwargs: Additional named arguments.
        :return: The data instance with model references already resolved.
        """
        missing = ModelRefExtractor(data).build()

        if not len(missing):
            return data

        recovered = await self._query(missing)

        return ModelRefInjector(data, recovered).build()

    async def _query(self, references: dict[str, set[UUID]]) -> dict[UUID, Model]:
        async with self.broker_pool.acquire() as broker:
            futures = (
                broker.send(BrokerMessageV1(f"Get{name}s", BrokerMessageV1Payload({"uuids": uuids})))
                for name, uuids in references.items()
            )
            await gather(*futures)

            return {model.uuid: model for model in await self._get_response(broker, len(references))}

    @staticmethod
    async def _get_response(broker: BrokerClient, count: int, **kwargs) -> Iterable[Model]:
        messages = [message async for message in broker.receive_many(count, **kwargs)]
        return chain(*(message.content for message in messages))
