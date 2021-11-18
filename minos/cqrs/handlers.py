from __future__ import (
    annotations,
)

from typing import (
    TypeVar,
)
from minos.aggregate import (
    AggregateDiff,
    ModelRefExtractor,
    ModelRefInjector,
)
from minos.common import (
    Model,
)
from minos.networks import (
    CommandBroker,
    DynamicHandler,
    DynamicHandlerPool,
)


class PreEventHandler:
    """Pre Event Handler class."""

    @classmethod
    async def handle(cls, diff: T, resolve_references: bool = True, **kwargs) -> T:
        if not isinstance(diff, AggregateDiff) or not resolve_references:
            return diff

        try:
            return await cls._query(diff, **kwargs)
        except Exception:
            return diff

    # noinspection PyUnusedLocal
    @classmethod
    async def _query(
        cls, diff, dynamic_handler_pool: DynamicHandlerPool, command_broker: CommandBroker, **kwargs
    ) -> None:
        missing = ModelRefExtractor(diff.fields_diff).build()

        recovered = dict()
        async with dynamic_handler_pool.acquire() as handler:
            for name, uuids in missing.items():
                await command_broker.send(data={"uuids": uuids}, topic=f"Get{name}s", reply_topic=handler.topic)
                for model in await cls._get_response(handler):
                    recovered[model.uuid] = model

        return ModelRefInjector(diff, recovered).build()

    @staticmethod
    async def _get_response(handler: DynamicHandler, **kwargs) -> list[Model]:
        handler_entry = await handler.get_one(**kwargs)
        response = handler_entry.data
        return response.data


T = TypeVar("T")
