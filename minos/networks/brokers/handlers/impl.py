from __future__ import (
    annotations,
)

from asyncio import (
    CancelledError,
    PriorityQueue,
    create_task,
    gather,
)
from typing import (
    NoReturn,
    Optional,
)

from dependency_injector.wiring import (
    Provide,
    inject,
)

from minos.common import (
    MinosConfig,
    MinosSetup,
)

from ..dispatchers import (
    BrokerDispatcher,
)
from ..subscribers import (
    BrokerSubscriber,
)


class BrokerHandler(MinosSetup):
    """TODO"""

    def __init__(self, dispatcher: BrokerDispatcher, subscriber: BrokerSubscriber, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._dispatcher = dispatcher
        self._subscriber = subscriber

        self._queue = PriorityQueue(maxsize=1000)
        self._consumers = list()
        self._consumer_concurrency = 15

    @classmethod
    def _from_config(cls, config: MinosConfig, **kwargs) -> BrokerHandler:
        from ..subscribers import (
            KafkaBrokerSubscriber,
        )

        dispatcher = cls._get_dispatcher(config, **kwargs)
        subscriber = KafkaBrokerSubscriber.from_config(config, topics=set(dispatcher.actions.keys()))

        return cls(dispatcher, subscriber, **kwargs)

    @staticmethod
    @inject
    def _get_dispatcher(
        config: MinosConfig,
        dispatcher: Optional[BrokerDispatcher] = None,
        broker_dispatcher: BrokerDispatcher = Provide["broker_dispatcher"],
        **kwargs,
    ) -> BrokerDispatcher:
        if dispatcher is None:
            dispatcher = broker_dispatcher
        if dispatcher is None or isinstance(dispatcher, Provide):
            dispatcher = BrokerDispatcher.from_config(config, **kwargs)
        return dispatcher

    async def _setup(self) -> None:
        await super()._setup()
        await self._dispatcher.setup()
        await self._subscriber.setup()

        await self._create_consumers()

    async def _destroy(self) -> None:
        await self._destroy_consumers()

        await self._subscriber.destroy()
        await self._dispatcher.destroy()
        await super()._destroy()

    async def run(self) -> NoReturn:
        """TODO"""

        while True:
            async for message in self._subscriber.receive_all():
                await self._queue.put(message)

    async def _create_consumers(self):
        while len(self._consumers) < self._consumer_concurrency:
            self._consumers.append(create_task(self._consume()))

    async def _destroy_consumers(self):
        for consumer in self._consumers:
            consumer.cancel()
        await gather(*self._consumers, return_exceptions=True)
        self._consumers = list()

        while not self._queue.empty():
            # FIXME
            message = self._queue.get_nowait()  # noqa

    async def _consume(self) -> None:
        while True:
            await self._consume_one()

    async def _consume_one(self) -> None:
        message = await self._queue.get()
        try:
            try:
                await self._dispatcher.dispatch(message)
            except (CancelledError, Exception) as exc:
                # await self.submit_query(self._queries["update_not_processed"], (entry.id,))
                raise exc
            # await self.submit_query(self._queries["delete_processed"], (entry.id,))
        finally:
            self._queue.task_done()
