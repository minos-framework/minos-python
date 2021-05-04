from aiomisc.service.periodic import (
    Service,
    PeriodicService,
)
from minos.common import MinosConfig
from .dispatcher import MinosEventHandler
from .event_server import MinosEventServer
from aiokafka import AIOKafkaConsumer
from typing import (
    Awaitable,
    Any,
)


class MinosEventServerService(Service):
    """Minos QueueDispatcherService class."""

    def __init__(self, config: MinosConfig = None, **kwargs):
        super().__init__(**kwargs)
        self.dispatcher = MinosEventServer.from_config(config=config)
        self.consumer = None

    """
    def create_task(self, coro: Awaitable[Any]):
        task = self.loop.create_task(coro)
        self.dispatcher._tasks.add(task)
        task.add_done_callback(self.dispatcher._tasks.remove)
    """

    async def start(self) -> None:
        """Method to be called at the startup by the internal ``aiomisc`` loigc.

        :return: This method does not return anything.
        """
        await self.dispatcher.setup()

        # start the Service Event Consumer for Kafka
        self.consumer = AIOKafkaConsumer(
            group_id=self.dispatcher._broker_group_name,
            auto_offset_reset="latest",
            bootstrap_servers=self.dispatcher._kafka_conn_data,
        )

        await self.consumer.start()
        self.consumer.subscribe(self.dispatcher._topics)

        # self.create_task(self.dispatcher.handle_message(self.consumer))

        await self.dispatcher.handle_message(self.consumer)

    async def stop(self, exception: Exception = None) -> Any:
        if self.consumer is not None:
            await self.consumer.stop()


class MinosEventPeriodicService(PeriodicService):
    """Minos QueueDispatcherService class."""

    def __init__(self, config: MinosConfig = None, **kwargs):
        super().__init__(**kwargs)
        self.dispatcher = MinosEventHandler.from_config(config=config)

    async def start(self) -> None:
        """Method to be called at the startup by the internal ``aiomisc`` loigc.

        :return: This method does not return anything.
        """
        await super().start()
        await self.dispatcher.setup()

    async def callback(self) -> None:
        """Method to be called periodically by the internal ``aiomisc`` logic.

        :return:This method does not return anything.
        """
        await self.dispatcher.event_queue_checker()
