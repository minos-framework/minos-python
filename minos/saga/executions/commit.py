import logging
from typing import (
    Optional,
)
from uuid import (
    UUID,
)

from dependency_injector.wiring import (
    Provide,
    inject,
)

from minos.common import (
    CommandReply,
    MinosBroker,
    MinosHandler,
    MinosPool,
    ModelType,
)

logger = logging.getLogger(__name__)

TransactionRequest = ModelType.build("TransactionRequest", {"transaction_uuid": UUID, "reply_topic": Optional[str]})


class TransactionManager:
    """Commit Executor class."""

    @inject
    def __init__(
        self,
        execution_uuid: UUID,
        count: int,
        dynamic_handler_pool: MinosPool[MinosHandler] = Provide["dynamic_handler_pool"],
        command_broker: MinosBroker = Provide["command_broker"],
        **kwargs,
    ):
        self.execution_uuid = execution_uuid

        self.count = count

        self.dynamic_handler_pool = dynamic_handler_pool
        self.command_broker = command_broker

    # noinspection PyUnusedCommit,PyMethodOverriding
    async def commit(self, **kwargs) -> None:
        """TODO"""
        logger.info("committing!")
        if await self._can_commit(**kwargs):
            await self._commit()
        else:
            await self.reject()

    async def _can_commit(self, **kwargs) -> bool:
        async with self.dynamic_handler_pool.acquire() as handler:
            content = TransactionRequest(self.execution_uuid, handler.topic)
            await self.command_broker.send(content, topic="SagaCommitted")

            count = 0
            for _ in range(self.count + 1):
                reply = await self._get_reply(handler, **kwargs)
                if reply.status == "reserved":
                    count += 1
            return count == self.count + 1

    async def _get_reply(self, handler: MinosHandler, **kwargs) -> CommandReply:
        reply = None
        while reply is None or reply["transaction_uuid"] != self.execution_uuid:
            entry = await handler.get_one(**kwargs)
            reply = entry.data.data
        return reply

    async def _commit(self) -> None:
        content = TransactionRequest(self.execution_uuid)
        await self.command_broker.send(content, topic="TransactionCommitted")

    async def reject(self) -> None:
        content = TransactionRequest(self.execution_uuid)
        await self.command_broker.send(content, topic="TransactionRejected")
