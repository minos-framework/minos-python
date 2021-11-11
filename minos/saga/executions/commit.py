import logging
from uuid import (
    UUID,
)

from dependency_injector.wiring import (
    Provide,
    inject,
)

from minos.common import (
    NULL_UUID,
)
from minos.networks import (
    CommandBroker,
    CommandReply,
    DynamicHandler,
    DynamicHandlerPool,
)

from .steps import (
    SagaStepExecution,
)

logger = logging.getLogger(__name__)


class TransactionCommitter:
    """Transaction Committer class."""

    @inject
    def __init__(
        self,
        execution_uuid: UUID,
        executed_steps: list[SagaStepExecution],
        dynamic_handler_pool: DynamicHandlerPool = Provide["dynamic_handler_pool"],
        command_broker: CommandBroker = Provide["command_broker"],
        **kwargs,
    ):
        self.executed_steps = executed_steps
        self.execution_uuid = execution_uuid

        self.dynamic_handler_pool = dynamic_handler_pool
        self.command_broker = command_broker

    # noinspection PyUnusedCommit,PyMethodOverriding
    async def commit(self, **kwargs) -> None:
        """Commit the transaction.

        :param kwargs: Additional named arguments.
        :return: This method does not return anything.
        """
        logger.info("committing...")

        if await self._reserve():
            await self._commit()
        else:
            await self.reject()
            raise ValueError("Some transactions could not be committed.")

    async def _reserve(self) -> bool:
        async with self.dynamic_handler_pool.acquire() as handler:
            for executed_step in self.executed_steps:

                await self.command_broker.send(
                    data=self.execution_uuid,
                    topic=f"Reserve{executed_step.service_name.title()}Transaction",
                    saga=NULL_UUID,
                    reply_topic=handler.topic,
                )
                response = await self._get_response(handler)
                if not response.ok:
                    return False
        return True

    async def _commit(self) -> None:
        async with self.dynamic_handler_pool.acquire() as handler:
            for executed_step in self.executed_steps:
                await self.command_broker.send(
                    data=self.execution_uuid,
                    topic=f"Commit{executed_step.service_name.title()}Transaction",
                    saga=NULL_UUID,
                    reply_topic=handler.topic,
                )
                await self._get_response(handler)
        logger.info("Successfully committed!")

    async def reject(self) -> None:
        """Reject the transaction.

        :return:
        """
        async with self.dynamic_handler_pool.acquire() as handler:
            for executed_step in self.executed_steps:
                await self.command_broker.send(
                    data=self.execution_uuid,
                    topic=f"Reject{executed_step.service_name.title()}Transaction",
                    saga=NULL_UUID,
                    reply_topic=handler.topic,
                )
                await self._get_response(handler)

        logger.info("Successfully rejected!")

    @staticmethod
    async def _get_response(handler: DynamicHandler, **kwargs) -> CommandReply:
        handler_entry = await handler.get_one(**kwargs)
        response = handler_entry.data
        return response
