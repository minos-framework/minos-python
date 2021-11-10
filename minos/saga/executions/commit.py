import logging
from uuid import (
    UUID,
)

from dependency_injector.wiring import (
    Provide,
    inject,
)

from minos.common import (
    MinosBroker,
    MinosHandler,
    MinosPool,
)

from .steps import (
    SagaStepExecution,
)

logger = logging.getLogger(__name__)


class TransactionManager:
    """Commit Executor class."""

    @inject
    def __init__(
        self,
        executed_steps: list[SagaStepExecution],
        execution_uuid: UUID,
        dynamic_handler_pool: MinosPool[MinosHandler] = Provide["dynamic_handler_pool"],
        command_broker: MinosBroker = Provide["command_broker"],
        **kwargs,
    ):
        self.executed_steps = executed_steps
        self.execution_uuid = execution_uuid

        self.dynamic_handler_pool = dynamic_handler_pool
        self.command_broker = command_broker

    # noinspection PyUnusedCommit,PyMethodOverriding
    async def commit(self, **kwargs) -> None:
        """TODO"""
        logger.info("committing!")
        if await self._reserve():
            await self._commit()
        else:
            await self.reject()

    async def _reserve(self) -> bool:
        async with self.dynamic_handler_pool.acquire() as handler:
            for executed_step in self.executed_steps:

                await self.command_broker.send(
                    self.execution_uuid, topic=f"Reserve{executed_step.service_name.title()}Transaction"
                )
                response = await self._get_response(handler)
                if not response.ok:
                    return False
        return True

    async def _commit(self) -> None:
        async with self.dynamic_handler_pool.acquire() as handler:
            for executed_step in self.executed_steps:
                await self.command_broker.send(
                    self.execution_uuid, topic=f"Commit{executed_step.service_name.title()}Transaction"
                )
                await self._get_response(handler)
        logger.info("Successfully committed!")

    async def reject(self) -> None:
        """TODO"""
        async with self.dynamic_handler_pool.acquire() as handler:
            for executed_step in self.executed_steps:

                await self.command_broker.send(
                    self.execution_uuid, topic=f"Reject{executed_step.service_name.title()}Transaction"
                )
                await self._get_response(handler)

        logger.info("Successfully rejected!")

    @staticmethod
    async def _get_response(handler: MinosHandler, **kwargs):
        handler_entry = await handler.get_one(**kwargs)
        response = handler_entry.data
        return response
