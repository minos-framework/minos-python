import logging
from uuid import (
    UUID,
)

from cached_property import (
    cached_property,
)
from dependency_injector.wiring import (
    Provide,
    inject,
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
            for (uuid, service_name) in self.transactions:
                await self.command_broker.send(
                    data=uuid, topic=f"Reserve{service_name.title()}Transaction", reply_topic=handler.topic,
                )
                response = await self._get_response(handler)
                if not response.ok:
                    return False
        return True

    async def _commit(self) -> None:
        async with self.dynamic_handler_pool.acquire() as handler:
            for (uuid, service_name) in self.transactions:
                await self.command_broker.send(
                    data=uuid, topic=f"Commit{service_name.title()}Transaction", reply_topic=handler.topic,
                )
                await self._get_response(handler)
        logger.info("Successfully committed!")

    async def reject(self) -> None:
        """Reject the transaction.

        :return:
        """
        async with self.dynamic_handler_pool.acquire() as handler:
            for (uuid, service_name) in self.transactions:
                await self.command_broker.send(
                    data=uuid, topic=f"Reject{service_name.title()}Transaction", reply_topic=handler.topic,
                )
                await self._get_response(handler)

        logger.info("Successfully rejected!")

    @staticmethod
    async def _get_response(handler: DynamicHandler, **kwargs) -> CommandReply:
        handler_entry = await handler.get_one(**kwargs)
        response = handler_entry.data
        return response

    @cached_property
    def transactions(self) -> list[tuple[UUID, str]]:
        """TODO"""
        transactions = list()
        uniques = set()

        for executed_step in self.executed_steps:
            pair = (self.execution_uuid, executed_step.service_name)
            if pair in uniques:
                continue
            transactions.append(pair)
            uniques.add(pair)

        return transactions
