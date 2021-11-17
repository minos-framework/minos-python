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
    ConditionalSagaStepExecution,
    SagaStepExecution,
)

logger = logging.getLogger(__name__)


class TransactionCommitter:
    """Transaction Committer class."""

    # noinspection PyUnusedLocal
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
        async with self.dynamic_handler_pool.acquire() as handler:
            if await self._reserve(handler):
                await self._commit(handler)
            else:
                await self._reject(handler)
                raise ValueError("Some transactions could not be committed.")

    async def _reserve(self, handler: DynamicHandler) -> bool:
        for (uuid, service_name) in self.transactions:
            await self.command_broker.send(
                data=uuid, topic=f"Reserve{service_name.title()}Transaction", reply_topic=handler.topic,
            )
            response = await self._get_response(handler)
            if not response.ok:
                return False
        return True

    async def _commit(self, handler: DynamicHandler) -> None:
        for (uuid, service_name) in self.transactions:
            await self.command_broker.send(
                data=uuid, topic=f"Commit{service_name.title()}Transaction", reply_topic=handler.topic,
            )
            await self._get_response(handler)
        logger.info("Successfully committed!")

    async def reject(self) -> None:
        """Reject the transaction.

        :return: This method does not return anything.
        """
        async with self.dynamic_handler_pool.acquire() as handler:
            await self._reject(handler)

    async def _reject(self, handler: DynamicHandler) -> None:
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
        """Get the list of transactions used during the saga execution.

        :return: A list of tuples in which the first value is the identifier of the transaction and the second one is
            the name of the microservice in which the saga was executed.
        """
        transactions = list()
        uniques = set()

        def _fn(uuid: UUID, steps: list[SagaStepExecution]) -> None:
            for step in steps:
                if isinstance(step, ConditionalSagaStepExecution):
                    inner = step.inner
                    return _fn(inner.uuid, inner.executed_steps)

                pair = (uuid, step.service_name)
                if pair in uniques:
                    continue
                transactions.append(pair)
                uniques.add(pair)

        _fn(self.execution_uuid, self.executed_steps)
        return transactions
