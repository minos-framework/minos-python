import logging
from asyncio import (
    gather,
)
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
            reserved = await self._reserve(handler)

        if reserved:
            await self._commit()
        else:
            await self.reject()
            raise ValueError("Some transactions could not be committed.")

    async def _reserve(self, handler: DynamicHandler) -> bool:
        futures = (
            self.command_broker.send(
                data=uuid, topic=f"Reserve{service_name.title()}Transaction", reply_topic=handler.topic
            )
            for (uuid, service_name) in self.transactions
        )
        await gather(*futures)

        return await self._get_response(handler, len(self.transactions))

    async def _commit(self) -> None:
        futures = (
            self.command_broker.send(data=uuid, topic=f"Commit{service_name.title()}Transaction")
            for (uuid, service_name) in self.transactions
        )
        await gather(*futures)

        # await self._get_response(handler, len(self.transactions))
        logger.info("Successfully committed!")

    async def reject(self) -> None:
        """Reject the transaction.

        :return: This method does not return anything.
        """
        futures = (
            self.command_broker.send(data=uuid, topic=f"Reject{service_name.title()}Transaction")
            for (uuid, service_name) in self.transactions
        )
        await gather(*futures)

        logger.info("Successfully rejected!")

    @staticmethod
    async def _get_response(handler: DynamicHandler, count: int, **kwargs) -> bool:
        entries = await handler.get_many(count, **kwargs)
        return all(entry.data.ok for entry in entries)

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
