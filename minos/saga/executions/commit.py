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
    BrokerPublisher,
    DynamicBroker,
    DynamicBrokerPool,
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
        broker_pool: DynamicBrokerPool = Provide["broker_pool"],
        broker_publisher: BrokerPublisher = Provide["broker_publisher"],
        **kwargs,
    ):
        self.executed_steps = executed_steps
        self.execution_uuid = execution_uuid

        self.broker_pool = broker_pool
        self.broker_publisher = broker_publisher

    # noinspection PyUnusedCommit,PyMethodOverriding
    async def commit(self, **kwargs) -> None:
        """Commit the transaction.

        :param kwargs: Additional named arguments.
        :return: This method does not return anything.
        """
        logger.info("committing...")

        if not await self._reserve():
            await self.reject()
            raise ValueError("Some transactions could not be committed.")

        await self._commit()

    async def _reserve(self) -> bool:
        async with self.broker_pool.acquire() as broker:
            futures = (
                broker.send(data=uuid, topic=f"Reserve{service_name.title()}Transaction")
                for (uuid, service_name) in self.transactions
            )
            await gather(*futures)

            return await self._get_response(broker, len(self.transactions))

    async def _commit(self) -> None:
        futures = (
            self.broker_publisher.send(data=uuid, topic=f"Commit{service_name.title()}Transaction")
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
            self.broker_publisher.send(data=uuid, topic=f"Reject{service_name.title()}Transaction")
            for (uuid, service_name) in self.transactions
        )
        await gather(*futures)

        logger.info("Successfully rejected!")

    @staticmethod
    async def _get_response(handler: DynamicBroker, count: int, **kwargs) -> bool:
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
