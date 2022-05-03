import logging
from asyncio import (
    gather,
)
from typing import (
    Optional,
)
from uuid import (
    UUID,
)

from cached_property import (
    cached_property,
)

from minos.common import (
    Inject,
    NotProvidedException,
    PoolFactory,
)
from minos.networks import (
    BrokerClient,
    BrokerClientPool,
    BrokerMessageV1,
    BrokerMessageV1Payload,
    BrokerPublisher,
)

from .steps import (
    ConditionalSagaStepExecution,
    SagaStepExecution,
)

logger = logging.getLogger(__name__)


class TransactionCommitter:
    """Transaction Committer class."""

    # noinspection PyUnusedLocal
    @Inject()
    def __init__(
        self,
        execution_uuid: UUID,
        executed_steps: list[SagaStepExecution],
        broker_publisher: BrokerPublisher,
        broker_pool: Optional[BrokerClientPool] = None,
        pool_factory: Optional[PoolFactory] = None,
        **kwargs,
    ):

        if broker_pool is None and pool_factory is not None:
            broker_pool = pool_factory.get_pool("broker")

        if broker_pool is None:
            raise NotProvidedException(f"A {BrokerClientPool!r} instance is required.")

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
                broker.send(BrokerMessageV1(f"_Reserve{service_name.title()}Transaction", BrokerMessageV1Payload(uuid)))
                for (uuid, service_name) in self.transactions
            )
            await gather(*futures)

            return await self._get_response(broker, len(self.transactions))

    async def _commit(self) -> None:
        futures = (
            self.broker_publisher.send(
                BrokerMessageV1(f"_Commit{service_name.title()}Transaction", BrokerMessageV1Payload(uuid))
            )
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
            self.broker_publisher.send(
                BrokerMessageV1(f"_Reject{service_name.title()}Transaction", BrokerMessageV1Payload(uuid))
            )
            for (uuid, service_name) in self.transactions
        )
        await gather(*futures)

        logger.info("Successfully rejected!")

    @staticmethod
    async def _get_response(handler: BrokerClient, count: int, **kwargs) -> bool:
        messages = [message async for message in handler.receive_many(count, **kwargs)]
        return all(message.ok for message in messages)

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
                    if inner is not None:
                        _fn(inner.uuid, inner.executed_steps)
                else:
                    for service_name in step.related_services:
                        pair = (uuid, service_name)
                        if pair not in uniques:
                            transactions.append(pair)
                            uniques.add(pair)

        _fn(self.execution_uuid, self.executed_steps)
        transactions.sort()
        return transactions
