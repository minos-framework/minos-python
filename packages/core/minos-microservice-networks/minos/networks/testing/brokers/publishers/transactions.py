from abc import (
    abstractmethod,
)
from typing import (
    Optional,
)
from uuid import (
    UUID,
    uuid4,
)

from minos.common import (
    DatabaseOperation,
)
from minos.common.testing import (
    MinosTestCase,
    MockedDatabaseClient,
    MockedDatabaseOperation,
)

from ....brokers import (
    BrokerMessageV1,
    BrokerMessageV1Payload,
    BrokerPublisherTransactionDatabaseOperationFactory,
    BrokerPublisherTransactionEntry,
    BrokerPublisherTransactionRepository,
)


class MockedBrokerPublisherTransactionDatabaseOperationFactory(BrokerPublisherTransactionDatabaseOperationFactory):
    """For testing purposes"""

    def build_create(self) -> DatabaseOperation:
        """For testing purposes"""
        return MockedDatabaseOperation("create")

    def build_query(self, transaction_uuid: Optional[UUID]) -> DatabaseOperation:
        """For testing purposes"""
        return MockedDatabaseOperation("query")

    def build_submit(self, message: bytes, transaction_uuid: UUID) -> DatabaseOperation:
        """For testing purposes"""
        return MockedDatabaseOperation("submit")

    def build_delete_batch(self, transaction_uuid: UUID) -> DatabaseOperation:
        """For testing purposes"""
        return MockedDatabaseOperation("delete_batch")


MockedDatabaseClient.set_factory(
    BrokerPublisherTransactionDatabaseOperationFactory, MockedBrokerPublisherTransactionDatabaseOperationFactory
)


class BrokerPublisherTransactionRepositoryTestCase(MinosTestCase):
    __test__ = False

    def setUp(self):
        super().setUp()
        self.repository = self.build_repository()

        self.transaction_uuid_1 = uuid4()
        self.transaction_uuid_2 = uuid4()
        self.entries = [
            BrokerPublisherTransactionEntry(
                BrokerMessageV1("AddFoo", BrokerMessageV1Payload(56)), self.transaction_uuid_1
            ),
            BrokerPublisherTransactionEntry(
                BrokerMessageV1("AddFoo", BrokerMessageV1Payload(56)), self.transaction_uuid_1
            ),
            BrokerPublisherTransactionEntry(
                BrokerMessageV1("AddFoo", BrokerMessageV1Payload(56)), self.transaction_uuid_2
            ),
        ]

    async def asyncSetUp(self):
        await super().asyncSetUp()
        await self.repository.setup()

    async def asyncTearDown(self):
        await self.repository.destroy()
        await super().asyncTearDown()

    def test_is_subclass(self):
        self.assertIsInstance(self.repository, BrokerPublisherTransactionRepository)

    @abstractmethod
    def build_repository(self) -> BrokerPublisherTransactionRepository:
        raise NotImplementedError

    async def populate(self) -> None:

        for entry in self.entries:
            await self.repository.submit(entry)

    async def test_submit(self):
        entry = self.entries[0]
        await self.repository.submit(entry)

        observed = [entry async for entry in self.repository.select(entry.transaction_uuid)]

        self.assertEqual([entry], observed)

    async def test_select_all(self):
        await self.populate()

        observed = [entry async for entry in self.repository.select()]

        self.assertEqual(self.entries, observed)

    async def test_select_transaction(self):
        await self.populate()

        observed = [entry async for entry in self.repository.select(self.transaction_uuid_1)]

        self.assertEqual([self.entries[0], self.entries[1]], observed)

    async def test_delete(self):
        await self.populate()

        await self.repository.delete_batch(self.transaction_uuid_2)
        observed = [entry async for entry in self.repository.select()]

        self.assertEqual([self.entries[0], self.entries[1]], observed)
