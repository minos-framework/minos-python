from abc import (
    abstractmethod,
)
from uuid import (
    uuid4,
)

from minos.common.testing import (
    MinosTestCase,
)
from minos.networks import (
    BrokerMessageV1,
    BrokerMessageV1Payload,
    BrokerPublisherTransactionEntry,
    BrokerPublisherTransactionRepository,
)


class BrokerPublisherTransactionRepositoryTestCase(MinosTestCase):
    __test__ = False

    def setUp(self):
        super().setUp()
        self.repository = self.build_repository()

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
        m1 = BrokerMessageV1("AddFoo", BrokerMessageV1Payload(56))
        m2 = BrokerMessageV1("AddFoo", BrokerMessageV1Payload(56))
        m3 = BrokerMessageV1("AddFoo", BrokerMessageV1Payload(56))

        self.transaction_uuid_1 = uuid4()
        self.transaction_uuid_2 = uuid4()
        self.entries = [
            BrokerPublisherTransactionEntry(m1, self.transaction_uuid_1),
            BrokerPublisherTransactionEntry(m2, self.transaction_uuid_1),
            BrokerPublisherTransactionEntry(m3, self.transaction_uuid_2),
        ]

        for entry in self.entries:
            await self.repository.submit(entry)

    async def test_submit(self):
        message = BrokerMessageV1("AddFoo", BrokerMessageV1Payload(56))
        transaction_uuid = uuid4()
        entry = BrokerPublisherTransactionEntry(message, transaction_uuid)
        await self.repository.submit(entry)

        observed = [entry async for entry in self.repository.select(transaction_uuid)]

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
