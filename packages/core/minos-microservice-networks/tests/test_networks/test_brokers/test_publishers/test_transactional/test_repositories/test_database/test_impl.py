import unittest
from unittest.mock import (
    patch,
)

from minos.common import (
    DatabaseClient,
)
from minos.common.testing import (
    DatabaseMinosTestCase,
)
from minos.networks import (
    BrokerPublisherTransactionRepository,
    DatabaseBrokerPublisherTransactionRepository,
)
from minos.networks.testing import (
    BrokerPublisherTransactionRepositoryTestCase,
)
from tests.utils import (
    FakeAsyncIterator,
    NetworksTestCase,
)


class TestDatabaseBrokerPublisherTransactionRepository(
    NetworksTestCase, DatabaseMinosTestCase, BrokerPublisherTransactionRepositoryTestCase
):
    __test__ = True

    def build_repository(self) -> BrokerPublisherTransactionRepository:
        return DatabaseBrokerPublisherTransactionRepository.from_config(self.config)

    async def populate(self) -> None:
        await super().populate()

    async def test_submit(self):
        iterator = FakeAsyncIterator([(self.entries[0].message, self.entries[0].transaction_uuid)])
        with patch.object(DatabaseClient, "fetch_all", return_value=iterator):
            await super().test_submit()

    async def test_select_all(self):
        iterator = FakeAsyncIterator(
            [
                (self.entries[0].message, self.entries[0].transaction_uuid),
                (self.entries[1].message, self.entries[1].transaction_uuid),
                (self.entries[2].message, self.entries[2].transaction_uuid),
            ]
        )
        with patch.object(DatabaseClient, "fetch_all", return_value=iterator):
            await super().test_select_all()

    async def test_select_transaction(self):
        iterator = FakeAsyncIterator(
            [
                (self.entries[0].message, self.entries[0].transaction_uuid),
                (self.entries[1].message, self.entries[1].transaction_uuid),
            ]
        )
        with patch.object(DatabaseClient, "fetch_all", return_value=iterator):
            await super().test_select_transaction()

    async def test_delete(self):
        iterator = FakeAsyncIterator(
            [
                (self.entries[0].message, self.entries[0].transaction_uuid),
                (self.entries[1].message, self.entries[1].transaction_uuid),
            ]
        )
        with patch.object(DatabaseClient, "fetch_all", return_value=iterator):
            await super().test_delete()


if __name__ == "__main__":
    unittest.main()
