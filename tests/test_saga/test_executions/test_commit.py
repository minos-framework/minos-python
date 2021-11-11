import unittest
from unittest.mock import (
    patch,
)
from uuid import (
    uuid4,
)

from minos.aggregate import (
    EventRepositoryConflictException,
    TransactionEntry,
    TransactionStatus,
)
from minos.saga import (
    TransactionCommitter,
)
from tests.utils import (
    MinosTestCase,
)


class TestTransactionCommitter(MinosTestCase):
    def setUp(self) -> None:
        super().setUp()

        self.execution_uuid = uuid4()
        self.committer = TransactionCommitter(self.execution_uuid)

    async def asyncSetUp(self):
        await super().asyncSetUp()
        await TransactionEntry(self.execution_uuid).save()

    async def test_commit_true(self):
        await self.committer.commit()

        observed = await self.transaction_repository.get(self.execution_uuid)
        self.assertEqual(TransactionStatus.COMMITTED, observed.status)

    async def test_commit_false(self):
        with patch("minos.aggregate.TransactionEntry.reserve", side_effect=EventRepositoryConflictException("", 0)):

            with self.assertRaises(ValueError):
                await self.committer.commit()

        observed = await self.transaction_repository.get(self.execution_uuid)
        self.assertEqual(TransactionStatus.REJECTED, observed.status)

    async def test_reject(self):
        await self.committer.reject()

        observed = await self.transaction_repository.get(self.execution_uuid)
        self.assertEqual(TransactionStatus.REJECTED, observed.status)


if __name__ == "__main__":
    unittest.main()
