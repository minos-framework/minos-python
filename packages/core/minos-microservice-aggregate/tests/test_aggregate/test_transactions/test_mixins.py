import unittest
from uuid import (
    UUID,
)

from minos.aggregate import (
    TransactionalMixin,
)
from minos.common import (
    SetupMixin,
)
from tests.utils import (
    AggregateTestCase,
)


class _Observer(TransactionalMixin):
    """For testing purposes."""

    async def get_related_transactions(self, transaction_uuid: UUID) -> set[UUID]:
        """For testing purposes."""

    async def commit_transaction(self, transaction_uuid: UUID, destination_transaction_uuid: UUID) -> None:
        """For testing purposes."""


class TestTransactionalMixin(AggregateTestCase):
    def test_abstract(self) -> None:
        self.assertTrue(issubclass(TransactionalMixin, SetupMixin))

    def test_constructor(self):
        observer = _Observer()
        self.assertEqual(self.transaction_repository, observer.transaction_repository)

    async def test_register_into_observable(self):
        observer = _Observer()
        self.assertNotIn(observer, self.transaction_repository.observers)
        async with observer:
            self.assertIn(observer, self.transaction_repository.observers)
        self.assertNotIn(observer, self.transaction_repository.observers)


if __name__ == "__main__":
    unittest.main()
