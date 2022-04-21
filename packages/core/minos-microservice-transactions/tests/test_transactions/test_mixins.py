import unittest
from uuid import (
    uuid4,
)

from minos.common import (
    NotProvidedException,
    SetupMixin,
)
from minos.transactions import (
    TransactionalMixin,
)
from tests.utils import (
    TransactionsTestCase,
)


class TestTransactionalMixin(TransactionsTestCase):
    def test_abstract(self) -> None:
        self.assertTrue(issubclass(TransactionalMixin, SetupMixin))

    def test_constructor(self):
        observer = TransactionalMixin()
        self.assertEqual(self.transaction_repository, observer.transaction_repository)

    def test_constructor_raises(self):
        with self.assertRaises(NotProvidedException):
            # noinspection PyTypeChecker
            TransactionalMixin(transaction_repository=None)

    async def test_register_into_observable(self):
        observer = TransactionalMixin()
        self.assertNotIn(observer, self.transaction_repository.observers)
        async with observer:
            self.assertIn(observer, self.transaction_repository.observers)
        self.assertNotIn(observer, self.transaction_repository.observers)

    async def test_get_collided_transactions(self):
        mixin = TransactionalMixin()
        self.assertEqual(set(), await mixin.get_collided_transactions(uuid4()))


if __name__ == "__main__":
    unittest.main()
