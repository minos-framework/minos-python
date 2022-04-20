import unittest
from uuid import (
    uuid4,
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


class TestTransactionalMixin(AggregateTestCase):
    def test_abstract(self) -> None:
        self.assertTrue(issubclass(TransactionalMixin, SetupMixin))

    def test_constructor(self):
        observer = TransactionalMixin()
        self.assertEqual(self.transaction_repository, observer.transaction_repository)

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
