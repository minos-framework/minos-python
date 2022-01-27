import unittest
from uuid import (
    uuid4,
)

from minos.aggregate import (
    TransactionStatus,
)
from minos.saga import (
    Executor,
    SagaContext,
    SagaOperation,
)
from tests.utils import (
    MinosTestCase,
)


class TestExecutor(MinosTestCase):
    def setUp(self) -> None:
        super().setUp()

        self.execution_uuid = uuid4()
        self.executor = Executor(self.execution_uuid)

    def test_constructor(self):
        self.assertIsInstance(self.executor, Executor)

    async def test_exec(self):
        def _fn(c):
            return c["foo"]

        context = SagaContext(foo="bar")
        operation = SagaOperation(_fn)
        observed = await self.executor.exec(operation, context)
        self.assertEqual("bar", observed)

    async def test_exec_with_parameters(self):
        def _fn(c, one):
            return one

        context = SagaContext(foo="bar")
        operation = SagaOperation(_fn, parameters=SagaContext(one=1))
        observed = await self.executor.exec(operation, context)
        self.assertEqual(1, observed)

    async def test_exec_async(self):
        async def _fn(c):
            return c["foo"]

        context = SagaContext(foo="bar")
        operation = SagaOperation(_fn)
        observed = await self.executor.exec(operation, context)
        self.assertEqual("bar", observed)

    async def test_exec_function(self):
        def _fn(c):
            return f"[{c}]"

        observed = await self.executor.exec_function(_fn, "foo")
        self.assertEqual("[foo]", observed)

    async def test_exec_function_transaction(self):
        await self.executor.exec_function(str, 3)
        transaction = await self.transaction_repository.get(self.execution_uuid)
        self.assertEqual(TransactionStatus.PENDING, transaction.status)


if __name__ == "__main__":
    unittest.main()
