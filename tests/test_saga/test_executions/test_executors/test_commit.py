import unittest
from unittest.mock import (
    AsyncMock,
)

from minos.saga import (
    CommitExecutor,
    Executor,
    SagaContext,
    SagaFailedCommitCallbackException,
    SagaOperation,
)
from tests.utils import (
    Foo,
    commit_callback,
    commit_callback_raises,
)


class TestRequestExecutor(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.executor = CommitExecutor()

    def test_constructor(self):
        self.assertIsInstance(self.executor, Executor)

    async def test_exec(self):
        operation = SagaOperation(commit_callback)
        initial = SagaContext(product=Foo("create_product!"))

        observed = await self.executor.exec(operation, initial)

        self.assertEqual(SagaContext(product=Foo("create_product!"), status="Finished!"), observed)
        self.assertEqual(SagaContext(product=Foo("create_product!")), initial)

    async def test_exec_not_defined(self):
        context = SagaContext(product=Foo("create_product!"))

        observed = await self.executor.exec(None, context)

        self.assertEqual(context, observed)

    async def test_exec_return_none(self):
        operation = SagaOperation(AsyncMock(return_value=None))
        initial = SagaContext(product=Foo("create_product!"))

        observed = await self.executor.exec(operation, initial)

        self.assertEqual(initial, observed)

    async def test_exec_raises(self):
        operation = SagaOperation(commit_callback_raises)
        context = SagaContext(product=Foo("create_product!"))

        with self.assertRaises(SagaFailedCommitCallbackException):
            await self.executor.exec(operation, context)


if __name__ == "__main__":
    unittest.main()
