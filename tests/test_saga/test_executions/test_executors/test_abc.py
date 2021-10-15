import unittest

from minos.saga import (
    Executor,
    SagaContext,
    SagaOperation,
)


class TestExecutor(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.executor = Executor()

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


if __name__ == "__main__":
    unittest.main()
