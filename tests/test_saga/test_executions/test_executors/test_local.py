"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import unittest

from minos.saga import (
    LocalExecutor,
    SagaContext,
    SagaOperation,
)


class TestLocalExecutor(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.executor = LocalExecutor()

    def test_constructor(self):
        self.assertIsInstance(self.executor, LocalExecutor)

    async def test_exec_operation(self):
        def _fn(c):
            return c["foo"]

        context = SagaContext(foo="bar")
        operation = SagaOperation(_fn)
        observed = await self.executor.exec_operation(operation, context)
        self.assertEqual("bar", observed)

    async def test_exec_operation_with_parameters(self):
        def _fn(c, one):
            return one

        context = SagaContext(foo="bar")
        operation = SagaOperation(_fn, parameters=SagaContext(one=1))
        observed = await self.executor.exec_operation(operation, context)
        self.assertEqual(1, observed)

    async def test_exec_operation_async(self):
        async def _fn(c):
            return c["foo"]

        context = SagaContext(foo="bar")
        operation = SagaOperation(_fn)
        observed = await self.executor.exec_operation(operation, context)
        self.assertEqual("bar", observed)


if __name__ == "__main__":
    unittest.main()
