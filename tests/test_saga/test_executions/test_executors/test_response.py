import unittest
from unittest.mock import (
    AsyncMock,
)

from minos.common import (
    CommandStatus,
)
from minos.saga import (
    Executor,
    ResponseExecutor,
    SagaContext,
    SagaFailedExecutionStepException,
    SagaOperation,
    SagaResponse,
)
from tests.utils import (
    Foo,
    handle_ticket_success,
)


class TestResponseExecutor(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.executor = ResponseExecutor()

    def test_constructor(self):
        self.assertIsInstance(self.executor, Executor)

    async def test_exec(self):
        operation = SagaOperation(handle_ticket_success)
        expected = SagaContext(one=1, ticket=Foo("text"))
        observed = await self.executor.exec(operation, SagaContext(one=1), response=SagaResponse(Foo("text")))
        self.assertEqual(expected, observed)

    async def test_exec_raises(self):
        response = SagaResponse(status=CommandStatus.ERROR)
        operation = SagaOperation(AsyncMock(side_effect=ValueError))
        with self.assertRaises(SagaFailedExecutionStepException):
            await self.executor.exec(operation, SagaContext(), response=response)

    async def test_exec_return_exception_raises(self):
        response = SagaResponse(status=CommandStatus.ERROR)
        operation = SagaOperation(AsyncMock(return_value=ValueError("This is an example")))
        with self.assertRaises(SagaFailedExecutionStepException):
            await self.executor.exec(operation, SagaContext(), response=response)


if __name__ == "__main__":
    unittest.main()
