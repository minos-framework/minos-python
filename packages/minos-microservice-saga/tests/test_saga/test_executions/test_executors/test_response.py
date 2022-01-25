import unittest
from unittest.mock import (
    AsyncMock,
)
from uuid import (
    uuid4,
)

from minos.saga import (
    Executor,
    ResponseExecutor,
    SagaContext,
    SagaFailedExecutionStepException,
    SagaOperation,
    SagaResponse,
    SagaResponseStatus,
)
from tests.utils import (
    Foo,
    MinosTestCase,
    handle_ticket_success,
)


class TestResponseExecutor(MinosTestCase):
    def setUp(self) -> None:
        super().setUp()

        self.execution_uuid = uuid4()
        self.executor = ResponseExecutor(self.execution_uuid)

    def test_constructor(self):
        self.assertIsInstance(self.executor, Executor)

    async def test_exec(self):
        operation = SagaOperation(handle_ticket_success)
        expected = SagaContext(one=1, ticket=Foo("text"))
        response = SagaResponse(Foo("text"), {"ticket"})
        observed = await self.executor.exec(operation, SagaContext(one=1), response=response)
        self.assertEqual(expected, observed)

    async def test_exec_raises(self):
        response = SagaResponse(status=SagaResponseStatus.ERROR, related_services={"ticket"})
        operation = SagaOperation(AsyncMock(side_effect=ValueError))
        with self.assertRaises(SagaFailedExecutionStepException):
            await self.executor.exec(operation, SagaContext(), response=response)

    async def test_exec_return_exception_raises(self):
        response = SagaResponse(status=SagaResponseStatus.ERROR, related_services={"ticket"})
        operation = SagaOperation(AsyncMock(return_value=ValueError("This is an example")))
        with self.assertRaises(SagaFailedExecutionStepException):
            await self.executor.exec(operation, SagaContext(), response=response)


if __name__ == "__main__":
    unittest.main()
