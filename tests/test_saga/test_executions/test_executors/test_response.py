import unittest
from unittest.mock import (
    AsyncMock,
)

from minos.common import (
    CommandStatus,
)
from minos.saga import (
    LocalExecutor,
    MinosSagaFailedExecutionStepException,
    ResponseExecutor,
    SagaContext,
    SagaOperation,
)
from tests.utils import (
    Foo,
    fake_reply,
    handle_ticket_success,
)


class TestResponseExecutor(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.executor = ResponseExecutor()

    def test_constructor(self):
        self.assertIsInstance(self.executor, LocalExecutor)

    async def test_exec(self):
        operation = SagaOperation(handle_ticket_success)
        expected = SagaContext(one=1, ticket=Foo("text"))
        observed = await self.executor.exec(operation, SagaContext(one=1), reply=fake_reply(Foo("text")))
        self.assertEqual(expected, observed)

    async def test_exec_raises(self):
        reply = fake_reply(status=CommandStatus.ERROR)
        operation = SagaOperation(AsyncMock(side_effect=ValueError))
        with self.assertRaises(MinosSagaFailedExecutionStepException):
            await self.executor.exec(operation, SagaContext(), reply=reply)


if __name__ == "__main__":
    unittest.main()
