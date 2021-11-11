import unittest
from unittest.mock import (
    MagicMock,
    call,
)
from uuid import (
    uuid4,
)

from minos.common import (
    NotProvidedException,
)
from minos.saga import (
    Executor,
    RequestExecutor,
    SagaContext,
    SagaFailedExecutionStepException,
    SagaOperation,
)
from tests.utils import (
    Foo,
    MinosTestCase,
    send_create_product,
)


class TestRequestExecutor(MinosTestCase):
    def setUp(self) -> None:
        super().setUp()

        self.execution_uuid = uuid4()
        self.user = uuid4()
        self.executor = RequestExecutor(execution_uuid=self.execution_uuid, user=self.user)

    def test_constructor(self):
        self.assertIsInstance(self.executor, Executor)
        self.assertEqual(self.execution_uuid, self.executor.execution_uuid)
        self.assertEqual(self.user, self.executor.user)
        self.assertEqual(self.command_broker, self.executor.command_broker)

    def test_constructor_without_broker(self):
        with self.assertRaises(NotProvidedException):
            RequestExecutor(command_broker=None, execution_uuid=self.execution_uuid, user=self.user)

    async def test_exec(self):
        operation = SagaOperation(send_create_product)
        context = SagaContext(product=Foo("create_product!"))

        mock = MagicMock(side_effect=self.command_broker.send)
        self.command_broker.send = mock
        await self.executor.exec(operation, context)

        self.assertEqual(1, mock.call_count)
        args = call(
            data=Foo("create_product!"),
            topic="CreateProduct",
            saga=self.execution_uuid,
            user=self.user,
            reply_topic="OrderReply",
        )
        self.assertEqual(args, mock.call_args)

    async def test_exec_none_user(self):
        executor = RequestExecutor(execution_uuid=self.execution_uuid, user=None,)
        operation = SagaOperation(send_create_product)
        context = SagaContext(product=Foo("create_product!"))

        mock = MagicMock(side_effect=self.command_broker.send)
        self.command_broker.send = mock
        await executor.exec(operation, context)

        self.assertEqual(1, mock.call_count)
        args = call(
            data=Foo("create_product!"),
            topic="CreateProduct",
            saga=self.execution_uuid,
            user=None,
            reply_topic="OrderReply",
        )
        self.assertEqual(args, mock.call_args)

    async def test_exec_raises(self):
        operation = SagaOperation(send_create_product)
        context = SagaContext(product=Foo("create_product!"))

        async def _fn(*args, **kwargs):
            raise ValueError("This is an exception")

        mock = MagicMock(side_effect=_fn)
        self.command_broker.send = mock

        with self.assertRaises(SagaFailedExecutionStepException) as result:
            await self.executor.exec(operation, context)
        self.assertEqual(
            "There was a failure while 'SagaStepExecution' was executing: ValueError('This is an exception')",
            str(result.exception),
        )


if __name__ == "__main__":
    unittest.main()
