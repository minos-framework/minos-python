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
    NaiveBroker,
    send_create_product,
)


class TestRequestExecutor(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.reply_topic = "AddFoo"
        self.broker = NaiveBroker()
        self.execution_uuid = uuid4()
        self.user = uuid4()
        self.executor = RequestExecutor(
            reply_topic=self.reply_topic, execution_uuid=self.execution_uuid, user=self.user, broker=self.broker
        )

    def test_constructor(self):
        self.assertIsInstance(self.executor, Executor)
        self.assertEqual(self.reply_topic, self.executor.reply_topic)
        self.assertEqual(self.execution_uuid, self.executor.execution_uuid)
        self.assertEqual(self.user, self.executor.user)
        self.assertEqual(self.broker, self.executor.broker)

    def test_constructor_without_broker(self):
        with self.assertRaises(NotProvidedException):
            RequestExecutor(reply_topic="AddFoo", execution_uuid=self.execution_uuid, user=self.user)

    async def test_exec(self):
        operation = SagaOperation(send_create_product)
        context = SagaContext(product=Foo("create_product!"))

        mock = MagicMock(side_effect=self.broker.send)
        self.broker.send = mock
        await self.executor.exec(operation, context)

        self.assertEqual(1, mock.call_count)
        args = call(
            data=Foo("create_product!"),
            topic="CreateProduct",
            saga=self.execution_uuid,
            user=self.user,
            reply_topic=self.reply_topic,
        )
        self.assertEqual(args, mock.call_args)

    async def test_exec_none_reply_topic(self):
        executor = RequestExecutor(
            reply_topic=None, execution_uuid=self.execution_uuid, user=self.user, broker=self.broker
        )
        operation = SagaOperation(send_create_product)
        context = SagaContext(product=Foo("create_product!"))

        mock = MagicMock(side_effect=self.broker.send)
        self.broker.send = mock
        await executor.exec(operation, context)

        self.assertEqual(1, mock.call_count)
        args = call(
            data=Foo("create_product!"),
            topic="CreateProduct",
            saga=self.execution_uuid,
            user=self.user,
            reply_topic=None,
        )
        self.assertEqual(args, mock.call_args)

    async def test_exec_none_user(self):
        executor = RequestExecutor(
            reply_topic=self.reply_topic, execution_uuid=self.execution_uuid, user=None, broker=self.broker
        )
        operation = SagaOperation(send_create_product)
        context = SagaContext(product=Foo("create_product!"))

        mock = MagicMock(side_effect=self.broker.send)
        self.broker.send = mock
        await executor.exec(operation, context)

        self.assertEqual(1, mock.call_count)
        args = call(
            data=Foo("create_product!"),
            topic="CreateProduct",
            saga=self.execution_uuid,
            user=None,
            reply_topic=self.reply_topic,
        )
        self.assertEqual(args, mock.call_args)

    async def test_exec_raises(self):
        operation = SagaOperation(send_create_product)
        context = SagaContext(product=Foo("create_product!"))

        async def _fn(*args, **kwargs):
            raise ValueError("This is an exception")

        mock = MagicMock(side_effect=_fn)
        self.broker.send = mock

        with self.assertRaises(SagaFailedExecutionStepException) as result:
            await self.executor.exec(operation, context)
        self.assertEqual(
            "There was a failure while 'SagaStepExecution' was executing: ValueError('This is an exception')",
            str(result.exception),
        )


if __name__ == "__main__":
    unittest.main()
