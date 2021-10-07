import unittest
from unittest.mock import (
    MagicMock,
    call,
)
from uuid import (
    uuid4,
)

from minos.common import (
    MinosBrokerNotProvidedException,
)
from minos.saga import (
    LocalExecutor,
    MinosSagaFailedExecutionStepException,
    RequestExecutor,
    SagaContext,
    SagaOperation,
)
from tests.utils import (
    Foo,
    NaiveBroker,
    send_create_product,
)


class TestRequestExecutor(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.broker = NaiveBroker()
        self.uuid = uuid4()
        self.executor = RequestExecutor(reply_topic="AddFoo", execution_uuid=self.uuid, broker=self.broker)

    def test_constructor(self):
        self.assertIsInstance(self.executor, LocalExecutor)
        self.assertEqual("AddFoo", self.executor.reply_topic)
        self.assertEqual(self.uuid, self.executor.execution_uuid)
        self.assertEqual(self.broker, self.executor.broker)

    def test_constructor_without_broker(self):
        with self.assertRaises(MinosBrokerNotProvidedException):
            RequestExecutor(reply_topic="AddFoo", execution_uuid=self.uuid)

    async def test_exec(self):
        operation = SagaOperation(send_create_product)
        context = SagaContext(product=Foo("create_product!"))

        mock = MagicMock(side_effect=self.broker.send)
        self.broker.send = mock
        await self.executor.exec(operation, context)

        self.assertEqual(1, mock.call_count)
        args = call(
            data=Foo("create_product!"), topic="CreateProduct", saga=self.uuid, reply_topic=self.executor.reply_topic
        )
        self.assertEqual(args, mock.call_args)

    async def test_exec_raises(self):
        operation = SagaOperation(send_create_product)
        context = SagaContext(product=Foo("create_product!"))

        async def _fn(*args, **kwargs):
            raise ValueError("This is an exception")

        mock = MagicMock(side_effect=_fn)
        self.broker.send = mock

        with self.assertRaises(MinosSagaFailedExecutionStepException) as result:
            await self.executor.exec(operation, context)
        self.assertEqual(
            "There was a failure while 'SagaStepExecution' was executing: ValueError('This is an exception')",
            str(result.exception),
        )


if __name__ == "__main__":
    unittest.main()
