"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
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
    PublishExecutor,
    SagaContext,
    SagaOperation,
)
from tests.utils import (
    Foo,
    NaiveBroker,
    foo_fn,
)


class TestPublishExecutor(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.broker = NaiveBroker()
        self.uuid = uuid4()
        self.executor = PublishExecutor(definition_name="AddFoo", execution_uuid=self.uuid, broker=self.broker)

    def test_constructor(self):
        self.assertIsInstance(self.executor, LocalExecutor)
        self.assertEqual("AddFoo", self.executor.definition_name)
        self.assertEqual(self.uuid, self.executor.execution_uuid)
        self.assertEqual(self.broker, self.executor.broker)
        self.assertTrue(self.executor.asynchronous)

    def test_constructor_without_broker(self):
        with self.assertRaises(MinosBrokerNotProvidedException):
            PublishExecutor(definition_name="AddFoo", execution_uuid=self.uuid)

    def test_reply_topic(self):
        self.assertEqual(self.executor.definition_name, self.executor.reply_topic)

    def test_reply_topic_synchronous(self):
        executor = PublishExecutor(
            definition_name="AddFoo", execution_uuid=self.uuid, broker=self.broker, asynchronous=False
        )
        self.assertEqual(str(self.executor.execution_uuid), executor.reply_topic)

    async def test_exec(self):
        operation = SagaOperation(foo_fn, "AddBar")
        context = SagaContext()

        mock = MagicMock(side_effect=self.broker.send)
        self.broker.send = mock
        await self.executor.exec(operation, context)

        self.assertEqual(1, mock.call_count)
        args = call(data=Foo("hello"), topic="AddBar", saga=self.uuid, reply_topic=self.executor.reply_topic)
        self.assertEqual(args, mock.call_args)

    async def test_exec_raises(self):
        operation = SagaOperation(foo_fn, "AddBar")
        context = SagaContext()

        async def _fn(*args, **kwargs):
            raise ValueError("This is an exception")

        mock = MagicMock(side_effect=_fn)
        self.broker.send = mock

        with self.assertRaises(MinosSagaFailedExecutionStepException) as result:
            await self.executor.exec(operation, context)
        self.assertEqual(
            "There was a failure while 'SagaExecutionStep' was executing: ValueError('This is an exception')",
            str(result.exception),
        )


if __name__ == "__main__":
    unittest.main()
