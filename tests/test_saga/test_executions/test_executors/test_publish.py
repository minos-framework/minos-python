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

from minos.saga import (
    LocalExecutor,
    MinosSagaFailedExecutionStepException,
    PublishExecutor,
    SagaContext,
    SagaStepOperation,
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

    def test_constructor(self):
        executor = PublishExecutor(definition_name="AddFoo", execution_uuid=self.uuid, broker=self.broker)

        self.assertIsInstance(executor, LocalExecutor)
        self.assertEqual("AddFoo", executor.definition_name)
        self.assertEqual(self.uuid, executor.execution_uuid)
        self.assertEqual(self.broker, executor.broker)

    async def test_exec(self):
        executor = PublishExecutor(definition_name="AddFoo", execution_uuid=self.uuid, broker=self.broker)

        operation = SagaStepOperation("AddBar", foo_fn)
        context = SagaContext()

        mock = MagicMock(side_effect=self.broker.send)
        self.broker.send = mock
        await executor.exec(operation, context)

        self.assertEqual(1, mock.call_count)
        args = call([Foo("hello")], topic="AddBar", saga_uuid=str(self.uuid), reply_topic=None)
        self.assertEqual(args, mock.call_args)

    async def test_exec_with_reply(self):
        executor = PublishExecutor(definition_name="AddFoo", execution_uuid=self.uuid, broker=self.broker)

        operation = SagaStepOperation("AddBar", foo_fn)
        context = SagaContext()

        mock = MagicMock(side_effect=self.broker.send)
        self.broker.send = mock
        await executor.exec(operation, context, True)

        self.assertEqual(1, mock.call_count)
        args = call([Foo("hello")], topic="AddBar", saga_uuid=str(self.uuid), reply_topic="AddFoo")
        self.assertEqual(args, mock.call_args)

    async def test_exec_raises(self):
        executor = PublishExecutor(definition_name="AddFoo", execution_uuid=self.uuid, broker=self.broker)
        operation = SagaStepOperation("AddBar", foo_fn)
        context = SagaContext()

        async def _fn(*args, **kwargs):
            raise ValueError("This is an exception")

        mock = MagicMock(side_effect=_fn)
        self.broker.send = mock

        with self.assertRaises(MinosSagaFailedExecutionStepException) as result:
            await executor.exec(operation, context, True)
        self.assertEqual(
            "There was a failure while 'SagaExecutionStep' was executing: ValueError('This is an exception')",
            str(result.exception),
        )


if __name__ == "__main__":
    unittest.main()
