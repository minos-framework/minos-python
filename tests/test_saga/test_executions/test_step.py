"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""

import unittest
import uuid
from unittest.mock import (
    MagicMock,
)

from dependency_injector import (
    containers,
    providers,
)

from minos.common import (
    MinosConfig,
)
from minos.saga import (
    MinosSagaFailedExecutionStepException,
    MinosSagaPausedExecutionStepException,
    MinosSagaRollbackExecutionStepException,
    SagaContext,
    SagaExecutionStep,
    SagaStep,
    SagaStepStatus,
)
from tests.utils import (
    BASE_PATH,
    Foo,
    NaiveBroker,
    fake_reply,
    foo_fn,
    foo_fn_raises,
)


class TestSagaExecutionStep(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.config = MinosConfig(path=BASE_PATH / "config.yml")
        self.broker = NaiveBroker()
        self.execute_kwargs = {"definition_name": "FoodAdd", "execution_uuid": uuid.uuid4()}

        self.publish_mock = MagicMock(side_effect=self.broker.send_one)
        self.broker.send_one = self.publish_mock

        self.container = containers.DynamicContainer()
        self.container.config = providers.Object(self.config)
        self.container.command_broker = providers.Object(self.broker)

        from minos import (
            saga,
        )

        self.container.wire(modules=[saga])

    def tearDown(self) -> None:
        self.container.unwire()

    def test_execute_invoke_participant(self):
        step = SagaStep().invoke_participant("FooAdd", foo_fn)
        context = SagaContext()
        execution = SagaExecutionStep(step)

        execution.execute(context, broker=self.broker, **self.execute_kwargs)
        self.assertEqual(1, self.publish_mock.call_count)

        self.assertEqual(SagaStepStatus.Finished, execution.status)

    def test_execute_invoke_participant_errored(self):
        step = SagaStep().invoke_participant("FooAdd", foo_fn_raises).with_compensation("FooDelete", foo_fn)
        context = SagaContext()
        execution = SagaExecutionStep(step)

        with self.assertRaises(MinosSagaFailedExecutionStepException):
            execution.execute(context, **self.execute_kwargs)
        self.assertEqual(0, self.publish_mock.call_count)

        self.assertEqual(SagaStepStatus.ErroredInvokeParticipant, execution.status)

    def test_execute_invoke_participant_with_on_reply(self):
        step = SagaStep().invoke_participant("FooAdd", foo_fn).on_reply("foo", lambda foo: foo)
        context = SagaContext()
        execution = SagaExecutionStep(step)

        with self.assertRaises(MinosSagaPausedExecutionStepException):
            execution.execute(context, broker=self.broker, **self.execute_kwargs)
        self.assertEqual(1, self.publish_mock.call_count)

        self.assertEqual(SagaStepStatus.PausedOnReply, execution.status)

        reply = fake_reply(Foo("foo"))
        execution.execute(context, reply=reply, broker=self.broker)
        self.assertEqual(SagaStepStatus.Finished, execution.status)

    def test_execute_on_reply(self):
        step = SagaStep().invoke_participant("FooAdd", foo_fn).on_reply("foo", lambda foo: foo)
        context = SagaContext()
        execution = SagaExecutionStep(step)

        reply = fake_reply(Foo("foo"))
        context = execution.execute(context, reply=reply, **self.execute_kwargs)
        self.assertEqual(SagaContext(foo=Foo("foo")), context)
        self.assertEqual(SagaStepStatus.Finished, execution.status)

    def test_execute_on_reply_errored(self):
        step = SagaStep().invoke_participant("FooAdd", foo_fn).on_reply("foo", foo_fn_raises)
        context = SagaContext()
        execution = SagaExecutionStep(step)

        reply = fake_reply(Foo("foo"))
        with self.assertRaises(MinosSagaFailedExecutionStepException):
            execution.execute(context, reply=reply, **self.execute_kwargs)

        self.assertEqual(SagaStepStatus.ErroredOnReply, execution.status)

    def test_rollback(self):
        step = (
            SagaStep()
            .invoke_participant("FooAdd", foo_fn)
            .with_compensation("FooDelete", foo_fn)
            .on_reply("foo", foo_fn_raises)
        )
        context = SagaContext()
        execution = SagaExecutionStep(step)

        with self.assertRaises(MinosSagaRollbackExecutionStepException):
            execution.rollback(context, broker=self.broker, **self.execute_kwargs)
        self.assertEqual(0, self.publish_mock.call_count)

        try:
            execution.execute(context, broker=self.broker, **self.execute_kwargs)
        except MinosSagaPausedExecutionStepException:
            pass
        self.assertEqual(1, self.publish_mock.call_count)
        self.publish_mock.reset_mock()

        execution.rollback(context, broker=self.broker, **self.execute_kwargs)
        self.assertEqual(1, self.publish_mock.call_count)

        self.publish_mock.reset_mock()
        with self.assertRaises(MinosSagaRollbackExecutionStepException):
            execution.rollback(context, broker=self.broker, **self.execute_kwargs)
        self.assertEqual(0, self.publish_mock.call_count)

    def test_raw(self):
        definition = (
            SagaStep().invoke_participant("CreateFoo", foo_fn).with_compensation("DeleteFoo", foo_fn).on_reply("foo")
        )
        execution = SagaExecutionStep(definition)

        expected = {
            "already_rollback": False,
            "definition": {
                "invoke_participant": {"callback": "tests.utils.foo_fn", "name": "CreateFoo"},
                "on_reply": {"callback": "minos.saga.definitions.step.identity_fn", "name": "foo"},
                "with_compensation": {"callback": "tests.utils.foo_fn", "name": "DeleteFoo"},
            },
            "status": "created",
        }
        self.assertEqual(expected, execution.raw)

    def test_from_raw(self):

        raw = {
            "already_rollback": False,
            "definition": {
                "invoke_participant": {"callback": "tests.utils.foo_fn", "name": "CreateFoo"},
                "on_reply": {"callback": "minos.saga.definitions.step.identity_fn", "name": "foo"},
                "with_compensation": {"callback": "tests.utils.foo_fn", "name": "DeleteFoo"},
            },
            "status": "created",
        }
        expected = SagaExecutionStep(
            (SagaStep().invoke_participant("CreateFoo", foo_fn).with_compensation("DeleteFoo", foo_fn).on_reply("foo")),
        )
        observed = SagaExecutionStep.from_raw(raw)
        self.assertEqual(expected, observed)

    def test_from_raw_already(self):
        expected = SagaExecutionStep(
            (SagaStep().invoke_participant("CreateFoo", foo_fn).with_compensation("DeleteFoo", foo_fn).on_reply("foo")),
        )
        observed = SagaExecutionStep.from_raw(expected)
        self.assertEqual(expected, observed)


if __name__ == "__main__":
    unittest.main()
