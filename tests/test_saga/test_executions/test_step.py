"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""

import unittest
from unittest.mock import (
    patch,
)

from minos.saga import (
    MinosSagaFailedExecutionStepException,
    MinosSagaPausedExecutionStepException,
    SagaContext,
    SagaExecutionStep,
    SagaStep,
    SagaStepStatus,
)
from tests.utils import (
    Foo,
    foo_fn,
    foo_fn_raises,
)

_PUBLISH_MOCKER = patch("minos.saga.executions.executors.publish.PublishExecutor.publish")


class TestSagaExecutionStep(unittest.TestCase):
    def test_execute_invoke_participant(self):
        step = SagaStep().invoke_participant("FooAdd", foo_fn)
        context = SagaContext()
        execution = SagaExecutionStep(step)

        with _PUBLISH_MOCKER as mock:
            execution.execute(context)
            self.assertEqual(1, mock.call_count)

        self.assertEqual(SagaStepStatus.Finished, execution.status)

    def test_execute_invoke_participant_errored(self):
        step = SagaStep().invoke_participant("FooAdd", foo_fn_raises)
        context = SagaContext()
        execution = SagaExecutionStep(step)

        with _PUBLISH_MOCKER as mock:
            with self.assertRaises(MinosSagaFailedExecutionStepException):
                execution.execute(context)
            self.assertEqual(0, mock.call_count)

        self.assertEqual(SagaStepStatus.ErroredInvokeParticipant, execution.status)

    def test_execute_invoke_participant_with_on_reply(self):
        step = SagaStep().invoke_participant("FooAdd", foo_fn).on_reply("foo", lambda foo: foo)
        context = SagaContext()
        execution = SagaExecutionStep(step)

        with _PUBLISH_MOCKER as mock:
            with self.assertRaises(MinosSagaPausedExecutionStepException):
                execution.execute(context)
            self.assertEqual(1, mock.call_count)

            self.assertEqual(SagaStepStatus.PausedOnReply, execution.status)

        execution.execute(context, response=Foo("foo"))
        self.assertEqual(SagaStepStatus.Finished, execution.status)

    def test_execute_on_reply(self):
        step = SagaStep().invoke_participant("FooAdd", foo_fn).on_reply("foo", lambda foo: foo)
        context = SagaContext()
        execution = SagaExecutionStep(step)

        context = execution.execute(context, response=Foo("foo"))
        self.assertEqual(SagaContext({"foo": Foo("foo")}), context)
        self.assertEqual(SagaStepStatus.Finished, execution.status)

    def test_execute_on_reply_errored(self):
        step = SagaStep().invoke_participant("FooAdd", foo_fn).on_reply("foo", foo_fn_raises)
        context = SagaContext()
        execution = SagaExecutionStep(step)

        with self.assertRaises(MinosSagaFailedExecutionStepException):
            execution.execute(context, response=Foo("foo"))

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

        with _PUBLISH_MOCKER as mock:
            execution.rollback(context)
            self.assertEqual(1, mock.call_count)

            mock.reset_mock()
            execution.rollback(context)
            self.assertEqual(0, mock.call_count)


if __name__ == "__main__":
    unittest.main()
