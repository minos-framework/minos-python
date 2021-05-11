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
    MinosSagaRollbackExecutionStepException,
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
        step = SagaStep().invoke_participant("FooAdd", foo_fn_raises).with_compensation("FooDelete", foo_fn)
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
            with self.assertRaises(MinosSagaRollbackExecutionStepException):
                execution.rollback(context)
            self.assertEqual(0, mock.call_count)

            try:
                execution.execute(context)
            except MinosSagaPausedExecutionStepException:
                pass
            self.assertEqual(1, mock.call_count)
            mock.reset_mock()

            execution.rollback(context)
            self.assertEqual(1, mock.call_count)

            mock.reset_mock()
            with self.assertRaises(MinosSagaRollbackExecutionStepException):
                execution.rollback(context)
            self.assertEqual(0, mock.call_count)

    def test_raw(self):
        from minos.saga import (
            identity_fn,
        )

        definition = (
            SagaStep().invoke_participant("CreateFoo", foo_fn).with_compensation("DeleteFoo", foo_fn).on_reply("foo")
        )
        execution = SagaExecutionStep(definition)

        expected = {
            "already_rollback": False,
            "definition": {
                "raw_invoke_participant": {"callback": foo_fn, "name": "CreateFoo"},
                "raw_on_reply": {"callback": identity_fn, "name": "foo"},
                "raw_with_compensation": {"callback": foo_fn, "name": "DeleteFoo"},
            },
            "status": "created",
        }
        self.assertEqual(expected, execution.raw)

    def test_from_raw(self):
        from minos.saga import (
            identity_fn,
        )

        raw = {
            "already_rollback": False,
            "definition": {
                "raw_invoke_participant": {"callback": foo_fn, "name": "CreateFoo"},
                "raw_on_reply": {"callback": identity_fn, "name": "foo"},
                "raw_with_compensation": {"callback": foo_fn, "name": "DeleteFoo"},
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
