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
    Saga,
    SagaContext,
    SagaExecution,
    SagaExecutionStep,
    SagaStepStatus,
)
from tests.utils import (
    Foo,
    foo_fn,
    foo_fn_raises,
)


class TestSagaExecutionStep(unittest.TestCase):
    def test_execute_invoke_participant(self):
        saga_definition = Saga("FooAdded").step().invoke_participant("FooAdd", foo_fn).commit()
        saga_execution = SagaExecution.from_saga(saga_definition)
        step_execution = SagaExecutionStep(saga_execution, saga_definition.steps[0])

        with patch("minos.saga.executions.executors.publish.PublishExecutor.publish") as mock:
            step_execution.execute(saga_execution.context)
            self.assertEqual(1, mock.call_count)
            self.assertEqual(SagaStepStatus.Finished, step_execution.status)

    def test_execute_invoke_participant_errored(self):
        saga_definition = Saga("FooAdded").step().invoke_participant("FooAdd", foo_fn_raises).commit()
        saga_execution = SagaExecution.from_saga(saga_definition)
        step_execution = SagaExecutionStep(saga_execution, saga_definition.steps[0])

        with patch("minos.saga.executions.executors.publish.PublishExecutor.publish") as mock:
            with self.assertRaises(MinosSagaFailedExecutionStepException):
                step_execution.execute(saga_execution.context)
            self.assertEqual(0, mock.call_count)
            self.assertEqual(SagaStepStatus.ErroredInvokeParticipant, step_execution.status)

    def test_execute_invoke_participant_with_on_reply(self):
        saga_definition = (
            Saga("FooAdded").step().invoke_participant("FooAdd", foo_fn).on_reply("foo", foo_fn_raises).commit()
        )
        saga_execution = SagaExecution.from_saga(saga_definition)
        step_execution = SagaExecutionStep(saga_execution, saga_definition.steps[0])

        with patch("minos.saga.executions.executors.publish.PublishExecutor.publish") as mock:
            with self.assertRaises(MinosSagaPausedExecutionStepException):
                step_execution.execute(saga_execution.context)
            self.assertEqual(SagaStepStatus.PausedOnReply, step_execution.status)

    def test_execute_on_reply(self):
        saga_definition = (
            Saga("FooAdded").step().invoke_participant("FooAdd", foo_fn).on_reply("foo", lambda foo: foo).commit()
        )
        saga_execution = SagaExecution.from_saga(saga_definition)
        step_execution = SagaExecutionStep(saga_execution, saga_definition.steps[0])

        context = step_execution.execute(saga_execution.context, response=Foo("foo"))
        self.assertEqual(SagaContext({"foo": Foo("foo")}), context)
        self.assertEqual(SagaStepStatus.Finished, step_execution.status)

    def test_execute_on_reply_errored(self):
        saga_definition = (
            Saga("FooAdded").step().invoke_participant("FooAdd", foo_fn).on_reply("foo", foo_fn_raises).commit()
        )
        saga_execution = SagaExecution.from_saga(saga_definition)
        step_execution = SagaExecutionStep(saga_execution, saga_definition.steps[0])

        with self.assertRaises(MinosSagaFailedExecutionStepException):
            step_execution.execute(saga_execution.context, response=Foo("foo"))
        self.assertEqual(SagaStepStatus.ErroredOnReply, step_execution.status)


if __name__ == "__main__":
    unittest.main()
