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
    MinosSagaStorage,
    Saga,
    SagaContext,
    SagaExecution,
    SagaExecutionStep,
    SagaStepStatus,
)
from tests.utils import (
    BASE_PATH,
    Foo,
    foo_fn,
)


# noinspection PyUnusedLocal
def _fn_exception(response):
    raise ValueError()


class TestSagaExecutionStep(unittest.TestCase):
    DB_PATH = BASE_PATH / "test_db.lmdb"

    def test_execute_invoke_participant(self):
        saga_definition = Saga("FooAdded", self.DB_PATH).step().invoke_participant("FooAdd", foo_fn).commit()
        saga_execution = SagaExecution.from_saga(saga_definition)
        step_execution = SagaExecutionStep(saga_execution, saga_definition.steps[0])

        with patch("minos.saga.executions.executors.publish.PublishExecutor.publish") as mock:
            with MinosSagaStorage.from_execution(saga_execution) as storage:
                step_execution.execute(saga_execution.context, storage)
                self.assertEqual(1, mock.call_count)
                self.assertEqual(SagaStepStatus.Finished, step_execution.status)

    def test_execute_invoke_participant_errored(self):
        saga_definition = Saga("FooAdded", self.DB_PATH).step().invoke_participant("FooAdd", _fn_exception).commit()
        saga_execution = SagaExecution.from_saga(saga_definition)
        step_execution = SagaExecutionStep(saga_execution, saga_definition.steps[0])

        with patch("minos.saga.executions.executors.publish.PublishExecutor.publish") as mock:
            with MinosSagaStorage.from_execution(saga_execution) as storage:
                with self.assertRaises(MinosSagaFailedExecutionStepException):
                    step_execution.execute(saga_execution.context, storage)
                self.assertEqual(0, mock.call_count)
                self.assertEqual(SagaStepStatus.ErroredInvokeParticipant, step_execution.status)

    def test_execute_invoke_participant_with_on_reply(self):
        saga_definition = (
            Saga("FooAdded", self.DB_PATH)
            .step()
            .invoke_participant("FooAdd", foo_fn)
            .on_reply("foo", _fn_exception)
            .commit()
        )
        saga_execution = SagaExecution.from_saga(saga_definition)
        step_execution = SagaExecutionStep(saga_execution, saga_definition.steps[0])

        with patch("minos.saga.executions.executors.publish.PublishExecutor.publish") as mock:
            with MinosSagaStorage.from_execution(saga_execution) as storage:
                with self.assertRaises(MinosSagaPausedExecutionStepException):
                    step_execution.execute(saga_execution.context, storage)
                self.assertEqual(SagaStepStatus.PausedOnReply, step_execution.status)

    def test_execute_on_reply(self):
        saga_definition = (
            Saga("FooAdded", self.DB_PATH)
            .step()
            .invoke_participant("FooAdd", foo_fn)
            .on_reply("foo", lambda foo: foo)
            .commit()
        )
        saga_execution = SagaExecution.from_saga(saga_definition)
        step_execution = SagaExecutionStep(saga_execution, saga_definition.steps[0])

        with MinosSagaStorage.from_execution(saga_execution) as storage:
            context = step_execution.execute(saga_execution.context, storage, response=Foo("foo"))
            self.assertEqual(SagaContext(content={"foo": Foo("foo")}), context)
            self.assertEqual(SagaStepStatus.Finished, step_execution.status)

    def test_execute_on_reply_errored(self):
        saga_definition = (
            Saga("FooAdded", self.DB_PATH)
            .step()
            .invoke_participant("FooAdd", foo_fn)
            .on_reply("foo", _fn_exception)
            .commit()
        )
        saga_execution = SagaExecution.from_saga(saga_definition)
        step_execution = SagaExecutionStep(saga_execution, saga_definition.steps[0])

        with MinosSagaStorage.from_execution(saga_execution) as storage:
            with self.assertRaises(MinosSagaFailedExecutionStepException):
                step_execution.execute(saga_execution.context, storage, response=Foo("foo"))
            self.assertEqual(SagaStepStatus.ErroredOnReply, step_execution.status)


if __name__ == "__main__":
    unittest.main()
