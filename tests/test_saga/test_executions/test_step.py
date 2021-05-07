"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""

import unittest

from minos.saga import (
    MinosSagaFailedExecutionStepException,
    MinosSagaStorage,
    Saga,
    SagaExecution,
    SagaExecutionStep,
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

    def test_execute_raises_failed(self):
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


if __name__ == "__main__":
    unittest.main()
