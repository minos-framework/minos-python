"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import unittest
from pathlib import (
    Path,
)

from minos.saga import (
    LocalExecutor,
    MinosSagaFailedExecutionStepException,
    OnReplyExecutor,
    SagaContext,
    SagaStepOperation,
)
from tests.utils import (
    Foo,
    fake_reply,
)


class TesOnReplyExecutor(unittest.IsolatedAsyncioTestCase):
    def test_constructor(self):
        executor = OnReplyExecutor()
        self.assertIsInstance(executor, LocalExecutor)

    async def test_exec_raises(self):
        executor = OnReplyExecutor()
        operation = SagaStepOperation("foo", lambda s: Path.cwd())
        with self.assertRaises(MinosSagaFailedExecutionStepException):
            await executor.exec(operation, SagaContext(), reply=fake_reply(Foo("text")))


if __name__ == "__main__":
    unittest.main()
