"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import unittest
from pathlib import (
    Path,
)

from minos.common import (
    CommandStatus,
)
from minos.saga import (
    LocalExecutor,
    MinosCommandReplyFailedException,
    MinosSagaFailedExecutionStepException,
    OnReplyExecutor,
    SagaContext,
    SagaOperation,
    identity_fn,
)
from tests.utils import (
    Foo,
    fake_reply,
)


class TesOnReplyExecutor(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.executor = OnReplyExecutor()

    def test_constructor(self):
        self.assertIsInstance(self.executor, LocalExecutor)

    async def test_exec_raises_callback(self):
        operation = SagaOperation(lambda s: Path.cwd(), "foo")
        with self.assertRaises(MinosSagaFailedExecutionStepException):
            await self.executor.exec(operation, SagaContext(), reply=fake_reply(Foo("text")))

    async def test_exec_raises_reply_status(self):
        reply = fake_reply(status=CommandStatus.ERROR)
        operation = SagaOperation(identity_fn, "foo")
        with self.assertRaises(MinosCommandReplyFailedException):
            await self.executor.exec(operation, SagaContext(), reply=reply)


if __name__ == "__main__":
    unittest.main()
