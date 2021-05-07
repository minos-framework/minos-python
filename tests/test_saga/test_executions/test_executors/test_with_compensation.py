"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import unittest
from asyncio import (
    AbstractEventLoop,
)

from minos.saga import (
    WithCompensationExecutor,
)


class TesWithCompensationExecutor(unittest.TestCase):
    def test_constructor(self):
        executor = WithCompensationExecutor()
        self.assertIsInstance(executor.loop, AbstractEventLoop)


if __name__ == "__main__":
    unittest.main()
