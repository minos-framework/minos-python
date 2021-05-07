"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import unittest
from asyncio import (
    AbstractEventLoop,
)
from shutil import (
    rmtree,
)

from minos.saga import (
    LocalExecutor,
    MinosSagaStorage,
)
from tests.utils import (
    BASE_PATH,
)


class TestLocalExecutor(unittest.TestCase):
    DB_PATH = BASE_PATH / "test_db.lmdb"

    def tearDown(self) -> None:
        rmtree(self.DB_PATH, ignore_errors=True)

    def setUp(self) -> None:
        self.storage = MinosSagaStorage("foo", "uuid4", self.DB_PATH)

    def test_constructor(self):
        executor = LocalExecutor(self.storage)
        self.assertEqual(self.storage, executor.storage)
        self.assertIsInstance(executor.loop, AbstractEventLoop)


if __name__ == "__main__":
    unittest.main()
