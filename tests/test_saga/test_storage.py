"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""

import unittest
from shutil import (
    rmtree,
)

from minos.saga import (
    MinosSagaStorage,
)
from tests.utils import (
    BASE_PATH,
)


class TestMinosSagaStorage(unittest.TestCase):
    DB_PATH = BASE_PATH / "test_db.lmdb"

    def tearDown(self) -> None:
        rmtree(self.DB_PATH, ignore_errors=True)

    def test_constructor(self):
        storage = MinosSagaStorage("foo", "uuid4", self.DB_PATH)
        self.assertEqual("uuid4", storage.uuid)
        self.assertEqual("foo", storage.saga_name)


if __name__ == "__main__":
    unittest.main()
