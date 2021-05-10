"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""

import unittest
from shutil import (
    rmtree,
)

from minos.common import (
    MinosStorageLmdb,
)
from minos.saga import (
    MinosLocalState,
)
from tests.utils import (
    BASE_PATH,
)


class TestMinosLocalState(unittest.TestCase):
    DB_PATH = BASE_PATH / "test_db.lmdb"

    def tearDown(self) -> None:
        rmtree(self.DB_PATH, ignore_errors=True)

    def test_add_get(self):
        storage = MinosLocalState(storage_cls=MinosStorageLmdb, db_path=self.DB_PATH)
        storage.add("foo", "bar")
        self.assertEqual("bar", storage.get("foo"))

    def test_update_get(self):
        storage = MinosLocalState(storage_cls=MinosStorageLmdb, db_path=self.DB_PATH)
        storage.add("foo", "bar")
        storage.update("foo", "foobar")
        self.assertEqual("foobar", storage.get("foo"))

    def test_add_delete(self):
        storage = MinosLocalState(storage_cls=MinosStorageLmdb, db_path=self.DB_PATH)
        storage.add("foo", "bar")
        storage.delete("foo")


if __name__ == "__main__":
    unittest.main()
