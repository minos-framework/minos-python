import shutil
import unittest

from minos.common import (
    MinosStorageLmdb,
)
from tests.utils import (
    BASE_PATH,
)


class TestMinosStorageLmdb(unittest.TestCase):
    def setUp(self) -> None:
        self.path = BASE_PATH / "order.lmdb"

    def tearDown(self) -> None:
        shutil.rmtree(self.path, ignore_errors=True)

    def test_storage_add_text(self):
        storage = MinosStorageLmdb.build(self.path)
        storage.add("TestOne", "first", "Text Value")

        value_returned = storage.get("TestOne", "first")
        assert value_returned == "Text Value"

    def test_storage_add_int(self):
        storage = MinosStorageLmdb.build(self.path)
        storage.add("TestOne", "first", 123)

        value_returned = storage.get("TestOne", "first")
        assert value_returned == 123

    def test_storage_add_dict(self):
        storage = MinosStorageLmdb.build(self.path)
        storage.add("TestOne", "first", {"key_one": "hello", "key_two": "minos"})

        value_returned = storage.get("TestOne", "first")
        assert value_returned["key_one"] == "hello"
        assert value_returned["key_two"] == "minos"

    def test_storage_add_multi_dict(self):
        storage = MinosStorageLmdb.build(self.path)
        storage.add("TestOne", "first", {"key_one": "hello", "key_two": {"sub_key": "this is a sub text"}})

        value_returned = storage.get("TestOne", "first")
        assert value_returned["key_one"] == "hello"
        assert value_returned["key_two"]["sub_key"] == "this is a sub text"

    def test_storage_add_list(self):
        storage = MinosStorageLmdb.build(self.path)
        storage.add("TestOne", "first", ["hello", "minos"])

        value_returned = storage.get("TestOne", "first")
        assert value_returned[0] == "hello"
        assert value_returned[1] == "minos"

    def test_storage_add_multi_table(self):
        storage = MinosStorageLmdb.build(self.path)
        storage.add("TestOne", "first", "Text Value")
        storage.add("TestTwo", "first_double", "Text Double Value")
        storage.add("TestTwo", "first", "Text Value Diff")

        value_returned = storage.get("TestOne", "first")
        assert value_returned == "Text Value"

        value_returned = storage.get("TestTwo", "first_double")
        assert value_returned == "Text Double Value"

        value_returned = storage.get("TestTwo", "first")
        assert value_returned == "Text Value Diff"

    def test_storage_delete(self):
        storage = MinosStorageLmdb.build(self.path)
        storage.add("TestOne", "first", "Text Value")
        storage.add("TestOne", "second", "Text Second Value")

        storage.delete("TestOne", "first")
        value_returned = storage.get("TestOne", "second")
        assert value_returned == "Text Second Value"

        exception_value = storage.get("TestOne", "first")
        assert exception_value is None

    def test_storage_update(self):
        storage = MinosStorageLmdb.build(self.path)
        storage.add("TestOne", "first", "Text Value")

        value_returned = storage.get("TestOne", "first")
        assert value_returned == "Text Value"

        storage.update("TestOne", "first", "Updated Text Value")
        updated_value = storage.get("TestOne", "first")
        assert updated_value == "Updated Text Value"


if __name__ == "__main__":
    unittest.main()
