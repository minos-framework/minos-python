import shutil

import pytest
from minos.common.storage.lmdb import MinosStorageLmdb

@pytest.fixture
def path():
    return "./tests/test_db.lmdb"

@pytest.fixture(autouse=True)
def clear_database(path):
    yield
    # Code that will run after your test, for example:
    shutil.rmtree(path, ignore_errors=True)



def test_storage_add_text(path):
    minosEnv = MinosStorageLmdb.build(path)
    minosEnv.add("TestOne", "first", "Text Value")

    value_returned = minosEnv.get("TestOne", "first")
    assert value_returned == "Text Value"


def test_storage_add_int(path):
    minosEnv = MinosStorageLmdb.build(path)
    minosEnv.add("TestOne", "first", 123)

    value_returned = minosEnv.get("TestOne", "first")
    assert value_returned == 123


def test_storage_add_dict(path):
    minosEnv = MinosStorageLmdb.build(path)
    minosEnv.add("TestOne", "first", {"key_one": "hello", "key_two": "minos"})

    value_returned = minosEnv.get("TestOne", "first")
    assert value_returned["key_one"] == "hello"
    assert value_returned["key_two"] == "minos"


def test_storage_add_multi_dict(path):
    minosEnv = MinosStorageLmdb.build(path)
    minosEnv.add("TestOne", "first", {"key_one": "hello", "key_two": {"sub_key": "this is a sub text"}})

    value_returned = minosEnv.get("TestOne", "first")
    assert value_returned["key_one"] == "hello"
    assert value_returned["key_two"]["sub_key"] == "this is a sub text"


def test_storage_add_list(path):
    minosEnv = MinosStorageLmdb.build(path)
    minosEnv.add("TestOne", "first", ["hello", "minos"])

    value_returned = minosEnv.get("TestOne", "first")
    assert value_returned[0] == "hello"
    assert value_returned[1] == "minos"


def test_storage_add_multi_table(path):
    minosEnv = MinosStorageLmdb.build(path)
    minosEnv.add("TestOne", "first", "Text Value")
    minosEnv.add("TestTwo", "first_double", "Text Double Value")
    minosEnv.add("TestTwo", "first", "Text Value Diff")

    value_returned = minosEnv.get("TestOne", "first")
    assert value_returned == "Text Value"

    value_returned = minosEnv.get("TestTwo", "first_double")
    assert value_returned == "Text Double Value"

    value_returned = minosEnv.get("TestTwo", "first")
    assert value_returned == "Text Value Diff"


def test_storage_delete(path):
    minosEnv = MinosStorageLmdb.build(path)
    minosEnv.add("TestOne", "first", "Text Value")
    minosEnv.add("TestOne", "second", "Text Second Value")

    minosEnv.delete("TestOne", "first")
    value_returned = minosEnv.get("TestOne", "second")
    assert value_returned == "Text Second Value"

    exception_value = minosEnv.get("TestOne", "first")
    assert exception_value is None


def test_storage_update(path):
    minosEnv = MinosStorageLmdb.build(path)
    minosEnv.add("TestOne", "first", "Text Value")

    value_returned = minosEnv.get("TestOne", "first")
    assert value_returned == "Text Value"

    minosEnv.update("TestOne", "first", "Updated Text Value")
    updated_value = minosEnv.get("TestOne", "first")
    assert updated_value == "Updated Text Value"
