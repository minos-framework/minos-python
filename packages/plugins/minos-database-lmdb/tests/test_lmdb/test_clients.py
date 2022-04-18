import shutil
import unittest
from pathlib import (
    Path,
)

from minos.common import (
    DatabaseClient,
    DatabaseOperation,
)
from minos.plugins.lmdb import (
    LmdbDatabaseClient,
    LmdbDatabaseOperation,
    LmdbDatabaseOperationType,
)
from tests.utils import (
    BASE_PATH,
)


class TestLmdbDatabaseClient(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.path = BASE_PATH / "order.lmdb"

    def test_subclass(self) -> None:
        self.assertTrue(issubclass(LmdbDatabaseClient, DatabaseClient))

    def tearDown(self) -> None:
        shutil.rmtree(self.path, ignore_errors=True)
        shutil.rmtree(".lmdb", ignore_errors=True)

    async def test_constructor_default_path(self):
        async with LmdbDatabaseClient():
            self.assertTrue(Path(".lmdb").exists())

    async def test_is_valid(self):
        async with LmdbDatabaseClient(self.path) as client:
            self.assertTrue(await client.is_valid())

    async def test_execute_raises_unsupported(self):
        class _DatabaseOperation(DatabaseOperation):
            """For testing purposes."""

        async with LmdbDatabaseClient(self.path) as client:
            with self.assertRaises(ValueError):
                await client.execute(_DatabaseOperation())

    async def test_execute_create_text(self):
        create_op = LmdbDatabaseOperation(LmdbDatabaseOperationType.CREATE, "TestOne", "first", "Text Value")
        read_op = LmdbDatabaseOperation(LmdbDatabaseOperationType.READ, "TestOne", "first")

        async with LmdbDatabaseClient(self.path) as client:
            await client.execute(create_op)
            await client.execute(read_op)

            self.assertEqual("Text Value", await client.fetch_one())

    async def test_execute_create_int(self):
        create_op = LmdbDatabaseOperation(LmdbDatabaseOperationType.CREATE, "TestOne", "first", 123)
        read_op = LmdbDatabaseOperation(LmdbDatabaseOperationType.READ, "TestOne", "first")

        async with LmdbDatabaseClient(self.path) as client:
            await client.execute(create_op)
            await client.execute(read_op)

            self.assertEqual(123, await client.fetch_one())

    async def test_execute_create_dict(self):
        create_op = LmdbDatabaseOperation(
            LmdbDatabaseOperationType.CREATE, "TestOne", "first", {"key_one": "hello", "key_two": "minos"}
        )
        read_op = LmdbDatabaseOperation(LmdbDatabaseOperationType.READ, "TestOne", "first")

        async with LmdbDatabaseClient(self.path) as client:
            await client.execute(create_op)
            await client.execute(read_op)

            self.assertEqual({"key_one": "hello", "key_two": "minos"}, await client.fetch_one())

    async def test_execute_create_multi_dict(self):
        create_op = LmdbDatabaseOperation(
            LmdbDatabaseOperationType.CREATE,
            "TestOne",
            "first",
            {"key_one": "hello", "key_two": {"sub_key": "this is a sub text"}},
        )
        read_op = LmdbDatabaseOperation(LmdbDatabaseOperationType.READ, "TestOne", "first")

        async with LmdbDatabaseClient(self.path) as client:
            await client.execute(create_op)
            await client.execute(read_op)

            self.assertEqual(
                {"key_one": "hello", "key_two": {"sub_key": "this is a sub text"}}, await client.fetch_one()
            )

    async def test_execute_create_list(self):
        create_op = LmdbDatabaseOperation(LmdbDatabaseOperationType.CREATE, "TestOne", "first", ["hello", "minos"])
        read_op = LmdbDatabaseOperation(LmdbDatabaseOperationType.READ, "TestOne", "first")

        async with LmdbDatabaseClient(self.path) as client:
            await client.execute(create_op)
            await client.execute(read_op)

            self.assertEqual(["hello", "minos"], await client.fetch_one())

    async def test_execute_create_multi_table(self):
        create_op_1 = LmdbDatabaseOperation(LmdbDatabaseOperationType.CREATE, "TestOne", "first", "Text Value")
        create_op_2 = LmdbDatabaseOperation(
            LmdbDatabaseOperationType.CREATE, "TestTwo", "first_double", "Text Double Value"
        )
        create_op_3 = LmdbDatabaseOperation(LmdbDatabaseOperationType.CREATE, "TestTwo", "first", "Text Value Diff")

        read_op_1 = LmdbDatabaseOperation(LmdbDatabaseOperationType.READ, "TestOne", "first")
        read_op_2 = LmdbDatabaseOperation(LmdbDatabaseOperationType.READ, "TestTwo", "first_double")
        read_op_3 = LmdbDatabaseOperation(LmdbDatabaseOperationType.READ, "TestTwo", "first")

        async with LmdbDatabaseClient(self.path) as client:
            await client.execute(create_op_1)
            await client.execute(create_op_2)
            await client.execute(create_op_3)

            await client.execute(read_op_1)
            self.assertEqual("Text Value", await client.fetch_one())

            await client.execute(read_op_2)
            self.assertEqual("Text Double Value", await client.fetch_one())

            await client.execute(read_op_3)
            self.assertEqual("Text Value Diff", await client.fetch_one())

    async def test_execute_delete(self):
        create_op_1 = LmdbDatabaseOperation(LmdbDatabaseOperationType.CREATE, "TestOne", "first", "Text Value")
        create_op_2 = LmdbDatabaseOperation(LmdbDatabaseOperationType.CREATE, "TestOne", "second", "Text Second Value")
        delete_op_1 = LmdbDatabaseOperation(LmdbDatabaseOperationType.DELETE, "TestOne", "first")
        read_op_1 = LmdbDatabaseOperation(LmdbDatabaseOperationType.READ, "TestOne", "second")
        read_op_2 = LmdbDatabaseOperation(LmdbDatabaseOperationType.READ, "TestOne", "first")

        async with LmdbDatabaseClient(self.path) as client:
            await client.execute(create_op_1)
            await client.execute(create_op_2)
            await client.execute(delete_op_1)

            await client.execute(read_op_1)
            self.assertEqual("Text Second Value", await client.fetch_one())

            await client.execute(read_op_2)
            self.assertEqual(None, await client.fetch_one())

    async def test_execute_update(self):
        create_op = LmdbDatabaseOperation(LmdbDatabaseOperationType.CREATE, "TestOne", "first", "Text Value")
        update_op = LmdbDatabaseOperation(LmdbDatabaseOperationType.UPDATE, "TestOne", "first", "Updated Text Value")
        read_op = LmdbDatabaseOperation(LmdbDatabaseOperationType.READ, "TestOne", "first")

        async with LmdbDatabaseClient(self.path) as client:
            await client.execute(create_op)
            await client.execute(read_op)

            self.assertEqual("Text Value", await client.fetch_one())

            await client.execute(update_op)
            await client.execute(read_op)

            self.assertEqual("Updated Text Value", await client.fetch_one())


if __name__ == "__main__":
    unittest.main()
