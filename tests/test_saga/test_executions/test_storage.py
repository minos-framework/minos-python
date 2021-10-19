import unittest
from shutil import (
    rmtree,
)

from minos.saga import (
    SagaExecution,
    SagaExecutionNotFoundException,
    SagaExecutionStorage,
    SagaPausedExecutionStepException,
)
from tests.utils import (
    ADD_ORDER,
    BASE_PATH,
    Foo,
    NaiveBroker,
    fake_reply,
)


class TestSagaExecutionStorage(unittest.IsolatedAsyncioTestCase):
    DB_PATH = BASE_PATH / "test_db.lmdb"

    async def asyncSetUp(self) -> None:
        self.broker = NaiveBroker()

        execution = SagaExecution.from_definition(ADD_ORDER)
        with self.assertRaises(SagaPausedExecutionStepException):
            await execution.execute(broker=self.broker)

        reply = fake_reply(Foo("hola"))
        with self.assertRaises(SagaPausedExecutionStepException):
            await execution.execute(reply=reply, broker=self.broker)

        self.execution = execution

    def tearDown(self) -> None:
        rmtree(self.DB_PATH, ignore_errors=True)

    def test_store(self):
        storage = SagaExecutionStorage(path=self.DB_PATH)

        storage.store(self.execution)

        self.assertEqual(self.execution, storage.load(self.execution.uuid))

    def test_store_overwrite(self):
        storage = SagaExecutionStorage(path=self.DB_PATH)

        storage.store(self.execution)
        self.assertEqual(self.execution, storage.load(self.execution.uuid))

        another = SagaExecution.from_definition(ADD_ORDER)
        another.uuid = self.execution.uuid
        storage.store(another)

        self.assertNotEqual(self.execution, storage.load(self.execution.uuid))
        self.assertEqual(another, storage.load(self.execution.uuid))

    def test_load_raises(self):
        storage = SagaExecutionStorage(path=self.DB_PATH)

        with self.assertRaises(SagaExecutionNotFoundException):
            storage.load(self.execution.uuid)

    def test_delete(self):
        storage = SagaExecutionStorage(path=self.DB_PATH)

        storage.store(self.execution)
        storage.delete(self.execution)
        with self.assertRaises(SagaExecutionNotFoundException):
            storage.load(self.execution.uuid)


if __name__ == "__main__":
    unittest.main()
