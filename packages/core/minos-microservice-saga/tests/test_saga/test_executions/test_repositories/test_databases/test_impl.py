import unittest
from shutil import (
    rmtree,
)

from minos.saga import (
    DatabaseSagaExecutionRepository,
    SagaExecution,
    SagaExecutionNotFoundException,
    SagaPausedExecutionStepException,
    SagaResponse,
)
from tests.utils import (
    ADD_ORDER,
    DB_PATH,
    Foo,
    SagaTestCase,
)


class TestDatabaseSagaExecutionRepository(SagaTestCase):
    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()

        execution = SagaExecution.from_definition(ADD_ORDER)
        with self.assertRaises(SagaPausedExecutionStepException):
            await execution.execute()

        response = SagaResponse(Foo("hola"), {"ticket"})
        with self.assertRaises(SagaPausedExecutionStepException):
            await execution.execute(response)

        self.execution = execution

    def tearDown(self) -> None:
        rmtree(DB_PATH, ignore_errors=True)
        super().tearDown()

    async def test_store(self):
        async with DatabaseSagaExecutionRepository(path=DB_PATH) as repository:
            await repository.store(self.execution)

            self.assertEqual(self.execution, await repository.load(self.execution.uuid))

    async def test_store_overwrite(self):
        async with DatabaseSagaExecutionRepository(path=DB_PATH) as repository:
            await repository.store(self.execution)
            self.assertEqual(self.execution, await repository.load(self.execution.uuid))

            another = SagaExecution.from_definition(ADD_ORDER)
            another.uuid = self.execution.uuid
            await repository.store(another)

            self.assertNotEqual(self.execution, await repository.load(self.execution.uuid))
            self.assertEqual(another, await repository.load(self.execution.uuid))

    async def test_load_from_str(self):
        async with DatabaseSagaExecutionRepository(path=DB_PATH) as repository:
            await repository.store(self.execution)
            self.assertEqual(self.execution, await repository.load(str(self.execution.uuid)))

    async def test_load_raises(self):
        async with DatabaseSagaExecutionRepository(path=DB_PATH) as repository:
            with self.assertRaises(SagaExecutionNotFoundException):
                await repository.load(self.execution.uuid)

    async def test_delete(self):
        async with DatabaseSagaExecutionRepository(path=DB_PATH) as repository:
            await repository.store(self.execution)
            await repository.delete(self.execution)
            with self.assertRaises(SagaExecutionNotFoundException):
                await repository.load(self.execution.uuid)

    async def test_delete_from_str(self):
        async with DatabaseSagaExecutionRepository(path=DB_PATH) as repository:
            await repository.store(self.execution)
            await repository.delete(str(self.execution.uuid))
            with self.assertRaises(SagaExecutionNotFoundException):
                await repository.load(self.execution.uuid)


if __name__ == "__main__":
    unittest.main()
