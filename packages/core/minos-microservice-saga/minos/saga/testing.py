from abc import (
    ABC,
    abstractmethod,
)
from uuid import (
    UUID,
)

from minos.common import (
    DatabaseOperation,
    DeclarativeModel,
)
from minos.common.testing import (
    MinosTestCase,
    MockedDatabaseClient,
    MockedDatabaseOperation,
)

from .context import (
    SagaContext,
)
from .definitions import (
    Saga,
)
from .exceptions import (
    SagaExecutionNotFoundException,
)
from .executions import (
    SagaExecution,
    SagaExecutionDatabaseOperationFactory,
    SagaExecutionRepository,
)


class MockedSagaExecutionDatabaseOperationFactory(SagaExecutionDatabaseOperationFactory):
    """For testing purposes"""

    def build_store(self, uuid: UUID, **kwargs) -> DatabaseOperation:
        """For testing purposes"""
        return MockedDatabaseOperation("create_table")

    def build_load(self, uuid: UUID) -> DatabaseOperation:
        """For testing purposes"""
        return MockedDatabaseOperation("create_table")

    def build_delete(self, uuid: UUID) -> DatabaseOperation:
        """For testing purposes"""
        return MockedDatabaseOperation("create_table")


MockedDatabaseClient.set_factory(
    SagaExecutionDatabaseOperationFactory,
    MockedSagaExecutionDatabaseOperationFactory,
)


class Foo(DeclarativeModel):
    """For testing purposes."""

    foo: str


def _fn1(context: SagaContext) -> SagaContext:  # pragma: no cover
    context["payment"] = "payment"
    return context


def _fn2(context: SagaContext) -> SagaContext:  # pragma: no cover
    context["payment"] = None
    return context


_SAGA = Saga().local_step(_fn1).on_failure(_fn2).commit()


class SagaExecutionRepositoryTestCase(MinosTestCase, ABC):
    __test__ = False

    def setUp(self) -> None:
        super().setUp()
        self.saga_execution_repository = self.build_saga_execution_repository()

    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()

        await self.saga_execution_repository.setup()

        execution = SagaExecution.from_definition(_SAGA)
        await execution.execute(autocommit=False)

        self.execution = execution

        self.another = SagaExecution.from_definition(_SAGA)

    async def asyncTearDown(self):
        await self.saga_execution_repository.destroy()
        await super().asyncTearDown()

    @abstractmethod
    def build_saga_execution_repository(self) -> SagaExecutionRepository:
        """For testing purposes."""

    async def test_store(self):
        await self.saga_execution_repository.store(self.execution)

        self.assertEqual(self.execution, await self.saga_execution_repository.load(self.execution.uuid))

    async def test_store_overwrite(self):
        await self.saga_execution_repository.store(self.execution)
        self.assertEqual(self.execution, await self.saga_execution_repository.load(self.execution.uuid))

        self.another.uuid = self.execution.uuid
        await self.saga_execution_repository.store(self.another)

        self.assertNotEqual(self.execution, await self.saga_execution_repository.load(self.execution.uuid))
        self.assertEqual(self.another, await self.saga_execution_repository.load(self.execution.uuid))

    async def test_load_from_str(self):
        await self.saga_execution_repository.store(self.execution)
        self.assertEqual(self.execution, await self.saga_execution_repository.load(str(self.execution.uuid)))

    async def test_load_raises(self):
        with self.assertRaises(SagaExecutionNotFoundException):
            await self.saga_execution_repository.load(self.execution.uuid)

    async def test_delete(self):
        await self.saga_execution_repository.store(self.execution)
        await self.saga_execution_repository.delete(self.execution)
        with self.assertRaises(SagaExecutionNotFoundException):
            await self.saga_execution_repository.load(self.execution.uuid)

    async def test_delete_from_str(self):
        await self.saga_execution_repository.store(self.execution)
        await self.saga_execution_repository.delete(str(self.execution.uuid))
        with self.assertRaises(SagaExecutionNotFoundException):
            await self.saga_execution_repository.load(self.execution.uuid)
