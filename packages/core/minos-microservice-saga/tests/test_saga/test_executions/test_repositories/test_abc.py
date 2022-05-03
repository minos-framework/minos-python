import unittest
from unittest.mock import (
    AsyncMock,
    call,
)
from uuid import (
    UUID,
)

from minos.saga import (
    SagaExecution,
    SagaExecutionRepository,
)
from tests.utils import (
    ADD_ORDER,
    SagaTestCase,
)


class _SagaExecutionRepository(SagaExecutionRepository):
    async def _store(self, execution: SagaExecution) -> None:
        """For testing purposes."""

    async def _load(self, uuid: UUID) -> SagaExecution:
        """For testing purposes."""

    async def _delete(self, key: UUID) -> None:
        """For testing purposes."""


class TestSagaExecutionRepository(SagaTestCase):
    async def test_store(self):
        mock = AsyncMock()
        repository = _SagaExecutionRepository()
        repository._store = mock
        execution = SagaExecution.from_definition(ADD_ORDER)

        await repository.store(execution)

        self.assertEqual([call(execution)], mock.call_args_list)

    async def test_load(self):
        execution = SagaExecution.from_definition(ADD_ORDER)
        repository = _SagaExecutionRepository()
        mock = AsyncMock(return_value=execution)
        repository._load = mock

        observed = await repository.load(execution.uuid)

        self.assertEqual(execution, observed)
        self.assertEqual([call(execution.uuid)], mock.call_args_list)

    async def test_load_from_str(self):
        execution = SagaExecution.from_definition(ADD_ORDER)
        repository = _SagaExecutionRepository()
        mock = AsyncMock(return_value=execution)
        repository._load = mock

        observed = await repository.load(str(execution.uuid))

        self.assertEqual(execution, observed)
        self.assertEqual([call(execution.uuid)], mock.call_args_list)

    async def test_delete(self):
        execution = SagaExecution.from_definition(ADD_ORDER)
        repository = _SagaExecutionRepository()
        mock = AsyncMock(return_value=execution)
        repository._delete = mock

        await repository.delete(execution)

        self.assertEqual([call(execution.uuid)], mock.call_args_list)

    async def test_delete_from_uuid(self):
        execution = SagaExecution.from_definition(ADD_ORDER)
        repository = _SagaExecutionRepository()
        mock = AsyncMock(return_value=execution)
        repository._delete = mock

        await repository.delete(execution.uuid)

        self.assertEqual([call(execution.uuid)], mock.call_args_list)

    async def test_delete_from_str(self):
        execution = SagaExecution.from_definition(ADD_ORDER)
        repository = _SagaExecutionRepository()
        mock = AsyncMock(return_value=execution)
        repository._delete = mock

        await repository.delete(str(execution.uuid))

        self.assertEqual([call(execution.uuid)], mock.call_args_list)


if __name__ == "__main__":
    unittest.main()
