import unittest
from unittest.mock import (
    patch,
)

from minos.common import (
    DatabaseClient,
    ProgrammingException,
)
from minos.saga import (
    DatabaseSagaExecutionRepository,
    SagaExecutionRepository,
)
from minos.saga.testing import (
    SagaExecutionRepositoryTestCase,
)
from tests.utils import (
    SagaTestCase,
)


class TestDatabaseSagaExecutionRepository(SagaTestCase, SagaExecutionRepositoryTestCase):
    __test__ = True

    def build_saga_execution_repository(self) -> SagaExecutionRepository:
        return DatabaseSagaExecutionRepository.from_config(self.config)

    async def test_store(self):
        with patch.object(DatabaseClient, "fetch_one", side_effect=[self.execution.raw]):
            await super().test_store()

    async def test_store_overwrite(self):
        another_raw = self.another.raw
        another_raw["uuid"] = self.execution.uuid
        with patch.object(DatabaseClient, "fetch_one", side_effect=[self.execution.raw, another_raw, another_raw]):
            await super().test_store_overwrite()

    async def test_load_from_str(self):
        with patch.object(DatabaseClient, "fetch_one", side_effect=[self.execution.raw]):
            await super().test_load_from_str()

    async def test_load_raises(self):
        with patch.object(DatabaseClient, "fetch_one", side_effect=[ProgrammingException("")]):
            await super().test_load_raises()

    async def test_delete(self):
        with patch.object(DatabaseClient, "fetch_one", side_effect=[ProgrammingException("")]):
            await super().test_delete()

    async def test_delete_from_str(self):
        with patch.object(DatabaseClient, "fetch_one", side_effect=[ProgrammingException("")]):
            await super().test_delete_from_str()


if __name__ == "__main__":
    unittest.main()
