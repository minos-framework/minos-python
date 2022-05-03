import unittest
from shutil import (
    rmtree,
)

from minos.saga import (
    DatabaseSagaExecutionRepository,
    SagaExecutionRepository,
)
from minos.saga.testing import (
    SagaExecutionRepositoryTestCase,
)
from tests.utils import (
    LmdbTestCase,
)


class TestDatabaseSagaExecutionRepository(LmdbTestCase, SagaExecutionRepositoryTestCase):
    __test__ = True

    def build_saga_execution_repository(self) -> SagaExecutionRepository:
        return DatabaseSagaExecutionRepository.from_config(self.config)

    def tearDown(self) -> None:
        path = self.config.get_database_by_name("saga")["path"]
        rmtree(path, ignore_errors=True)
        super().tearDown()


if __name__ == "__main__":
    unittest.main()
