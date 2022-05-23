import unittest

from minos.saga import (
    DatabaseSagaExecutionRepository,
    SagaExecutionRepository,
)
from minos.saga.testing import (
    SagaExecutionRepositoryTestCase,
)
from tests.utils import (
    AiopgTestCase,
)


class TestDatabaseSagaExecutionRepository(AiopgTestCase, SagaExecutionRepositoryTestCase):
    __test__ = True

    def build_saga_execution_repository(self) -> SagaExecutionRepository:
        """For testing purposes."""
        return DatabaseSagaExecutionRepository.from_config(self.config)


if __name__ == "__main__":
    unittest.main()
