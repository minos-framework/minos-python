import unittest
from abc import (
    ABC,
)

from minos.common import (
    DatabaseOperationFactory,
)
from minos.saga import (
    SagaExecutionDatabaseOperationFactory,
)


class TestSagaExecutionDatabaseOperationFactory(unittest.TestCase):
    def test_abstract(self):
        self.assertTrue(issubclass(SagaExecutionDatabaseOperationFactory, (DatabaseOperationFactory, ABC)))
        # noinspection PyUnresolvedReferences
        self.assertEqual(
            {"build_store", "build_load", "build_delete"}, SagaExecutionDatabaseOperationFactory.__abstractmethods__
        )


if __name__ == '__main__':
    unittest.main()
