from abc import (
    ABC,
)

from minos.common import (
    DatabaseOperationFactory,
)


class SagaExecutionDatabaseOperationFactory(DatabaseOperationFactory, ABC):
    """TODO"""
