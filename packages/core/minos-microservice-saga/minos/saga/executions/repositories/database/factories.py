from abc import (
    ABC,
    abstractmethod,
)
from uuid import (
    UUID,
)

from minos.common import (
    DatabaseOperation,
    DatabaseOperationFactory,
)


class SagaExecutionDatabaseOperationFactory(DatabaseOperationFactory, ABC):
    """TODO"""

    @abstractmethod
    def build_store(self, uuid: UUID, **kwargs) -> DatabaseOperation:
        """TODO"""
        raise NotImplementedError

    @abstractmethod
    def build_load(self, uuid: UUID) -> DatabaseOperation:
        """TODO"""
        raise NotImplementedError

    @abstractmethod
    def build_delete(self, uuid: UUID) -> DatabaseOperation:
        """TODO"""
        raise NotImplementedError
