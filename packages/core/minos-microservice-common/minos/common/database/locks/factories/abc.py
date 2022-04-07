from abc import (
    ABC,
    abstractmethod,
)

from ...operations import (
    DatabaseOperation,DatabaseOperationFactory
)


class LockDatabaseOperationFactory(DatabaseOperationFactory, ABC):
    """TODO"""

    @abstractmethod
    def build_acquire(self, hashed_key: int) -> DatabaseOperation:
        """TODO"""

    @abstractmethod
    def build_release(self, hashed_key: int) -> DatabaseOperation:
        """TODO"""
