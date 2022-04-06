from abc import (
    ABC,
    abstractmethod,
)

from ...operations import (
    DatabaseOperation,
)


class LockDatabaseOperationFactory(ABC):
    """TODO"""

    @abstractmethod
    def build_acquire(self, hashed_key: int) -> DatabaseOperation:
        """TODO"""

    @abstractmethod
    def build_release(self, hashed_key: int) -> DatabaseOperation:
        """TODO"""
