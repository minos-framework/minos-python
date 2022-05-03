from abc import (
    ABC,
    abstractmethod,
)

from ..operations import (
    DatabaseOperation,
    DatabaseOperationFactory,
)


class LockDatabaseOperationFactory(DatabaseOperationFactory, ABC):
    """Lock Database Operation Factory class."""

    @abstractmethod
    def build_acquire(self, hashed_key: int) -> DatabaseOperation:
        """Build the database operation to acquire the lock.

        :param hashed_key: The hashed key that identifies the lock.
        :return: A ``DatabaseOperation`` instance.
        """

    @abstractmethod
    def build_release(self, hashed_key: int) -> DatabaseOperation:
        """Build the database operation to release the lock.

        :param hashed_key: The hashed key that identifies the lock.
        :return: A ``DatabaseOperation`` instance.
        """
