from minos.common import (
    DatabaseOperation,
    LockDatabaseOperationFactory,
)

from ...clients import (
    AiopgDatabaseClient,
)
from ...operations import (
    AiopgDatabaseOperation,
)


class AiopgLockDatabaseOperationFactory(LockDatabaseOperationFactory):
    """Aiopg Lock Database Operation Factory class."""

    def build_acquire(self, hashed_key: int) -> DatabaseOperation:
        """Build the database operation to acquire the lock.

        :param hashed_key: The hashed key that identifies the lock.
        :return: A ``DatabaseOperation`` instance.
        """
        return AiopgDatabaseOperation("select pg_advisory_lock(%(hashed_key)s)", {"hashed_key": hashed_key})

    def build_release(self, hashed_key: int) -> DatabaseOperation:
        """Build the database operation to release the lock.

        :param hashed_key: The hashed key that identifies the lock.
        :return: A ``DatabaseOperation`` instance.
        """
        return AiopgDatabaseOperation("select pg_advisory_unlock(%(hashed_key)s)", {"hashed_key": hashed_key})


AiopgDatabaseClient.set_factory(LockDatabaseOperationFactory, AiopgLockDatabaseOperationFactory)
