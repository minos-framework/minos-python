from ...clients import (
    AiopgDatabaseClient,
)
from ...operations import (
    AiopgDatabaseOperation,
    DatabaseOperation,
)
from .abc import (
    LockDatabaseOperationFactory,
)


class AiopgLockDatabaseOperationFactory(LockDatabaseOperationFactory):
    """TODO"""

    def build_acquire(self, hashed_key: int) -> DatabaseOperation:
        """TODO"""
        return AiopgDatabaseOperation("select pg_advisory_lock(%(hashed_key)s)", {"hashed_key": hashed_key})

    def build_release(self, hashed_key: int) -> DatabaseOperation:
        """TODO"""
        return AiopgDatabaseOperation("select pg_advisory_unlock(%(hashed_key)s)", {"hashed_key": hashed_key})


AiopgDatabaseClient.register_factory(LockDatabaseOperationFactory, AiopgLockDatabaseOperationFactory)
