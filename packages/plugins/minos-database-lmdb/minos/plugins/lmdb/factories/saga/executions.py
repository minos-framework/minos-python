from uuid import (
    UUID,
)

from minos.common import (
    DatabaseOperation,
)
from minos.saga import (
    SagaExecutionDatabaseOperationFactory,
)

from ...clients import (
    LmdbDatabaseClient,
)
from ...operations import (
    LmdbDatabaseOperation,
    LmdbDatabaseOperationType,
)


class LmdbSagaExecutionDatabaseOperationFactory(SagaExecutionDatabaseOperationFactory):
    """Lmdb Saga Execution Database Operation Factory class."""

    # noinspection PyMethodMayBeStatic
    def build_table_name(self) -> str:
        """Get the table name.

        :return: A ``str`` value.
        """
        return "LocalState"

    def build_store(self, uuid: UUID, **kwargs) -> DatabaseOperation:
        """Build the database operation to store a saga execution.

        :param uuid: The identifier of the saga execution.
        :param kwargs: The attributes of the saga execution.
        :return: A ``DatabaseOperation`` instance.
        """
        key = str(uuid)
        value = kwargs | {"uuid": str(uuid)}

        return LmdbDatabaseOperation(LmdbDatabaseOperationType.CREATE, self.build_table_name(), key, value)

    def build_load(self, uuid: UUID) -> DatabaseOperation:
        """Build the database operation to load a saga execution.

        :param uuid: The identifier of the saga execution.
        :return: A ``DatabaseOperation`` instance.
        """
        key = str(uuid)
        return LmdbDatabaseOperation(LmdbDatabaseOperationType.READ, self.build_table_name(), key)

    def build_delete(self, uuid: UUID) -> DatabaseOperation:
        """Build the database operation to delete a saga execution.

        :param uuid: The identifier of the saga execution.
        :return: A ``DatabaseOperation`` instance.
        """
        key = str(uuid)
        return LmdbDatabaseOperation(LmdbDatabaseOperationType.DELETE, self.build_table_name(), key)


LmdbDatabaseClient.set_factory(SagaExecutionDatabaseOperationFactory, LmdbSagaExecutionDatabaseOperationFactory)
