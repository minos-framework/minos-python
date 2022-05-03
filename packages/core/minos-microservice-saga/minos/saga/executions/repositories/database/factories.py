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
    """Saga Execution Database Operation Factory class."""

    @abstractmethod
    def build_store(self, uuid: UUID, **kwargs) -> DatabaseOperation:
        """Build the database operation to store a saga execution.

        :param uuid: The identifier of the saga execution.
        :param kwargs: The attributes of the saga execution.
        :return: A ``DatabaseOperation`` instance.
        """
        raise NotImplementedError

    @abstractmethod
    def build_load(self, uuid: UUID) -> DatabaseOperation:
        """Build the database operation to load a saga execution.

        :param uuid: The identifier of the saga execution.
        :return: A ``DatabaseOperation`` instance.
        """
        raise NotImplementedError

    @abstractmethod
    def build_delete(self, uuid: UUID) -> DatabaseOperation:
        """Build the database operation to delete a saga execution.

        :param uuid: The identifier of the saga execution.
        :return: A ``DatabaseOperation`` instance.
        """
        raise NotImplementedError
