from abc import (
    ABC,
    abstractmethod,
)
from typing import (
    Any,
    Optional,
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
    def build_create(self) -> DatabaseOperation:
        """Build the database operation to create the delta table.

        :return: A ``DatabaseOperation`` instance.s
        """

    @abstractmethod
    def build_store(
        self,
        uuid: UUID,
        definition: dict[str, Any],
        status: str,
        executed_steps: list[dict[str, Any]],
        paused_step: Optional[dict[str, Any]],
        context: str,
        already_rollback: bool,
        user: Optional[UUID],
        **kwargs
    ) -> DatabaseOperation:
        """Build the database operation to store a saga execution.

        :param uuid: The identifier of the saga execution.
        :param definition: The ``Saga`` definition.
        :param context: The execution context.
        :param status: The status of the execution.
        :param executed_steps: The executed steps of the execution.
        :param paused_step: The paused step of the execution.
        :param already_rollback: ``True`` if already rollback of ``False`` otherwise.
        :param user: The user that launched the execution.
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
