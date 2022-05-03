from abc import (
    ABC,
    abstractmethod,
)
from typing import (
    Union,
)
from uuid import (
    UUID,
)

from minos.common import (
    SetupMixin,
)

from ..saga import (
    SagaExecution,
)


class SagaExecutionRepository(SetupMixin, ABC):
    """Saga Execution Repository class."""

    async def store(self, execution: SagaExecution) -> None:
        """Store an execution.

        :param execution: Execution to be stored.
        :return: This method does not return anything.
        """
        return await self._store(execution)

    async def _store(self, execution: SagaExecution) -> None:
        raise NotImplementedError

    async def load(self, uuid: Union[str, UUID]) -> SagaExecution:
        """Load the saga execution stored on the given key.

        :param uuid: The key to identify the execution.
        :return: A ``SagaExecution`` instance.
        """
        if not isinstance(uuid, UUID):
            uuid = UUID(uuid)
        return await self._load(uuid)

    @abstractmethod
    async def _load(self, uuid: UUID) -> SagaExecution:
        raise NotImplementedError

    async def delete(self, uuid: Union[SagaExecution, str, UUID]) -> None:
        """Delete the reference of the given key.

        :param uuid: Execution key to be deleted.
        :return: This method does not return anything.
        """
        if isinstance(uuid, SagaExecution):
            uuid = uuid.uuid
        if not isinstance(uuid, UUID):
            uuid = UUID(uuid)
        return await self._delete(uuid)

    async def _delete(self, key: UUID) -> None:
        raise NotImplementedError
