import logging
from uuid import (
    UUID,
)

from minos.aggregate import (
    EventRepositoryConflictException,
    TransactionEntry,
)

logger = logging.getLogger(__name__)


class TransactionCommitter:
    """Transaction Committer class."""

    def __init__(self, execution_uuid: UUID, *args, **kwargs):
        self.execution_uuid = execution_uuid

    # noinspection PyUnusedCommit,PyMethodOverriding
    async def commit(self, **kwargs) -> None:
        """Commit the transaction.

        :param kwargs: Additional named arguments.
        :return: This method does not return anything.
        """
        logger.info("committing...")

        transaction = TransactionEntry(self.execution_uuid)

        if await self._reserve(transaction):
            await self._commit(transaction)
        else:
            await self.reject()
            raise ValueError("Some transactions could not be committed.")

    @staticmethod
    async def _reserve(transaction: TransactionEntry) -> bool:
        try:
            await transaction.reserve()
            return True
        except EventRepositoryConflictException:
            return False

    @staticmethod
    async def _commit(transaction: TransactionEntry) -> None:
        await transaction.commit()

    async def reject(self) -> None:
        """Reject the transaction.

        :return:
        """
        transaction = TransactionEntry(self.execution_uuid)
        await transaction.reject()
