from uuid import (
    UUID,
)

from minos.common import (
    NULL_UUID,
    Inject,
)
from minos.transactions import (
    TRANSACTION_CONTEXT_VAR,
    TransactionalMixin,
)

from ...messages import (
    BrokerMessage,
)
from ..abc import (
    BrokerPublisher,
)
from . import (
    BrokerPublisherTransactionEntry,
)
from .repositories import (
    BrokerPublisherTransactionRepository,
)


class TransactionalBrokerPublisher(BrokerPublisher, TransactionalMixin):
    """TODO"""

    impl: BrokerPublisher
    repository: BrokerPublisherTransactionRepository

    @Inject()
    def __init__(self, impl: BrokerPublisher, repository: BrokerPublisherTransactionRepository, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.impl = impl
        self.repository = repository

    async def _send(self, message: BrokerMessage) -> None:
        transaction = TRANSACTION_CONTEXT_VAR.get()

        if transaction is None:
            await self.impl.send(message)
        else:
            entry = BrokerPublisherTransactionEntry(message, transaction.uuid)
            await self.repository.submit(entry)

    async def commit_transaction(self, transaction_uuid: UUID, destination_transaction_uuid: UUID) -> None:
        """Commit the transaction with given identifier.

        :param transaction_uuid: The identifier of the transaction to be committed.
        :param destination_transaction_uuid: The identifier of the destination transaction.
        :return: This method does not return anything.
        """
        iterable = self.repository.select(transaction_uuid=transaction_uuid)
        if destination_transaction_uuid == NULL_UUID:
            async for entry in iterable:
                await self.impl.send(entry.message)
        else:
            async for entry in iterable:
                new_entry = BrokerPublisherTransactionEntry(entry.message, destination_transaction_uuid)
                await self.repository.submit(new_entry)

        await self.repository.delete_batch(transaction_uuid=transaction_uuid)

    async def reject_transaction(self, transaction_uuid: UUID) -> None:
        """Reject the transaction with given identifier

        :param transaction_uuid: The identifier of the transaction to be committed.
        :return: This method does not return anything.
        """
        await self.repository.delete_batch(transaction_uuid=transaction_uuid)
