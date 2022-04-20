from typing import (
    Optional,
)
from uuid import (
    UUID,
)

from minos.common import (
    Inject,
    Lock,
    NotProvidedException,
    SetupMixin,
)

from .repositories import (
    TransactionRepository,
)


class TransactionalMixin(SetupMixin):
    """Transactional Mixin class."""

    @Inject()
    def __init__(self, transaction_repository: TransactionRepository, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if transaction_repository is None:
            raise NotProvidedException("A transaction repository instance is required.")
        self._transaction_repository = transaction_repository

    @property
    def transaction_repository(self) -> TransactionRepository:
        """Get the transaction repository.

        :return: A ``Transaction`` repository instance.
        """
        return self._transaction_repository

    async def _setup(self) -> None:
        await super()._setup()
        self._register_into_transaction_repository()

    async def _destroy(self) -> None:
        self._unregister_from_transaction_repository()
        await super()._destroy()

    def _register_into_transaction_repository(self) -> None:
        self._transaction_repository.register_observer(self)

    def _unregister_from_transaction_repository(self) -> None:
        self._transaction_repository.unregister_observer(self)

    async def get_collided_transactions(self, transaction_uuid: UUID) -> set[UUID]:
        """Get the set of collided transaction identifiers.

        :param transaction_uuid: The identifier of the transaction to be committed.
        :return: A ``set`` or ``UUID`` values.
        """
        return set()

    async def commit_transaction(self, transaction_uuid: UUID, destination_transaction_uuid: UUID) -> None:
        """Commit the transaction with given identifier.

        :param transaction_uuid: The identifier of the transaction to be committed.
        :param destination_transaction_uuid: The identifier of the destination transaction.
        :return: This method does not return anything.
        """

    async def reject_transaction(self, transaction_uuid: UUID) -> None:
        """Reject the transaction with given identifier

        :param transaction_uuid: The identifier of the transaction to be committed.
        :return: This method does not return anything.
        """

    def write_lock(self) -> Optional[Lock]:
        """Get a lock if available.

        :return: A ``Lock`` instance or ``None``.
        """
