from abc import (
    abstractmethod,
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
    """TODO"""

    @Inject()
    def __init__(self, transaction_repository: TransactionRepository, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if transaction_repository is None:
            raise NotProvidedException("A transaction repository instance is required.")
        self._transaction_repository = transaction_repository

    async def _setup(self) -> None:
        await super()._setup()
        self.subscribe_to_transactions()

    async def _destroy(self) -> None:
        self.unsubscribe_from_transactions()
        await super()._destroy()

    def subscribe_to_transactions(self) -> None:
        """TODO"""
        self._transaction_repository.add_subscriber(self)

    def unsubscribe_from_transactions(self) -> None:
        """TODO"""
        self._transaction_repository.remove_subscriber(self)

    @abstractmethod
    async def get_related_transactions(self, transaction_uuid: UUID) -> set[UUID]:
        """TODO"""

    @abstractmethod
    async def commit_transaction(self, transaction_uuid: UUID, destination: UUID) -> None:
        """TODO"""

    async def reject_transaction(self, transaction_uuid: UUID) -> None:
        """TODO"""

    @abstractmethod
    def write_lock(self) -> Lock:
        """TODO"""
