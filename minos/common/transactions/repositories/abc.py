from abc import (
    ABC,
    abstractmethod,
)
from typing import (
    AsyncIterator,
    Optional,
)
from uuid import (
    UUID,
)

from ...setup import (
    MinosSetup,
)
from ..models import (
    Transaction,
    TransactionStatus,
)


class TransactionRepository(ABC, MinosSetup):
    """TODO"""

    async def submit(self, transaction: Transaction) -> None:
        """TODO"""
        await self._submit(transaction)

    @abstractmethod
    async def _submit(self, transaction: Transaction) -> None:
        raise NotImplementedError

    async def select(
        self,
        uuid: Optional[UUID] = None,
        uuid_in: Optional[tuple[UUID, ...]] = None,
        status: Optional[TransactionStatus] = None,
        status_in: Optional[tuple[str, ...]] = None,
        event_offset: Optional[int] = None,
        event_offset_lt: Optional[int] = None,
        event_offset_gt: Optional[int] = None,
        event_offset_le: Optional[int] = None,
        event_offset_ge: Optional[int] = None,
        **kwargs
    ) -> AsyncIterator[Transaction]:
        """TODO"""
        generator = self._select(
            uuid=uuid,
            uuid_in=uuid_in,
            status=status,
            status_in=status_in,
            event_offset=event_offset,
            event_offset_lt=event_offset_lt,
            event_offset_gt=event_offset_gt,
            event_offset_le=event_offset_le,
            event_offset_ge=event_offset_ge,
            **kwargs,
        )
        # noinspection PyTypeChecker
        async for entry in generator:
            yield entry

    @abstractmethod
    async def _select(self, **kwargs) -> AsyncIterator[Transaction]:
        raise NotImplementedError
