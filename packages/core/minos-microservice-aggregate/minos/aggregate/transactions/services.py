import logging
from datetime import (
    timedelta,
)

from dependency_injector.wiring import (
    Provide,
    inject,
)

from minos.common import (
    MinosConfig,
    current_datetime,
)
from minos.networks import (
    EnrouteDecorator,
    Request,
    ResponseException,
    enroute,
)

from ..exceptions import (
    EventRepositoryConflictException,
    TransactionNotFoundException,
    TransactionRepositoryConflictException,
)
from .entries import (
    TransactionStatus,
)
from .repositories import (
    TransactionRepository,
)

logger = logging.getLogger(__name__)


class TransactionService:
    """Snapshot Service class."""

    # noinspection PyUnusedLocal
    @inject
    def __init__(
        self, *args, transaction_repository: TransactionRepository = Provide["transaction_repository"], **kwargs
    ):
        self.transaction_repository = transaction_repository

    @classmethod
    def __get_enroute__(cls, config: MinosConfig) -> dict[str, set[EnrouteDecorator]]:
        service_name = config.service.name
        return {
            cls.__reserve__.__name__: {enroute.broker.command(f"_Reserve{service_name.title()}Transaction")},
            cls.__reject__.__name__: {enroute.broker.command(f"_Reject{service_name.title()}Transaction")},
            cls.__commit__.__name__: {enroute.broker.command(f"_Commit{service_name.title()}Transaction")},
            cls.__reject_blocked__.__name__: {enroute.periodic.event("* * * * *")},
        }

    async def __reserve__(self, request: Request) -> None:
        uuid = await request.content()

        try:
            transaction = await self.transaction_repository.get(uuid)
        except TransactionNotFoundException:
            raise ResponseException(f"The transaction identified by {uuid!r} does not exist.")

        try:
            await transaction.reserve()
        except EventRepositoryConflictException:
            raise ResponseException("The transaction could not be reserved.")

    async def __reject__(self, request: Request) -> None:
        uuid = await request.content()

        try:
            transaction = await self.transaction_repository.get(uuid)
        except TransactionNotFoundException:
            raise ResponseException(f"The transaction identified by {uuid!r} does not exist.")

        if transaction.status == TransactionStatus.REJECTED:
            return

        try:
            await transaction.reject()
        except TransactionRepositoryConflictException as exc:
            raise ResponseException(f"{exc!s}")

    async def __commit__(self, request: Request) -> None:
        uuid = await request.content()

        try:
            transaction = await self.transaction_repository.get(uuid)
        except TransactionNotFoundException:
            raise ResponseException(f"The transaction identified by {uuid!r} does not exist.")

        try:
            await transaction.commit()
        except TransactionRepositoryConflictException as exc:
            raise ResponseException(f"{exc!s}")

    # noinspection PyUnusedLocal
    async def __reject_blocked__(self, request: Request) -> None:
        status_in = (TransactionStatus.RESERVED,)
        updated_at_lt = current_datetime() - timedelta(minutes=1)
        async for transaction in self.transaction_repository.select(status_in=status_in, updated_at_lt=updated_at_lt):
            logger.info(f"Rejecting {transaction.uuid!r} transaction as it has been reserved for a long time...")
            try:
                await transaction.reject()
            except TransactionRepositoryConflictException as exc:
                logger.warning(f"Raised an exception while trying to reject a blocked transaction: {exc!r}")
