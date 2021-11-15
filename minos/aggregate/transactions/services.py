import logging

from dependency_injector.wiring import (
    Provide,
    inject,
)

from minos.common import (
    MinosConfig,
)
from minos.networks import (
    EnrouteDecorator,
    Request,
    ResponseException,
    enroute,
)

from ..exceptions import (
    EventRepositoryConflictException,
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
            cls.__reserve_transaction__.__name__: {enroute.broker.command(f"Reserve{service_name.title()}Transaction")},
            cls.__reject_transaction__.__name__: {enroute.broker.command(f"Reject{service_name.title()}Transaction")},
            cls.__commit_transaction__.__name__: {enroute.broker.command(f"Commit{service_name.title()}Transaction")},
        }

    async def __reserve_transaction__(self, request: Request) -> None:
        uuid = await request.content()
        transaction = await self.transaction_repository.get(uuid)
        try:
            await transaction.reserve()
        except EventRepositoryConflictException:
            raise ResponseException("The transaction could not be reserved.")

    async def __reject_transaction__(self, request: Request) -> None:
        uuid = await request.content()
        transaction = await self.transaction_repository.get(uuid)
        await transaction.reject()

    async def __commit_transaction__(self, request: Request) -> None:
        uuid = await request.content()
        transaction = await self.transaction_repository.get(uuid)
        await transaction.commit()
