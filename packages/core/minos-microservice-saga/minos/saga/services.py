import logging
from datetime import (
    timedelta,
)

from minos.common import (
    Config,
    Inject,
    current_datetime,
)
from minos.networks import (
    BrokerRequest,
    EnrouteDecorator,
    Request,
    ResponseException,
    enroute,
)
from minos.saga import (
    SagaManager,
    SagaResponse,
)
from minos.transactions import (
    TransactionNotFoundException,
    TransactionRepository,
    TransactionRepositoryConflictException,
    TransactionStatus,
)

logger = logging.getLogger(__name__)


class SagaService:
    """Saga Service class"""

    # noinspection PyUnusedLocal
    @Inject()
    def __init__(self, *args, saga_manager: SagaManager, transaction_repository: TransactionRepository, **kwargs):
        self.saga_manager = saga_manager
        self.transaction_repository = transaction_repository

    @classmethod
    def __get_enroute__(cls, config: Config) -> dict[str, set[EnrouteDecorator]]:
        name = config.get_name()
        return {
            cls.__reply__.__name__: {enroute.broker.command(f"{config.get_name()}Reply")},
            cls.__reserve__.__name__: {enroute.broker.command(f"_Reserve{name.title()}Transaction")},
            cls.__reject__.__name__: {enroute.broker.command(f"_Reject{name.title()}Transaction")},
            cls.__commit__.__name__: {enroute.broker.command(f"_Commit{name.title()}Transaction")},
            cls.__reject_blocked__.__name__: {enroute.periodic.event("* * * * *")},
        }

    async def __reply__(self, request: BrokerRequest) -> None:
        response = SagaResponse.from_message(request.raw)
        await self.saga_manager.run(response=response, pause_on_disk=True, raise_on_error=False, return_execution=False)

    async def __reserve__(self, request: Request) -> None:
        uuid = await request.content()

        try:
            transaction = await self.transaction_repository.get(uuid)
        except TransactionNotFoundException:
            raise ResponseException(f"The transaction identified by {uuid!r} does not exist.")

        try:
            await transaction.reserve()
        except TransactionRepositoryConflictException:
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
