import logging
from datetime import (
    timedelta,
)

from minos.common import (
    UUID_REGEX,
    Config,
    Inject,
    current_datetime,
)
from minos.networks import (
    BrokerRequest,
    EnrouteDecorator,
    Request,
    Response,
    ResponseException,
    enroute,
)
from minos.saga import (
    SagaManager,
    SagaResponse,
    SagaStatus,
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
            cls._handle_reply.__name__: {
                enroute.broker.command(f"{config.get_name()}Reply"),
            },
            cls._handle_get.__name__: {
                enroute.broker.command(f"_Get{name.title()}SagaExecution"),
                enroute.rest.command(f"/{name}s/saga/executions/{{uuid:{UUID_REGEX.pattern}}}", "GET"),
            },
            cls._handle_reserve.__name__: {
                enroute.broker.command(f"_Reserve{name.title()}Transaction"),
            },
            cls._handle_reject.__name__: {
                enroute.broker.command(f"_Reject{name.title()}Transaction"),
            },
            cls._handle_commit.__name__: {
                enroute.broker.command(f"_Commit{name.title()}Transaction"),
            },
            cls._handle_reject_blocked.__name__: {
                enroute.periodic.event("* * * * *"),
            },
        }

    async def _handle_reply(self, request: BrokerRequest) -> None:
        response = SagaResponse.from_message(request.raw)
        await self.saga_manager.run(response=response, pause_on_disk=True, raise_on_error=False, return_execution=False)

    async def _handle_get(self, request: Request) -> Response:
        if request.has_params:
            uuid = (await request.params())["uuid"]
        else:
            uuid = (await request.content())["uuid"]

        execution = await self.saga_manager.get(uuid)

        content = {"uuid": execution.uuid, "status": execution.status}
        if execution.status == SagaStatus.Finished:
            content["context"] = execution.context

        return Response(content)

    async def _handle_reserve(self, request: Request) -> None:
        uuid = await request.content()

        try:
            transaction = await self.transaction_repository.get(uuid)
        except TransactionNotFoundException:
            raise ResponseException(f"The transaction identified by {uuid!r} does not exist.")

        try:
            await transaction.reserve()
        except TransactionRepositoryConflictException:
            raise ResponseException("The transaction could not be reserved.")

    async def _handle_reject(self, request: Request) -> None:
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

    async def _handle_commit(self, request: Request) -> None:
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
    async def _handle_reject_blocked(self, request: Request) -> None:
        status_in = (TransactionStatus.RESERVED,)
        updated_at_lt = current_datetime() - timedelta(minutes=1)
        async for transaction in self.transaction_repository.select(status_in=status_in, updated_at_lt=updated_at_lt):
            logger.info(f"Rejecting {transaction.uuid!r} transaction as it has been reserved for a long time...")
            try:
                await transaction.reject()
            except TransactionRepositoryConflictException as exc:
                logger.warning(f"Raised an exception while trying to reject a blocked transaction: {exc!r}")
