from dependency_injector.wiring import (
    Provide,
    inject,
)

from minos.aggregate import (
    EventRepositoryConflictException,
    TransactionRepository,
)
from minos.common import (
    MinosConfig,
)
from minos.networks import (
    CommandReply,
    EnrouteDecorator,
    HandlerRequest,
    ResponseException,
    enroute,
)
from minos.saga import (
    SagaManager,
    SagaResponse,
)


class SagaService:
    """Base Service class"""

    @inject
    def __init__(
        self,
        *args,
        config: MinosConfig = Provide["config"],
        transaction_repository: TransactionRepository = Provide["transaction_repository"],
        saga_manager: SagaManager = Provide["saga_manager"],
        **kwargs,
    ):
        self.config = config
        self.transaction_repository = transaction_repository
        self.saga_manager = saga_manager

    @classmethod
    def __get_enroute__(cls, config: MinosConfig) -> dict[str, set[EnrouteDecorator]]:
        service_name = config.service.name
        return {
            cls.__reserve_transaction__.__name__: {enroute.broker.command(f"Reserve{service_name.title()}Transaction")},
            cls.__reject_transaction__.__name__: {enroute.broker.command(f"Reject{service_name.title()}Transaction")},
            cls.__commit_transaction__.__name__: {enroute.broker.command(f"Commit{service_name.title()}Transaction")},
            cls.__saga_reply__.__name__: {enroute.broker.command(f"{service_name}Reply")},
        }

    async def __reserve_transaction__(self, request: HandlerRequest) -> None:
        uuid = await request.content()
        transaction = await self.transaction_repository.get(uuid)
        try:
            await transaction.reserve()
        except EventRepositoryConflictException:
            raise ResponseException("The transaction could not be reserved.")

    async def __reject_transaction__(self, request: HandlerRequest) -> None:
        uuid = await request.content()
        transaction = await self.transaction_repository.get(uuid)
        await transaction.reject()

    async def __commit_transaction__(self, request: HandlerRequest) -> None:
        uuid = await request.content()
        transaction = await self.transaction_repository.get(uuid)
        await transaction.commit()

    async def __saga_reply__(self, request: HandlerRequest) -> None:
        raw: CommandReply = request.raw
        response = SagaResponse(raw.data, raw.status, raw.service_name, raw.saga)
        await self.saga_manager.run(response=response, pause_on_disk=True, raise_on_error=False, return_execution=False)
