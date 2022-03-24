from minos.common import (
    Config,
    Inject,
)
from minos.networks import (
    BrokerRequest,
    EnrouteDecorator,
    enroute,
)
from minos.saga import (
    SagaManager,
    SagaResponse,
)


class SagaService:
    """Saga Service class"""

    # noinspection PyUnusedLocal
    @Inject()
    def __init__(self, *args, saga_manager: SagaManager, **kwargs):
        self.saga_manager = saga_manager

    @classmethod
    def __get_enroute__(cls, config: Config) -> dict[str, set[EnrouteDecorator]]:
        return {cls.__reply__.__name__: {enroute.broker.command(f"{config.get_name()}Reply")}}

    async def __reply__(self, request: BrokerRequest) -> None:
        response = SagaResponse.from_message(request.raw)
        await self.saga_manager.run(response=response, pause_on_disk=True, raise_on_error=False, return_execution=False)
