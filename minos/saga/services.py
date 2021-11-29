from uuid import (
    UUID,
)

from dependency_injector.wiring import (
    Provide,
    inject,
)

from minos.common import (
    MinosConfig,
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
    @inject
    def __init__(self, *args, saga_manager: SagaManager = Provide["saga_manager"], **kwargs):
        self.saga_manager = saga_manager

    @classmethod
    def __get_enroute__(cls, config: MinosConfig) -> dict[str, set[EnrouteDecorator]]:
        return {cls.__reply__.__name__: {enroute.broker.command(f"{config.service.name}Reply")}}

    async def __reply__(self, request: BrokerRequest) -> None:
        message = request.raw

        uuid = UUID(message.headers["saga"])
        service_name = str(message.service_name)
        if "service_names" in message.headers:
            service_name += f",{message.headers['service_names']}"

        response = SagaResponse(message.data, message.status, service_name, uuid)
        await self.saga_manager.run(response=response, pause_on_disk=True, raise_on_error=False, return_execution=False)
