from __future__ import (
    annotations,
)

import logging
from asyncio import (
    gather,
)
from typing import (
    TYPE_CHECKING,
    Type,
)
from uuid import (
    UUID,
)

from cached_property import (
    cached_property,
)
from dependency_injector.wiring import (
    Provide,
    inject,
)

from minos.common import (
    MinosConfig,
    ModelType,
    import_module,
)
from minos.networks import (
    EnrouteDecorator,
    Request,
    Response,
    ResponseException,
    enroute,
)

from .abc import (
    SnapshotRepository,
)

if TYPE_CHECKING:
    from ..models import (
        Aggregate,
    )

logger = logging.getLogger(__name__)


class SnapshotService:
    """Snapshot Service class."""

    # noinspection PyUnusedLocal
    @inject
    def __init__(
        self,
        *args,
        config: MinosConfig = Provide["config"],
        snapshot_repository: SnapshotRepository = Provide["snapshot_repository"],
        **kwargs,
    ):
        self.config = config
        self.snapshot_repository = snapshot_repository

    @classmethod
    def __get_enroute__(cls, config: MinosConfig) -> dict[str, set[EnrouteDecorator]]:
        aggregate_name = config.service.aggregate.rsplit(".", 1)[-1]
        return {
            cls.__get_one__.__name__: {enroute.broker.command(f"Get{aggregate_name}")},
            cls.__get_many__.__name__: {enroute.broker.command(f"Get{aggregate_name}s")},
            cls.__synchronize__.__name__: {enroute.periodic.event("* * * * *")},
        }

    async def __get_one__(self, request: Request) -> Response:
        """Get aggregate.

        :param request: The ``Request`` instance that contains the aggregate identifier.
        :return: A ``Response`` instance containing the requested aggregate.
        """
        try:
            content = await request.content(model_type=ModelType.build("Query", {"uuid": UUID}))
        except Exception as exc:
            raise ResponseException(f"There was a problem while parsing the given request: {exc!r}")

        try:
            aggregate = await self.__aggregate_cls__.get(content["uuid"])
        except Exception as exc:
            raise ResponseException(f"There was a problem while getting the aggregate: {exc!r}")

        return Response(aggregate)

    async def __get_many__(self, request: Request) -> Response:
        """Get aggregates.

        :param request: The ``Request`` instance that contains the product identifiers.
        :return: A ``Response`` instance containing the requested aggregates.
        """
        try:
            content = await request.content(model_type=ModelType.build("Query", {"uuids": list[UUID]}))
        except Exception as exc:
            raise ResponseException(f"There was a problem while parsing the given request: {exc!r}")

        try:
            aggregates = await gather(*(self.__aggregate_cls__.get(uuid) for uuid in content["uuids"]))
        except Exception as exc:
            raise ResponseException(f"There was a problem while getting aggregates: {exc!r}")

        return Response(aggregates)

    @cached_property
    def __aggregate_cls__(self) -> Type[Aggregate]:
        # noinspection PyTypeChecker
        return import_module(self.config.service.aggregate)

    # noinspection PyUnusedLocal
    async def __synchronize__(self, request: Request) -> None:
        """Performs a Snapshot synchronization every minute.

        :param request: A request containing information related with scheduling.
        :return: This method does not return anything.
        """
        logger.info("Performing periodic Snapshot synchronization...")
        await self.snapshot_repository.synchronize()
