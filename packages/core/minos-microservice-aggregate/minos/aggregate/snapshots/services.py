from __future__ import (
    annotations,
)

import logging
from asyncio import (
    gather,
)
from typing import (
    TYPE_CHECKING,
)
from uuid import (
    UUID,
)

from cached_property import (
    cached_property,
)

from minos.common import (
    Config,
    Inject,
    ModelType,
)
from minos.networks import (
    EnrouteDecorator,
    Request,
    Response,
    ResponseException,
    enroute,
)

from .repositories import (
    SnapshotRepository,
)

if TYPE_CHECKING:
    from ..entities import (
        Entity,
    )

logger = logging.getLogger(__name__)


class SnapshotService:
    """Snapshot Service class."""

    # noinspection PyUnusedLocal
    @Inject()
    def __init__(
        self,
        *args,
        config: Config,
        snapshot_repository: SnapshotRepository,
        **kwargs,
    ):
        self.config = config
        self.snapshot_repository = snapshot_repository

    @classmethod
    def __get_enroute__(cls, config: Config) -> dict[str, set[EnrouteDecorator]]:
        from ..entities import (
            RefResolver,
        )

        aggregate_config = config.get_aggregate()
        root_entity = aggregate_config["entities"][0]
        name = root_entity.__name__

        return {
            cls.__get_many__.__name__: {enroute.broker.command(RefResolver.build_topic_name(name))},
            cls.__synchronize__.__name__: {enroute.periodic.event("* * * * *")},
        }

    async def __get_many__(self, request: Request) -> Response:
        """Get many ``Entity`` instances.

        :param request: The ``Request`` instance that contains the instance identifiers.
        :return: A ``Response`` instance containing the requested instances.
        """
        try:
            content = await request.content(model_type=ModelType.build("Query", {"uuids": list[UUID]}))
        except Exception as exc:
            raise ResponseException(f"There was a problem while parsing the given request: {exc!r}")

        try:
            futures = (self.snapshot_repository.get(self.type_, uuid) for uuid in content["uuids"])
            instances = await gather(*futures)
        except Exception as exc:
            raise ResponseException(f"There was a problem while getting the instances: {exc!r}")

        return Response(instances)

    @cached_property
    def type_(self) -> type[Entity]:
        """Load the concrete ``Entity`` class.

        :return: A ``Type`` object.
        """
        aggregate_config = self.config.get_aggregate()
        return aggregate_config["entities"][0]

    # noinspection PyUnusedLocal
    async def __synchronize__(self, request: Request) -> None:
        """Performs a Snapshot synchronization every minute.

        :param request: A request containing information related with scheduling.
        :return: This method does not return anything.
        """
        logger.info("Performing periodic Snapshot synchronization...")
        await self.snapshot_repository.synchronize()
