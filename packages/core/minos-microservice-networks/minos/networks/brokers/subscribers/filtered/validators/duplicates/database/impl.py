from __future__ import (
    annotations,
)

from uuid import (
    UUID,
)

from minos.common import (
    Builder,
    Config,
    DatabaseMixin,
    IntegrityException,
)

from ..abc import (
    BrokerSubscriberDuplicateValidator,
)
from .factories import (
    BrokerSubscriberDuplicateValidatorDatabaseOperationFactory,
)


class DatabaseBrokerSubscriberDuplicateValidator(
    BrokerSubscriberDuplicateValidator,
    DatabaseMixin[BrokerSubscriberDuplicateValidatorDatabaseOperationFactory],
):
    """Database Broker Subscriber Duplicate Detector class."""

    async def _setup(self) -> None:
        await super()._setup()
        await self._create_table()

    async def _create_table(self) -> None:
        operation = self.operation_factory.build_create()
        await self.submit_query(operation)

    async def _is_unique(self, topic: str, uuid: UUID) -> bool:
        operation = self.operation_factory.build_submit(topic, uuid)
        try:
            await self.submit_query(operation)
            return True
        except IntegrityException:
            return False


class DatabaseBrokerSubscriberDuplicateValidatorBuilder(Builder[DatabaseBrokerSubscriberDuplicateValidator]):
    """Database Broker Subscriber Duplicate Detector Builder class."""

    def with_config(self, config: Config):
        """Set config.

        :param config: The config to be set.
        :return: This method return the builder instance.
        """
        self.kwargs |= {"database_key": None}
        return super().with_config(config)


DatabaseBrokerSubscriberDuplicateValidator.set_builder(DatabaseBrokerSubscriberDuplicateValidatorBuilder)
