from __future__ import (
    annotations,
)

from typing import (
    Optional,
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
    AiopgBrokerSubscriberDuplicateValidatorDatabaseOperationFactory,
    BrokerSubscriberDuplicateValidatorDatabaseOperationFactory,
)


class DatabaseBrokerSubscriberDuplicateValidator(BrokerSubscriberDuplicateValidator, DatabaseMixin):
    """PostgreSql Broker Subscriber Duplicate Detector class."""

    def __init__(
        self,
        operation_factory: Optional[BrokerSubscriberDuplicateValidatorDatabaseOperationFactory] = None,
        *args,
        **kwargs,
    ):
        if operation_factory is None:
            operation_factory = AiopgBrokerSubscriberDuplicateValidatorDatabaseOperationFactory()
        super().__init__(*args, **kwargs)
        self._operation_factory = operation_factory

    async def _setup(self) -> None:
        await super()._setup()
        await self._create_table()

    async def _create_table(self) -> None:
        operation = self._operation_factory.build_create_table()
        await self.submit_query(operation)

    @property
    def operation_factory(self) -> AiopgBrokerSubscriberDuplicateValidatorDatabaseOperationFactory:
        """Get the query factory.

        :return: A ``PostgreSqlBrokerSubscriberDuplicateValidatorQueryFactory`` instance.
        """
        return self._operation_factory

    async def _is_unique(self, topic: str, uuid: UUID) -> bool:
        operation = self._operation_factory.build_insert_row(topic, uuid)
        try:
            await self.submit_query(operation)
            return True
        except IntegrityException:
            return False


class DatabaseBrokerSubscriberDuplicateValidatorBuilder(Builder[DatabaseBrokerSubscriberDuplicateValidator]):
    """PostgreSql Broker Subscriber Duplicate Detector Builder class."""

    def with_config(self, config: Config):
        """Set config.

        :param config: The config to be set.
        :return: This method return the builder instance.
        """
        self.kwargs |= {"database_key": None}
        return super().with_config(config)


DatabaseBrokerSubscriberDuplicateValidator.set_builder(DatabaseBrokerSubscriberDuplicateValidatorBuilder)
