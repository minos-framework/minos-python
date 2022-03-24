from __future__ import (
    annotations,
)

from typing import (
    Optional,
)
from uuid import (
    UUID,
)

from psycopg2 import (
    IntegrityError,
)
from psycopg2.sql import (
    SQL,
)

from minos.common import (
    Config,
    PostgreSqlMinosDatabase,
)

from .abc import (
    BrokerSubscriberDuplicateDetector,
)


class PostgreSqlBrokerSubscriberDuplicateDetector(BrokerSubscriberDuplicateDetector, PostgreSqlMinosDatabase):
    """PostgreSql Broker Subscriber Duplicate Detector class."""

    def __init__(
        self, query_factory: Optional[PostgreSqlBrokerSubscriberDuplicateDetectorQueryFactory] = None, *args, **kwargs
    ):
        if query_factory is None:
            query_factory = PostgreSqlBrokerSubscriberDuplicateDetectorQueryFactory()
        super().__init__(*args, **kwargs)
        self._query_factory = query_factory

    @classmethod
    def _from_config(cls, config: Config, **kwargs) -> PostgreSqlBrokerSubscriberDuplicateDetector:
        return cls(**config.get_database_by_name("broker"), **kwargs)

    async def _setup(self) -> None:
        await super()._setup()
        await self._create_table()

    async def _create_table(self) -> None:
        await self.submit_query(
            self._query_factory.build_activate_uuid_extension(),
            lock=self._query_factory.build_uuid_extension_name(),
        )
        await self.submit_query(
            self._query_factory.build_create_table(),
            lock=self._query_factory.build_table_name(),
        )

    @property
    def query_factory(self) -> PostgreSqlBrokerSubscriberDuplicateDetectorQueryFactory:
        """Get the query factory.

        :return: A ``PostgreSqlBrokerSubscriberDuplicateDetectorQueryFactory`` instance.
        """
        return self._query_factory

    async def _is_valid(self, topic: str, uuid: UUID) -> bool:
        try:
            await self.submit_query(self._query_factory.build_insert_row(), {"topic": topic, "uuid": uuid})
            return True
        except IntegrityError:
            return False


class PostgreSqlBrokerSubscriberDuplicateDetectorQueryFactory:
    """PostgreSql Broker Subscriber Duplicate Detector Query Factory class."""

    @staticmethod
    def build_uuid_extension_name() -> str:
        """Build the uuid extension name.

        :return: A ``str`` instance.
        """
        return "uuid-ossp"

    def build_activate_uuid_extension(self) -> SQL:
        """Build activate uuid extension query.

        :return: A ``SQL`` instance.
        """
        return SQL(f'CREATE EXTENSION IF NOT EXISTS "{self.build_uuid_extension_name()}";')

    @staticmethod
    def build_table_name() -> str:
        """Build the table name.

        :return: A ``str`` instance.
        """
        return "broker_subscriber_processed_messages"

    def build_create_table(self) -> SQL:
        """Build the "create table" query.

        :return: A ``SQL`` instance.
        """
        return SQL(
            f"CREATE TABLE IF NOT EXISTS {self.build_table_name()} ("
            "   topic VARCHAR(255) NOT NULL, "
            "   uuid UUID NOT NULL, "
            "   created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),"
            "   PRIMARY KEY (topic, uuid)"
            ")"
        )

    def build_insert_row(self) -> SQL:
        """Build the "insert row" query.

        :return: A ``SQL`` instance.
        """
        return SQL(f"INSERT INTO {self.build_table_name()}(topic, uuid) VALUES(%(topic)s, %(uuid)s)")
