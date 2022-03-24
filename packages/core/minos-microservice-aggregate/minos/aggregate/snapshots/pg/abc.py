from __future__ import (
    annotations,
)

from typing import (
    Type,
    TypeVar,
)

from minos.common import (
    Config,
    PostgreSqlMinosDatabase,
)


class PostgreSqlSnapshotSetup(PostgreSqlMinosDatabase):
    """Minos Snapshot Setup Class"""

    @classmethod
    def _from_config(cls: Type[T], config: Config, **kwargs) -> T:
        return cls(**config.get_database_by_name("snapshot"), **kwargs)

    async def _setup(self) -> None:
        await self.submit_query('CREATE EXTENSION IF NOT EXISTS "uuid-ossp";', lock="uuid-ossp")
        await self.submit_query(_CREATE_TABLE_QUERY, lock="snapshot")
        await self.submit_query(_CREATE_OFFSET_TABLE_QUERY, lock="snapshot_aux_offset")


T = TypeVar("T", bound=PostgreSqlSnapshotSetup)

_CREATE_TABLE_QUERY = """
CREATE TABLE IF NOT EXISTS snapshot (
    uuid UUID NOT NULL,
    name TEXT NOT NULL,
    version INT NOT NULL,
    schema BYTEA,
    data JSONB,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    transaction_uuid UUID NOT NULL DEFAULT uuid_nil(),
    PRIMARY KEY (uuid, transaction_uuid)
);
""".strip()

_CREATE_OFFSET_TABLE_QUERY = """
CREATE TABLE IF NOT EXISTS snapshot_aux_offset (
    id bool PRIMARY KEY DEFAULT TRUE,
    value BIGINT NOT NULL,
    CONSTRAINT id_uni CHECK (id)
);
""".strip()
