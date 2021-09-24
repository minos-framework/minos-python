from __future__ import (
    annotations,
)

from typing import (
    Type,
    TypeVar,
)

from ...configuration import (
    MinosConfig,
)
from ...database import (
    PostgreSqlMinosDatabase,
)


class PostgreSqlSnapshotSetup(PostgreSqlMinosDatabase):
    """Minos Snapshot Setup Class"""

    @classmethod
    def _from_config(cls: Type[T], config: MinosConfig, **kwargs) -> T:
        return cls(**config.snapshot._asdict(), **kwargs)

    async def _setup(self) -> None:
        await self.submit_query(_CREATE_TABLE_QUERY, lock=hash("snapshot"))
        await self.submit_query(_CREATE_OFFSET_TABLE_QUERY, lock=hash("snapshot_aux_offset"))


T = TypeVar("T", bound=PostgreSqlSnapshotSetup)

_CREATE_TABLE_QUERY = """
CREATE TABLE IF NOT EXISTS snapshot (
    aggregate_uuid UUID NOT NULL,
    aggregate_name TEXT NOT NULL,
    version INT NOT NULL,
    schema BYTEA,
    data JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (aggregate_uuid, aggregate_name)
);
""".strip()

_CREATE_OFFSET_TABLE_QUERY = """
CREATE TABLE IF NOT EXISTS snapshot_aux_offset (
    id bool PRIMARY KEY DEFAULT TRUE,
    value BIGINT NOT NULL,
    CONSTRAINT id_uni CHECK (id)
);
""".strip()
