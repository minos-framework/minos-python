from ...database import (
    PostgreSqlMinosDatabase,
)


class PostgreSqlSnapshotSetup(PostgreSqlMinosDatabase):
    """Minos Snapshot Setup Class"""

    async def _setup(self) -> None:
        await self.submit_query(_CREATE_TABLE_QUERY, lock=hash("snapshot"))
        await self.submit_query(_CREATE_OFFSET_TABLE_QUERY, lock=hash("snapshot_aux_offset"))


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
