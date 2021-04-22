import asyncio
import time

import pytest

from minos.common.logs import log
from minos.networks.broker import (Aggregate, MinosCommandBroker, MinosEventBroker)
from tests.broker.database_testcase import (
    PostgresAsyncTestCase,
)


class AggregateTest(Aggregate):
    test: int


class TestPostgreSqlMinosBroker(PostgresAsyncTestCase):
    async def test_database_connection(self):
        database = await self._database()
        async with database as connect:
            assert database.closed == 0

    async def test_if_queue_table_exists(self):
        database = await self._database()
        async with database as connect:
            async with connect.cursor() as cur:

                await cur.execute("SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'queue';")
                ret = []
                async for row in cur:
                    ret.append(row)

        assert ret == [(1,)]

    async def test_events_broker_insertion(self):
        a = AggregateTest(test_id=1, test=2)

        m = MinosEventBroker("EventBroker", self._broker_config())

        affected_rows, queue_id = await m.send(a)

        assert affected_rows == 1
        assert queue_id > 0

    async def test_commands_broker_insertion(self):
        a = AggregateTest(test_id=1, test=2)

        m = MinosCommandBroker("CommandBroker", self._broker_config())

        affected_rows, queue_id = await m.send(model=a, callback="test")
        assert affected_rows == 1
        assert queue_id > 0



# create role broker with createdb login password 'br0k3r';
# CREATE DATABASE broker_db OWNER broker;
# 'CREATE TABLE IF NOT EXISTS "queue" ("queue_id" SERIAL NOT NULL PRIMARY KEY, "topic" VARCHAR(255) NOT NULL, "model" BYTEA NOT NULL, "retry" INTEGER NOT NULL, "creation_date" TIMESTAMP NOT NULL, "update_date" TIMESTAMP NOT NULL)', []
# 'SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = %s ORDER BY tablename', ('public',)
# 'DROP TABLE IF EXISTS "queue"'
