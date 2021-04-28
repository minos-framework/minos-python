import asyncio
import time

import pytest
from minos.common import Aggregate
from minos.common.configuration.config import MinosConfig
from minos.common.logs import log
from minos.networks.broker import MinosCommandBroker, MinosEventBroker, broker_queue_dispatcher, send_to_kafka
from tests.broker.database_testcase import PostgresAsyncTestCase


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

                await cur.execute(
                    "SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'producer_queue';"
                )
                ret = []
                async for row in cur:
                    ret.append(row)

        assert ret == [(1,)]

    async def test_send_to_kafka_ok(self):
        response = await send_to_kafka(topic="TestKafkaSend", message=bytes(), config=self._broker_config())
        assert response is True

    async def test_events_broker_insertion(self):
        a = AggregateTest(test_id=1, test=2, id=1, version=1)

        m = MinosEventBroker("EventBroker", self._broker_config())

        affected_rows, queue_id = await m.send(a)

        assert affected_rows == 1
        assert queue_id > 0

    async def test_if_events_was_deleted(self):
        a = AggregateTest(test_id=1, test=2, id=1, version=1)
        m = MinosEventBroker("EventBroker-Delete", self._broker_config())
        affected_rows_1, queue_id_1 = await m.send(a)
        affected_rows_2, queue_id_2 = await m.send(a)

        await broker_queue_dispatcher(self._broker_config())

        database = await self._database()
        async with database as connect:
            async with connect.cursor() as cur:
                await cur.execute("SELECT COUNT(*) FROM producer_queue WHERE topic = '%s'" % "EventBroker-Delete")
                records = await cur.fetchone()

        assert affected_rows_1 == 1
        assert queue_id_1 > 0
        assert affected_rows_2 == 1
        assert queue_id_2 > 0
        assert records[0] == 0

    async def test_commands_broker_insertion(self):
        a = AggregateTest(test_id=1, test=2, id=1, version=1)

        m = MinosCommandBroker("CommandBroker", self._broker_config())

        affected_rows, queue_id = await m.send(model=a, reply_on="test_reply_on")
        assert affected_rows == 1
        assert queue_id > 0

    async def test_if_commands_was_deleted(self):
        a = AggregateTest(test_id=1, test=2, id=1, version=1)
        m = MinosCommandBroker("CommandBroker-Delete", self._broker_config())
        affected_rows_1, queue_id_1 = await m.send(a, reply_on="test_reply_on")
        affected_rows_2, queue_id_2 = await m.send(a, reply_on="test_reply_on")

        await broker_queue_dispatcher(self._broker_config())

        database = await self._database()
        async with database as connect:
            async with connect.cursor() as cur:
                await cur.execute("SELECT COUNT(*) FROM producer_queue WHERE topic = '%s'" % "CommandBroker-Delete")
                records = await cur.fetchone()

        assert affected_rows_1 == 1
        assert queue_id_1 > 0
        assert affected_rows_2 == 1
        assert queue_id_2 > 0
        assert records[0] == 0

    async def test_if_commands_retry_was_incremented(self):
        a = AggregateTest(test_id=1, test=2, id=1, version=1)
        m = MinosCommandBroker("CommandBroker-Delete", self._broker_config())
        affected_rows_1, queue_id_1 = await m.send(a, reply_on="test_reply_on")
        affected_rows_2, queue_id_2 = await m.send(a, reply_on="test_reply_on")

        config = MinosConfig(path="./tests/wrong_test_config.yaml")

        await broker_queue_dispatcher(config)

        database = await self._database()
        async with database as connect:
            async with connect.cursor() as cur:
                await cur.execute("SELECT COUNT(*) FROM producer_queue WHERE topic = '%s'" % "CommandBroker-Delete")
                records = await cur.fetchone()

                await cur.execute("SELECT retry FROM producer_queue WHERE id=%d;" % queue_id_1)
                retry_1 = await cur.fetchone()

                await cur.execute("SELECT retry FROM producer_queue WHERE id=%d;" % queue_id_2)
                retry_2 = await cur.fetchone()

        assert affected_rows_1 == 1
        assert queue_id_1 > 0
        assert affected_rows_2 == 1
        assert queue_id_2 > 0
        assert records[0] == 2
        assert retry_1[0] > 0
        assert retry_2[0] > 0

    async def test_if_events_retry_was_incremented(self):
        a = AggregateTest(test_id=1, test=2, id=1, version=1)
        m = MinosEventBroker("EventBroker-Delete", self._broker_config())
        affected_rows_1, queue_id_1 = await m.send(a)
        affected_rows_2, queue_id_2 = await m.send(a)

        config = MinosConfig(path="./tests/wrong_test_config.yaml")

        await broker_queue_dispatcher(config)

        database = await self._database()
        async with database as connect:
            async with connect.cursor() as cur:
                await cur.execute("SELECT COUNT(*) FROM producer_queue WHERE topic = '%s'" % "EventBroker-Delete")
                records = await cur.fetchone()

                await cur.execute("SELECT retry FROM producer_queue WHERE id=%d;" % queue_id_1)
                retry_1 = await cur.fetchone()

                await cur.execute("SELECT retry FROM producer_queue WHERE id=%d;" % queue_id_2)
                retry_2 = await cur.fetchone()

        assert affected_rows_1 == 1
        assert queue_id_1 > 0
        assert affected_rows_2 == 1
        assert queue_id_2 > 0
        assert records[0] == 2
        assert retry_1[0] > 0
        assert retry_2[0] > 0


"""
create role broker with createdb login password 'br0k3r';
CREATE DATABASE broker_db OWNER broker;
'CREATE TABLE IF NOT EXISTS "queue" ("queue_id" SERIAL NOT NULL PRIMARY KEY, "topic" VARCHAR(255) NOT NULL, "model" BYTEA NOT NULL, "retry" INTEGER NOT NULL, "creation_date" TIMESTAMP NOT NULL, "update_date" TIMESTAMP NOT NULL)', []
'SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = %s ORDER BY tablename', ('public',)
'DROP TABLE IF EXISTS "queue"'
"""
