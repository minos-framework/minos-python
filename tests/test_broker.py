import asyncio
import time

import pytest
from minos.common.configuration.config import MinosConfig
from minos.common.logs import log
from minos.networks.broker import (Aggregate, BrokerDatabaseInitializer,
                                   EventBrokerQueueDispatcher,
                                   MinosBrokerDatabase, MinosCommandBroker,
                                   MinosEventBroker)


@pytest.fixture
def config():
    return MinosConfig(path="./tests/test_config.yaml")


@pytest.fixture
def services(config):
    return [BrokerDatabaseInitializer(config=config), EventBrokerQueueDispatcher(interval=0.5, delay=0, config=config)]


@pytest.fixture
async def database(config):
    return await MinosBrokerDatabase().get_connection(config)


@pytest.mark.asyncio
async def test_database_connection(database):
    assert database.closed == 0
    database.close()


@pytest.mark.asyncio
async def test_if_queue_table_exists(database):
    time.sleep(1)
    cur = await database.cursor()

    await cur.execute("SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'queue';")
    ret = []
    async for row in cur:
        ret.append(row)

    database.close()
    assert ret == [(1,)]


class AggregateTest(Aggregate):
    test: int


@pytest.mark.asyncio
async def test_events_broker_insertion(config):
    a = AggregateTest(test_id=1, test=2)

    m = MinosEventBroker("EventBroker", config)

    affected_rows, queue_id = await m.send(a)

    assert affected_rows == 1
    assert queue_id > 0
    """
    cur = await database.cursor()

    await cur.execute("SELECT 1 FROM queue WHERE topic = 'EventBroker' LIMIT 1;")
    ret = []
    async for row in cur:
        ret.append(row)

    database.close()
    assert ret == [(1,)]
    """


@pytest.mark.asyncio
async def test_commands_broker_insertion(config):
    a = AggregateTest(test_id=1, test=2)

    m = MinosCommandBroker("CommandBroker", config)

    affected_rows, queue_id = await m.send(model=a, callback="test")
    assert affected_rows == 1
    assert queue_id > 0


"""
async def test_drop_database(database):
    cur = await database.cursor()
    await cur.execute("DROP TABLE IF EXISTS queue;")
    database.close()
"""

# create role broker with createdb login password 'br0k3r';
# CREATE DATABASE broker_db OWNER broker;
# 'CREATE TABLE IF NOT EXISTS "queue" ("queue_id" SERIAL NOT NULL PRIMARY KEY, "topic" VARCHAR(255) NOT NULL, "model" BYTEA NOT NULL, "retry" INTEGER NOT NULL, "creation_date" TIMESTAMP NOT NULL, "update_date" TIMESTAMP NOT NULL)', []
# 'SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = %s ORDER BY tablename', ('public',)
# 'DROP TABLE IF EXISTS "queue"'
