import time

import pytest
from minos.common.configuration.config import MinosConfig
from minos.common.logs import log
from minos.networks.broker import (Aggregate, BrokerDatabaseInitializer,
                                   Dispatcher, MinosBrokerDatabase,
                                   MinosEventBroker, MinosCommandBroker)


@pytest.fixture()
def config():
    return MinosConfig(path="./tests/test_config_.yaml")


@pytest.fixture
def services(config):
    return [BrokerDatabaseInitializer(config=config)]


@pytest.fixture()
async def database(config):
    return await MinosBrokerDatabase().get_connection(config)


async def test_if_queue_table_exists(database):
    time.sleep(1)
    cur = await database.cursor()

    await cur.execute("SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'queue';")
    ret = []
    async for row in cur:
        ret.append(row)

    database.close()
    assert ret == [(1,)]


class AgregateTest(Aggregate):
    test: int


async def test_events_broker_insertion(config, database):
    a = AgregateTest(test_id=1, test=2)

    m = MinosEventBroker("EventBroker", config)
    await m.send(a)

    cur = await database.cursor()

    await cur.execute("SELECT 1 FROM queue WHERE topic = 'EventBroker' LIMIT 1;")
    ret = []
    async for row in cur:
        ret.append(row)

    database.close()
    assert ret == [(1,)]


async def test_commands_broker_insertion(config, database):
    a = AgregateTest(test_id=1, test=2)

    m = MinosCommandBroker("CommandBroker", config)
    await m.send(model=a, callback="test")

    cur = await database.cursor()

    await cur.execute("SELECT 1 FROM queue WHERE topic = 'CommandBroker' LIMIT 1;")
    ret = []
    async for row in cur:
        ret.append(row)

    database.close()
    assert ret == [(1,)]


"""
async def test_queue_dispatcher(config):
    d = Dispatcher(config)
    await d.run()



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
