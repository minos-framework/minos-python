import pytest

from minos.common.logs import log
from minos.networks.broker import create_event_tables, create_command_tables,\
    drop_event_tables, drop_commands_tables, Queue, MinosEventBroker, MinosCommandBroker, AggregateModel
from minos.common.configuration.config import MinosConfig
from peewee import *

# create role broker with createdb login password 'br0k3r';
# CREATE DATABASE broker_db OWNER broker;

@pytest.fixture()
def config():
    return MinosConfig(path='./tests/test_config.yaml')

@pytest.fixture()
def events_database(config):
    return PostgresqlDatabase(
        config.events.queue.database,
        user=config.events.queue.user,
        password=config.events.queue.password,
        host=config.events.queue.host,
        port=config.events.queue.port)


@pytest.fixture()
def commands_database(config):
    return PostgresqlDatabase(
        config.events.queue.database,
        user=config.events.queue.user,
        password=config.events.queue.password,
        host=config.events.queue.host,
        port=config.events.queue.port)


def test_broker_events_tables_creation(config, events_database):
    create_event_tables(config)
    assert events_database.table_exists(table_name="queue") is True


def test_broker_events_database_connection(events_database):
    assert events_database.connect() is True


def test_broker_event_tables_deletion(config, events_database):
    drop_event_tables(config)
    assert events_database.table_exists(table_name="queue") is False


def test_broker_commands_tables_creation(config, commands_database):
    create_command_tables(config)
    assert commands_database.table_exists(table_name="queue") is True


def test_broker_commands_database_connection(commands_database):
    assert commands_database.connect() is True


def test_events_broker_insertion(config, events_database):
    a = AggregateModel()
    a.name = "EventBroker"

    result = MinosEventBroker("EventBroker", a, config).send()

    query = Queue.select().where(Queue.topic == "EventBroker")

    assert result is not None
    assert query.get() is not None


def test_commands_broker_insertion(config, commands_database):
    a = AggregateModel()
    a.name = "CommandBroker"

    result = MinosCommandBroker("CommandBroker", a, config).send()

    query = Queue.select().where(Queue.topic == "CommandBroker")

    assert result is not None
    assert query.get() is not None


# Olways leave on end
def test_broker_commands_tables_deletion(config, commands_database):
    drop_commands_tables(config)
    assert commands_database.table_exists(table_name="queue") is False
