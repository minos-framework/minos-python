# Copyright (C) 2020 Clariteia SL
#
# This file is part of minos framework.
#
# Minos framework can not be copied and/or distributed without the express
# permission of Clariteia SL.

from minos.common.logs import log
from minos.common.configuration.config import MinosConfig
import typing as t
from peewee import *
import abc


database_proxy = DatabaseProxy()


class BrokerQueueBaseModel(Model):
    class Meta:
        database = database_proxy


class Queue(BrokerQueueBaseModel):
    queue_id = AutoField()
    topic = CharField()
    model = BinaryUUIDField()
    retry = IntegerField()
    creation_date = DateTimeField()
    update_date = DateTimeField()


def create_event_tables(config: MinosConfig):
    database = PostgresqlDatabase(
        config.events.queue.database,
        user=config.events.queue.user,
        password=config.events.queue.password,
        host=config.events.queue.host,
        port=config.events.queue.port)

    database_proxy.initialize(database)
    with database:
        database.create_tables([Queue])


def create_command_tables(config: MinosConfig):
    database = PostgresqlDatabase(
        config.commands.queue.database,
        user=config.commands.queue.user,
        password=config.commands.queue.password,
        host=config.commands.queue.host,
        port=config.commands.queue.port)

    database_proxy.initialize(database)
    with database:
        database.create_tables([Queue])


def drop_event_tables(config: MinosConfig):
    database = PostgresqlDatabase(
        config.events.queue.database,
        user=config.events.queue.user,
        password=config.events.queue.password,
        host=config.events.queue.host,
        port=config.events.queue.port)

    database_proxy.initialize(database)
    with database:
        database.drop_tables([Queue])


def drop_commands_tables(config: MinosConfig):
    database = PostgresqlDatabase(
        config.commands.queue.database,
        user=config.commands.queue.user,
        password=config.commands.queue.password,
        host=config.commands.queue.host,
        port=config.commands.queue.port)

    database_proxy.initialize(database)
    with database:
        database.drop_tables([Queue])
