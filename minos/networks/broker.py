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
import datetime


database_proxy = DatabaseProxy()


class MinosBrokerDatabase:
    def __init__(self, conf):
        self.database = PostgresqlDatabase(
            conf.queue.database,
            user=conf.queue.user,
            password=conf.queue.password,
            host=conf.queue.host,
            port=conf.queue.port,
        )

    def get_connection(self):
        database_proxy.initialize(self.database)
        return self.database


class BrokerQueueBaseModel(Model):
    class Meta:
        database = database_proxy


class Queue(BrokerQueueBaseModel):
    queue_id = AutoField()
    topic = CharField()
    model = BlobField()
    retry = IntegerField(default=0)
    creation_date = DateTimeField(default=datetime.datetime.now)
    update_date = DateTimeField(default=datetime.datetime.now)


def create_event_tables(config: MinosConfig):
    database = MinosBrokerDatabase(config.events).get_connection()

    with database:
        database.create_tables([Queue])


def create_command_tables(config: MinosConfig):
    database = MinosBrokerDatabase(config.commands).get_connection()

    with database:
        database.create_tables([Queue])


def drop_event_tables(config: MinosConfig):
    database = MinosBrokerDatabase(config.events).get_connection()
    with database:
        database.drop_tables([Queue])


def drop_commands_tables(config: MinosConfig):
    database = MinosBrokerDatabase(config.commands).get_connection()
    with database:
        database.drop_tables([Queue])


class AggregateModel:
    name: str
    pass


class ModelBase:
    pass


class EventModel(ModelBase):
    topic: str
    model: str
    items: AggregateModel


class CommandModel(ModelBase):
    topic: str
    model: str
    items: AggregateModel


class BrokerBase(abc.ABC):
    @abc.abstractmethod
    def _database(self):
        raise NotImplementedError

    @abc.abstractmethod
    def send(self):
        raise NotImplementedError


class MinosEventBroker(BrokerBase):
    def __init__(self, topic: str, model: AggregateModel, config: MinosConfig):
        self.config = config
        self.topic = topic
        self.model = model
        self._database()

    def _database(self):
        self.database = MinosBrokerDatabase(self.config.events).get_connection()

    def send(self):
        self.database.connect()

        # TODO: Change
        event_instance = EventModel()
        event_instance.topic = self.topic
        event_instance.name = self.model.name
        event_instance.items = self.model

        query = Queue.insert(topic=self.topic, model=hex(10))
        result = query.execute()
        log.debug(result)

        self.database.close()

        return result


class MinosCommandBroker(BrokerBase):
    def __init__(self, topic: str, model: AggregateModel, config: MinosConfig):
        self.config = config
        self.topic = topic
        self.model = model
        self._database()

    def _database(self):
        self.database = MinosBrokerDatabase(self.config.events).get_connection()

    def send(self):
        self.database.connect()

        # TODO: Change
        event_instance = CommandModel()
        event_instance.topic = self.topic
        event_instance.name = self.model.name
        event_instance.items = self.model

        query = Queue.insert(topic=self.topic, model=hex(10))
        result = query.execute()
        log.debug(result)

        self.database.close()

        return result
