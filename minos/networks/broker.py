# Copyright (C) 2020 Clariteia SL
#
# This file is part of minos framework.
#
# Minos framework can not be copied and/or distributed without the express
# permission of Clariteia SL.
import abc
import asyncio
import datetime
import typing as t

import aiomisc
import aiopg
from aiomisc.service.periodic import PeriodicService
from aiomisc.service.periodic import Service

from minos.common.configuration.config import MinosConfig
from minos.common.logs import log


class MinosBrokerDatabase:
    async def get_connection(self, conf):
        conn = await aiopg.connect(
            database=conf.events.queue.database,
            user=conf.events.queue.user,
            password=conf.events.queue.password,
            host=conf.events.queue.host,
            port=conf.events.queue.port,
        )
        return conn


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
    def _database(self):  # pragma: no cover
        raise NotImplementedError

    @abc.abstractmethod
    def send(self):  # pragma: no cover
        raise NotImplementedError


class MinosEventBroker(BrokerBase):
    def __init__(self, topic: str, model: AggregateModel, config: MinosConfig):
        self.config = config
        self.topic = topic
        self.model = model
        self._database()

    def _database(self):
        pass

    async def send(self):

        # TODO: Change
        event_instance = EventModel()
        event_instance.topic = self.topic
        event_instance.name = self.model.name
        event_instance.items = self.model

        bin_data = b"bytes object"

        conn = await MinosBrokerDatabase().get_connection(self.config)

        cur = await conn.cursor()
        await cur.execute(
            "INSERT INTO queue (topic, model, retry, creation_date, update_date) VALUES (%s, %s, %s, %s, %s) RETURNING queue_id;",
            (
                event_instance.topic,
                bin_data,
                0,
                datetime.datetime.now(),
                datetime.datetime.now(),
            ),
        )

        conn.close()


class MinosCommandBroker(BrokerBase):
    def __init__(self, topic: str, model: AggregateModel, config: MinosConfig):
        self.config = config
        self.topic = topic
        self.model = model
        self._database()

    def _database(self):
        pass

    async def send(self):

        # TODO: Change
        event_instance = CommandModel()
        event_instance.topic = self.topic
        event_instance.name = self.model.name
        event_instance.items = self.model

        bin_data = b"bytes object"

        conn = await MinosBrokerDatabase().get_connection(self.config)

        cur = await conn.cursor()
        await cur.execute(
            "INSERT INTO queue (topic, model, retry, creation_date, update_date) VALUES (%s, %s, %s, %s, %s) RETURNING queue_id;",
            (
                event_instance.topic,
                bin_data,
                0,
                datetime.datetime.now(),
                datetime.datetime.now(),
            ),
        )

        conn.close()


class BrokerDatabaseInitializer(Service):
    async def start(self):
        # Send signal to entrypoint for continue running
        self.start_event.set()

        conn = await MinosBrokerDatabase().get_connection(self.config)

        cur = await conn.cursor()
        await cur.execute(
            'CREATE TABLE IF NOT EXISTS "queue" ("queue_id" SERIAL NOT NULL PRIMARY KEY, '
            '"topic" VARCHAR(255) NOT NULL, "model" BYTEA NOT NULL, "retry" INTEGER NOT NULL, '
            '"creation_date" TIMESTAMP NOT NULL, "update_date" TIMESTAMP NOT NULL);'
        )

        conn.close()

        return
