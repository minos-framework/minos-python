"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""

import unittest
from typing import (
    NoReturn,
)
from unittest.mock import (
    MagicMock,
    call,
)

import aiopg
from psycopg2.sql import (
    SQL,
)

from minos.common import (
    Model,
)
from minos.common.testing import (
    PostgresAsyncTestCase,
)
from minos.networks import (
    Broker,
    BrokerSetup,
)
from tests.utils import (
    BASE_PATH,
)


class _FakeBroker(Broker):
    ACTION = "fake"

    async def send(self, items: list[Model], **kwargs) -> NoReturn:
        """For testing purposes"""


class TestBrokerSetup(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def setUp(self) -> None:
        super().setUp()
        self.broker_setup = BrokerSetup(**self.config.saga.queue._asdict())

    async def test_setup(self):
        async with self.broker_setup:
            pass

        async with aiopg.connect(**self.events_queue_db) as connection:
            async with connection.cursor() as cursor:
                await cursor.execute(
                    "SELECT 1 "
                    "FROM information_schema.tables "
                    "WHERE table_schema = 'public' AND table_name = 'producer_queue';"
                )
                ret = []
                async for row in cursor:
                    ret.append(row)

        assert ret == [(1,)]


class TestBroker(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def setUp(self) -> None:
        super().setUp()
        self.broker = _FakeBroker(**self.saga_queue_db)

    async def test_send_bytes(self):
        query = SQL(
            "INSERT INTO producer_queue (topic, model, retry, action, creation_date, update_date) "
            "VALUES (%s, %s, %s, %s, NOW(), NOW()) "
            "RETURNING id"
        )

        async def _fn(*args, **kwargs):
            return (56,)

        mock = MagicMock(side_effect=_fn)

        async with self.broker:
            self.broker.submit_query_and_fetchone = mock

            identifier = await self.broker.send_bytes("test_topic", b"test")

        self.assertEqual(56, identifier)
        self.assertEqual(1, mock.call_count)

        self.assertEqual(call(query, ("test_topic", b"test", 0, "fake")), mock.call_args)


if __name__ == "__main__":
    unittest.main()
