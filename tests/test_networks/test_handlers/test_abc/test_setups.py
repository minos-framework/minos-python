"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import unittest

import aiopg

from minos.common.testing import (
    PostgresAsyncTestCase,
)
from minos.networks import (
    HandlerSetup,
)
from tests.utils import (
    BASE_PATH,
)


class _FakeHandlerSetup(HandlerSetup):
    TABLE_NAME = "fake"


class TestHandlerSetup(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    async def test_if_queue_table_exists(self):
        async with _FakeHandlerSetup(**self.broker_queue_db):
            pass

        async with aiopg.connect(**self.broker_queue_db) as connect:
            async with connect.cursor() as cur:
                await cur.execute(
                    "SELECT 1 "
                    "FROM information_schema.tables "
                    "WHERE table_schema = 'public' AND table_name = 'fake';"
                )
                ret = []
                async for row in cur:
                    ret.append(row)

        assert ret == [(1,)]


if __name__ == "__main__":
    unittest.main()
