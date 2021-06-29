"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from typing import (
    NoReturn,
)

from minos.common import (
    MinosModel,
)
from minos.common.testing import (
    PostgresAsyncTestCase,
)
from minos.networks import (
    Handler,
    MinosActionNotFoundException,
)
from minos.networks.handlers import (
    HandlerEntry,
)
from tests.services.CommandTestService import (
    CommandService,
)
from tests.utils import (
    BASE_PATH,
)


class _FakeHandler(Handler):
    TABLE_NAME = "fake"

    def _build_data(self, value: bytes) -> MinosModel:
        pass

    async def _dispatch_one(self, row: HandlerEntry) -> NoReturn:
        pass


class TestHandler(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def setUp(self) -> None:
        super().setUp()
        handlers = {
            item.name: {"controller": item.controller, "action": item.action} for item in self.config.commands.items
        }
        self.handler = _FakeHandler(
            service_name=self.config.service.name, handlers=handlers, **self.config.commands.queue._asdict()
        )

    async def test_get_action(self):
        action = self.handler.get_action(topic="AddOrder")
        self.assertEqual(CommandService.add_order, action.__func__)

    async def test_get_action_raises(self):
        with self.assertRaises(MinosActionNotFoundException) as context:
            self.handler.get_action(topic="NotExisting")

        self.assertTrue(
            "topic NotExisting have no controller/action configured, please review th configuration file"
            in str(context.exception)
        )
