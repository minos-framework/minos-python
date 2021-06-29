"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import unittest

from minos.common.testing import (
    PostgresAsyncTestCase,
)
from minos.networks import (
    CommandReplyConsumer,
)
from tests.utils import (
    BASE_PATH,
)


class TestCommandReplyConsumer(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def test_from_config(self):
        dispatcher = CommandReplyConsumer.from_config(config=self.config)
        self.assertIsInstance(dispatcher, CommandReplyConsumer)
        self.assertEqual(["AddOrderReply", "DeleteOrderReply"], dispatcher._topics)
        self.assertEqual(self.config.saga.broker, dispatcher._broker)
        self.assertEqual(self.config.saga.queue.host, dispatcher.host)
        self.assertEqual(self.config.saga.queue.port, dispatcher.port)
        self.assertEqual(self.config.saga.queue.database, dispatcher.database)
        self.assertEqual(self.config.saga.queue.user, dispatcher.user)
        self.assertEqual(self.config.saga.queue.password, dispatcher.password)

    def test_table_name(self):
        self.assertEqual("command_reply_queue", CommandReplyConsumer.TABLE_NAME)


if __name__ == "__main__":
    unittest.main()
