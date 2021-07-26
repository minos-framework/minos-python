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
        expected_topics = {"OrderQueryReply", "AddOrderReply", "DeleteOrderReply"}
        consumer = CommandReplyConsumer.from_config(config=self.config)
        self.assertIsInstance(consumer, CommandReplyConsumer)
        self.assertEqual(expected_topics, consumer.topics)
        self.assertEqual(self.config.saga.broker, consumer._broker)
        self.assertEqual(self.config.saga.queue.host, consumer.host)
        self.assertEqual(self.config.saga.queue.port, consumer.port)
        self.assertEqual(self.config.saga.queue.database, consumer.database)
        self.assertEqual(self.config.saga.queue.user, consumer.user)
        self.assertEqual(self.config.saga.queue.password, consumer.password)

    def test_table_name(self):
        self.assertEqual("command_reply_queue", CommandReplyConsumer.TABLE_NAME)


if __name__ == "__main__":
    unittest.main()
