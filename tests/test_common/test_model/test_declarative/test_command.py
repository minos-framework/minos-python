"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import unittest

from minos.common import (
    Command,
)
from tests.model_classes import (
    Foo,
)


class TestCommand(unittest.TestCase):
    def setUp(self) -> None:
        self.topic = "FooCreated"
        self.items = [Foo("blue"), Foo("red")]

    def test_constructor(self):
        command = Command(self.topic, self.items, "saga_id4234")
        self.assertEqual(self.topic, command.topic)
        self.assertEqual(self.items, command.items)
        self.assertEqual("saga_id4234", command.saga_uuid)
        self.assertEqual(None, command.reply_topic)

    def test_constructor_with_reply_on(self):
        command = Command(self.topic, self.items, "saga_id4234", "AddOrderReply")
        self.assertEqual(self.topic, command.topic)
        self.assertEqual(self.items, command.items)
        self.assertEqual("saga_id4234", command.saga_uuid)
        self.assertEqual("AddOrderReply", command.reply_topic)

    def test_avro_serialization(self):
        command = Command(self.topic, self.items, "saga_id4234", "AddOrderReply")
        decoded_command = Command.from_avro_bytes(command.avro_bytes)
        self.assertEqual(command, decoded_command)


if __name__ == "__main__":
    unittest.main()
