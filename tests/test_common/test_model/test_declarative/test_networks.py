"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import unittest

from minos.common import (
    Command,
    CommandReply,
    Event,
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


class TestCommandReply(unittest.TestCase):
    def setUp(self) -> None:
        self.topic = "FooCreated"
        self.items = [Foo("blue"), Foo("red")]
        self.saga_uuid = "saga_id8972348237"

    def test_constructor(self):
        command_reply = CommandReply(self.topic, self.items, self.saga_uuid)
        self.assertEqual(self.topic, command_reply.topic)
        self.assertEqual(self.items, command_reply.items)
        self.assertEqual(self.saga_uuid, command_reply.saga_uuid)

    def test_avro_serialization(self):
        command_reply = CommandReply(self.topic, self.items, self.saga_uuid)
        decoded_command = CommandReply.from_avro_bytes(command_reply.avro_bytes)
        self.assertEqual(command_reply, decoded_command)


class TestEvent(unittest.TestCase):
    def setUp(self) -> None:
        self.items = [Foo("blue"), Foo("red")]
        self.topic = "FooCreated"

    def test_constructor(self):
        event = Event(self.topic, self.items)
        self.assertEqual(self.topic, event.topic)
        self.assertEqual(self.items, event.items)

    def test_avro_serialization(self):
        event = Event(self.topic, self.items)
        decoded_event = Event.from_avro_bytes(event.avro_bytes)
        self.assertEqual(event, decoded_event)


if __name__ == "__main__":
    unittest.main()
