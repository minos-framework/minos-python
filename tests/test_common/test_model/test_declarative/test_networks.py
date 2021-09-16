import unittest
from uuid import (
    uuid4,
)

from minos.common import (
    Action,
    AggregateDiff,
    Command,
    CommandReply,
    CommandStatus,
    Event,
    FieldDiff,
    FieldDiffContainer,
    current_datetime,
)
from tests.model_classes import (
    Foo,
)


class TestCommand(unittest.TestCase):
    def setUp(self) -> None:
        self.topic = "FooCreated"
        self.data = [Foo("blue"), Foo("red")]
        self.saga = uuid4()
        self.reply_topic = "AddOrderReply"
        self.user = uuid4()

    def test_constructor(self):
        command = Command(self.topic, self.data, self.saga, self.reply_topic, self.user)
        self.assertEqual(self.topic, command.topic)
        self.assertEqual(self.data, command.data)
        self.assertEqual(self.saga, command.saga)
        self.assertEqual(self.reply_topic, command.reply_topic)
        self.assertEqual(self.user, command.user)

    def test_avro_serialization(self):
        command = Command(self.topic, self.data, self.saga, self.reply_topic, self.user)
        decoded_command = Command.from_avro_bytes(command.avro_bytes)
        self.assertEqual(command, decoded_command)


class TestCommandReply(unittest.TestCase):
    def setUp(self) -> None:
        self.topic = "FooCreated"
        self.data = [Foo("blue"), Foo("red")]
        self.saga = uuid4()
        self.status = CommandStatus.SUCCESS

    def test_constructor(self):
        command_reply = CommandReply(self.topic, self.data, self.saga, self.status)
        self.assertEqual(self.topic, command_reply.topic)
        self.assertEqual(self.data, command_reply.data)
        self.assertEqual(self.saga, command_reply.saga)
        self.assertEqual(self.status, command_reply.status)

    def test_avro_serialization(self):
        command_reply = CommandReply(self.topic, self.data, self.saga, self.status)
        decoded_command = CommandReply.from_avro_bytes(command_reply.avro_bytes)
        self.assertEqual(command_reply, decoded_command)


class TestEvent(unittest.TestCase):
    def setUp(self) -> None:
        self.data = AggregateDiff(
            uuid4(), "Foo", 3, Action.CREATE, current_datetime(), FieldDiffContainer([FieldDiff("doors", int, 5)]),
        )
        self.topic = "FooCreated"

    def test_constructor(self):
        event = Event(self.topic, self.data)
        self.assertEqual(self.topic, event.topic)
        self.assertEqual(self.data, event.data)

    def test_avro_serialization(self):
        event = Event(self.topic, self.data)
        decoded_event = Event.from_avro_bytes(event.avro_bytes)
        self.assertEqual(event, decoded_event)


if __name__ == "__main__":
    unittest.main()
