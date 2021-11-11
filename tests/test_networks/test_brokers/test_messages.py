import unittest
from uuid import (
    uuid4,
)

from minos.networks import (
    Command,
    CommandReply,
    CommandStatus,
    Event,
)
from tests.utils import (
    FakeModel,
)


class TestCommand(unittest.TestCase):
    def setUp(self) -> None:
        self.topic = "FooCreated"
        self.data = [FakeModel("blue"), FakeModel("red")]
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
        self.data = [FakeModel("blue"), FakeModel("red")]
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
        self.data = FakeModel("blue")
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
