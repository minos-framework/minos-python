"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import unittest

from minos.common import (
    CommandReply,
)
from tests.model_classes import (
    Foo,
)


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


if __name__ == "__main__":
    unittest.main()
