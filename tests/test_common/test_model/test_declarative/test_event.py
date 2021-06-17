"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import unittest

from minos.common import (
    DataTransferObject,
    Event,
    ModelField,
)
from tests.model_classes import (
    Foo,
)


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


class TestEventDto(unittest.TestCase):
    def setUp(self) -> None:
        self.items = [
            DataTransferObject("User", fields={"username": ModelField("username", str, "foo")}),
            DataTransferObject("User", fields={"username": ModelField("username", str, "bar")}),
        ]
        self.topic = "UserCreated"

    def test_avro_serialization(self):
        event = Event(self.topic, self.items)
        decoded_event = Event.from_avro_bytes(event.avro_bytes)
        self.assertEqual(event, decoded_event)


if __name__ == "__main__":
    unittest.main()
