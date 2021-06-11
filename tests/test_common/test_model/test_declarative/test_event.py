"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import unittest

from minos.common import (
    Event,
    MultiTypeMinosModelSequenceException,
)
from tests.model_classes import (
    Base,
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

    def test_items_raises(self):
        with self.assertRaises(MultiTypeMinosModelSequenceException):
            Event(self.topic, [Foo("blue"), Base(1)])

    def test_model(self):
        event = Event(self.topic, self.items)
        self.assertEqual("tests.model_classes.Foo", event.model)

    def test_model_cls(self):
        event = Event(self.topic, self.items)
        self.assertEqual(Foo, event.model_cls)


if __name__ == "__main__":
    unittest.main()
