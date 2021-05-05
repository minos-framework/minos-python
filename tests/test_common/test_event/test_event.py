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
from tests.aggregate_classes import (
    Car,
    Owner,
)


class TestEvent(unittest.TestCase):
    def test_avro_serialization(self):
        event = Event("CarCreated", [Car(1, 1, 3, "blue"), Car(2, 1, 5, "red")])
        decoded_event = Event.from_avro_bytes(event.avro_bytes)
        self.assertEqual(event, decoded_event)

    def test_items_raises(self):
        with self.assertRaises(MultiTypeMinosModelSequenceException):
            Event("CarCreated", [Car(1, 1, 3, "blue"), Owner(2, 1, "Foo", "Bar")])


if __name__ == "__main__":
    unittest.main()
