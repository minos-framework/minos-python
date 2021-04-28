"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import unittest
from typing import (
    NoReturn,
)

from minos.common import (
    Aggregate,
    Command,
    Event,
    MinosBaseBroker,
    MultiTypeMinosModelSequenceException,
)
from tests.aggregate_classes import (
    Car,
    Owner,
)


class MinosBroker(MinosBaseBroker):
    async def send(self, items: list[Aggregate]) -> NoReturn:
        pass


class TestMinosBaseBroker(unittest.IsolatedAsyncioTestCase):
    def test_topic(self):
        broker = MinosBroker("CarAdded")
        self.assertEqual("CarAdded", broker.topic)

    async def test_send(self):
        broker = MinosBroker("CarAdded")
        self.assertEqual(None, await broker.send([Car(1, 1, 3, "red"), Car(1, 1, 3, "red")]))

    async def test_send_one(self):
        broker = MinosBroker("CarAdded")
        self.assertEqual(None, await broker.send_one(Car(1, 1, 3, "red")))


class TestEvent(unittest.TestCase):
    def test_avro_serialization(self):
        event = Event("CarCreated", [Car(1, 1, 3, "blue"), Car(2, 1, 5, "red")])
        decoded_event = Event.from_avro_bytes(event.avro_bytes)
        self.assertEqual(event, decoded_event)

    def test_items_raises(self):
        with self.assertRaises(MultiTypeMinosModelSequenceException):
            Event("CarCreated", [Car(1, 1, 3, "blue"), Owner(2, 1, "Foo", "Bar")])


class TestCommand(unittest.TestCase):
    def test_avro_serialization(self):
        command = Command("CarCreated", [Car(1, 1, 3, "blue"), Car(2, 1, 5, "red")], "reply_fn()")
        decoded_command = Command.from_avro_bytes(command.avro_bytes)
        self.assertEqual(command, decoded_command)


if __name__ == "__main__":
    unittest.main()
