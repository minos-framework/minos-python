"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import unittest

from minos.common import (
    Command,
)
from tests.aggregate_classes import (
    Car,
)


class TestCommand(unittest.TestCase):
    def test_constructor(self):
        command = Command("CarCreated", [Car(1, 1, 3, "blue"), Car(2, 1, 5, "red")], "saga_id4234", "task_id324532")
        self.assertEqual("CarCreated", command.topic)
        self.assertEqual([Car(1, 1, 3, "blue"), Car(2, 1, 5, "red")], command.items)
        self.assertEqual("saga_id4234", command.saga_id)
        self.assertEqual("task_id324532", command.task_id)
        self.assertEqual(None, command.reply_on)

    def test_constructor_with_reply_on(self):
        command = Command(
            "CarCreated", [Car(1, 1, 3, "blue"), Car(2, 1, 5, "red")], "saga_id4234", "task_id324532", "AddOrderReply"
        )
        self.assertEqual("CarCreated", command.topic)
        self.assertEqual([Car(1, 1, 3, "blue"), Car(2, 1, 5, "red")], command.items)
        self.assertEqual("saga_id4234", command.saga_id)
        self.assertEqual("task_id324532", command.task_id)
        self.assertEqual("AddOrderReply", command.reply_on)

    def test_avro_serialization(self):
        command = Command(
            "CarCreated", [Car(1, 1, 3, "blue"), Car(2, 1, 5, "red")], "saga_id4234", "task_id324532", "AddOrderReply"
        )
        decoded_command = Command.from_avro_bytes(command.avro_bytes)
        self.assertEqual(command, decoded_command)


if __name__ == "__main__":
    unittest.main()
