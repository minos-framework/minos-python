import unittest
from uuid import (
    uuid4,
)

from minos.common import (
    current_datetime,
)
from minos.networks import (
    BrokerHandlerEntry,
    BrokerMessage,
)
from tests.utils import (
    FakeModel,
)


class TestHandlerEntry(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.saga = uuid4()
        self.user = uuid4()
        self.service_name = "foo"

        self.message = BrokerMessage(
            "AddOrder", FakeModel("foo"), self.service_name, saga=self.saga, user=self.user, reply_topic="UpdateTicket",
        )

    def test_constructor(self):
        data_bytes = self.message.avro_bytes
        entry = BrokerHandlerEntry(1, "AddOrder", 0, data_bytes, 1)
        self.assertEqual(1, entry.id)
        self.assertEqual("AddOrder", entry.topic)
        self.assertEqual(0, entry.partition)
        self.assertEqual(data_bytes, entry.data_bytes)
        self.assertEqual(1, entry.retry)

    def test_constructor_extended(self):
        data_bytes = self.message.avro_bytes
        now = current_datetime()

        entry = BrokerHandlerEntry(1, "AddOrder", 0, data_bytes, 1, created_at=now, updated_at=now)
        self.assertEqual(1, entry.id)
        self.assertEqual("AddOrder", entry.topic)
        self.assertEqual(0, entry.partition)
        self.assertEqual(data_bytes, entry.data_bytes)
        self.assertEqual(1, entry.retry)
        self.assertEqual(now, entry.created_at)
        self.assertEqual(now, entry.updated_at)

    def test_sort(self):
        unsorted = [
            BrokerHandlerEntry(1, "", 0, BrokerMessage("", "foo", "").avro_bytes, 1),
            BrokerHandlerEntry(1, "", 0, BrokerMessage("", 4, "").avro_bytes, 1),
            BrokerHandlerEntry(1, "", 0, BrokerMessage("", 2, "").avro_bytes, 1),
            BrokerHandlerEntry(1, "", 0, BrokerMessage("", 3, "").avro_bytes, 1),
            BrokerHandlerEntry(1, "", 0, BrokerMessage("", 1, "").avro_bytes, 1),
            BrokerHandlerEntry(1, "", 0, BrokerMessage("", "bar", "").avro_bytes, 1),
        ]

        expected = [unsorted[0], unsorted[4], unsorted[2], unsorted[3], unsorted[1], unsorted[5]]

        observed = sorted(unsorted)
        self.assertEqual(expected, observed)

    def test_eq(self):
        data_bytes = self.message.avro_bytes
        now = current_datetime()
        one = BrokerHandlerEntry(1, "AddOrder", 0, data_bytes, 1, created_at=now, updated_at=now)
        two = BrokerHandlerEntry(1, "AddOrder", 0, data_bytes, 1, created_at=now, updated_at=now)
        self.assertEqual(one, two)


if __name__ == "__main__":
    unittest.main()
