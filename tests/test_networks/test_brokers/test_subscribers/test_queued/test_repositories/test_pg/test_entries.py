import unittest
from uuid import (
    uuid4,
)

from minos.common import (
    current_datetime,
)
from tests.utils import (
    FakeModel,
)


@unittest.skip("FIXME!")
class TestHandlerEntry(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.identifier = uuid4()
        self.user = uuid4()

        self.data = FakeModel("foo")
        self.data_bytes = self.data.avro_bytes

    def test_constructor(self):
        entry = BrokerHandlerEntry(1, "AddOrder", 0, self.data_bytes, 1)  # noqa
        self.assertEqual(1, entry.id_)
        self.assertEqual("AddOrder", entry.topic)
        self.assertEqual(0, entry.partition)
        self.assertEqual(self.data_bytes, entry.data_bytes)
        self.assertEqual(1, entry.retry)

    def test_constructor_extended(self):
        now = current_datetime()

        entry = BrokerHandlerEntry(1, "AddOrder", 0, self.data_bytes, 1, created_at=now, updated_at=now)  # noqa
        self.assertEqual(1, entry.id_)
        self.assertEqual("AddOrder", entry.topic)
        self.assertEqual(0, entry.partition)
        self.assertEqual(self.data_bytes, entry.data_bytes)
        self.assertEqual(1, entry.retry)
        self.assertEqual(now, entry.created_at)
        self.assertEqual(now, entry.updated_at)

    def test_data(self):
        entry = BrokerHandlerEntry(1, "AddOrder", 0, self.data_bytes, 1)  # noqa
        self.assertEqual(self.data, entry.data)

    def test_sort(self):
        unsorted = [
            BrokerHandlerEntry(1, "", 0, FakeModel("foo").avro_bytes, 1),  # noqa
            BrokerHandlerEntry(2, "", 0, FakeModel(4).avro_bytes, 1),  # noqa
            BrokerHandlerEntry(3, "", 0, FakeModel(2).avro_bytes, 1),  # noqa
            BrokerHandlerEntry(4, "", 0, FakeModel(3).avro_bytes, 1),  # noqa
            BrokerHandlerEntry(5, "", 0, FakeModel(1).avro_bytes, 1),  # noqa
            BrokerHandlerEntry(6, "", 0, FakeModel("bar").avro_bytes, 1),  # noqa
        ]

        expected = [unsorted[0], unsorted[4], unsorted[2], unsorted[3], unsorted[1], unsorted[5]]

        observed = sorted(unsorted)
        self.assertEqual(expected, observed)

    def test_eq(self):
        now = current_datetime()
        one = BrokerHandlerEntry(1, "AddOrder", 0, self.data_bytes, 1, created_at=now, updated_at=now)  # noqa
        two = BrokerHandlerEntry(1, "AddOrder", 0, self.data_bytes, 1, created_at=now, updated_at=now)  # noqa
        self.assertEqual(one, two)

    def test_repr_ok(self):
        entry = BrokerHandlerEntry(1, "AddOrder", 0, bytes())  # noqa
        self.assertEqual("BrokerHandlerEntry(1, 'AddOrder')", repr(entry))


if __name__ == "__main__":
    unittest.main()
