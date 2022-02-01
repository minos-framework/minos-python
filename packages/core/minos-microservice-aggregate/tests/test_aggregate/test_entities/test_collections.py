import unittest
from operator import (
    attrgetter,
)
from unittest.mock import (
    patch,
)

from minos.aggregate import (
    Action,
    EntitySet,
    IncrementalSetDiff,
    IncrementalSetDiffEntry,
)
from minos.common import (
    Model,
)
from tests.utils import (
    OrderItem,
)


class TestEntitySet(unittest.TestCase):
    def test_data(self):
        raw = {OrderItem("John"), OrderItem("Michael")}

        entities = EntitySet(raw)
        self.assertEqual({str(v.uuid): v for v in raw}, entities.data)

    def test_eq_true(self):
        raw = {OrderItem("John"), OrderItem("Michael")}
        observed = EntitySet(raw)
        self.assertEqual(EntitySet(raw), observed)
        self.assertEqual(raw, observed)
        self.assertEqual({str(v.uuid): v for v in raw}, observed)

    def test_eq_false(self):
        raw = {OrderItem("John"), OrderItem("Michael")}
        observed = EntitySet(raw)
        other = {OrderItem("Charlie")}
        self.assertNotEqual(EntitySet(other), observed)
        self.assertNotEqual(other, observed)
        self.assertNotEqual({str(v.uuid): v for v in other}, observed)
        self.assertNotEqual(list(raw), observed)

    def test_len(self):
        raw = {OrderItem("John"), OrderItem("Michael")}

        entities = EntitySet(raw)
        self.assertEqual(2, len(entities))

    def test_iter(self):
        raw = {OrderItem("John"), OrderItem("Michael")}

        entities = EntitySet(raw)
        self.assertEqual(raw, set(entities))

    def test_contains(self):
        raw = [OrderItem("John")]

        entities = EntitySet(raw)

        self.assertIn(raw[0], entities)
        self.assertNotIn(OrderItem("Charlie"), entities)
        self.assertNotIn(1234, entities)

    def test_add(self):
        raw = OrderItem("John")

        entities = EntitySet()
        entities.add(raw)

        self.assertEqual({raw}, entities)

    def test_get(self):
        raw = OrderItem("John")

        entities = EntitySet()
        entities.add(raw)
        self.assertEqual(raw, entities.get(raw.uuid))

    def test_remove(self):
        raw = [OrderItem("John"), OrderItem("Michael")]

        entities = EntitySet(raw)
        entities.remove(raw[1])

        self.assertEqual({raw[0]}, entities)

    def test_diff(self):
        raw = [OrderItem("John"), OrderItem("Michael")]
        entities = EntitySet(raw)

        observed = entities.diff(EntitySet([raw[0]]))
        expected = IncrementalSetDiff([IncrementalSetDiffEntry(Action.CREATE, raw[1])])

        self.assertEqual(observed, expected)

    def test_data_cls(self):
        raw = [OrderItem("John"), OrderItem("Michael")]
        entities = EntitySet(raw)
        self.assertEqual(OrderItem, entities.data_cls)

    def test_from_avro(self):
        values = {OrderItem("John"), OrderItem("Michael")}
        expected = EntitySet(values)
        schema = [
            {
                "logicalType": "minos.aggregate.entities.EntitySet",
                "type": "array",
                "items": OrderItem.avro_schema[0],
            },
        ]
        data = [v.avro_data for v in values]

        observed = Model.from_avro(schema, data)
        self.assertEqual(expected, observed)

    def test_avro_schema(self):
        with patch("minos.common.AvroSchemaEncoder.generate_random_str", return_value="hello"):
            expected = [
                {
                    "logicalType": "minos.aggregate.entities.collections.EntitySet",
                    "type": "array",
                    "items": OrderItem.avro_schema[0],
                },
            ]
            observed = EntitySet({OrderItem("John"), OrderItem("Michael")}).avro_schema
        self.assertEqual(expected, observed)

    def test_avro_data(self):
        values = {OrderItem("John"), OrderItem("Michael")}
        expected = sorted([v.avro_data for v in values], key=lambda v: v["uuid"])
        observed = EntitySet(values).avro_data
        self.assertIsInstance(observed, list)
        self.assertEqual(expected, sorted([v.avro_data for v in values], key=lambda v: v["uuid"]))

    def test_avro_bytes(self):
        expected = EntitySet({OrderItem("John"), OrderItem("Michael")})
        self.assertEqual(expected, Model.from_avro_bytes(expected.avro_bytes))


class TestEntitySetDiff(unittest.TestCase):
    def setUp(self) -> None:
        self.raw = [OrderItem("John"), OrderItem("Michael")]
        self.old = EntitySet(self.raw)

        self.clone = [OrderItem(name=entity.name, uuid=entity.uuid) for entity in self.raw]

    def test_from_difference_create(self):
        entities = EntitySet(self.clone)
        new = OrderItem("Charlie")
        entities.add(new)

        observed = IncrementalSetDiff.from_difference(entities, self.old, get_fn=attrgetter("uuid"))
        expected = IncrementalSetDiff([IncrementalSetDiffEntry(Action.CREATE, new)])
        self.assertEqual(expected, observed)

    def test_from_difference_delete(self):
        entities = EntitySet(self.clone)
        removed = self.clone[1]
        entities.remove(removed)

        observed = IncrementalSetDiff.from_difference(entities, self.old, get_fn=attrgetter("uuid"))
        expected = IncrementalSetDiff([IncrementalSetDiffEntry(Action.DELETE, removed)])
        self.assertEqual(expected, observed)

    def test_from_difference_update(self):
        entities = EntitySet(self.clone)
        updated = entities.get(self.clone[0].uuid)
        updated.name = "Ryan"

        observed = IncrementalSetDiff.from_difference(entities, self.old, get_fn=attrgetter("uuid"))
        expected = IncrementalSetDiff([IncrementalSetDiffEntry(Action.UPDATE, updated)])
        self.assertEqual(expected, observed)

    def test_from_difference_combined(self):
        entities = EntitySet(self.clone)
        new = OrderItem("Charlie")
        entities.add(new)

        removed = self.clone[1]
        entities.remove(removed)

        updated = entities.get(self.clone[0].uuid)
        updated.name = "Ryan"

        observed = IncrementalSetDiff.from_difference(entities, self.old, get_fn=attrgetter("uuid"))

        expected = IncrementalSetDiff(
            [
                IncrementalSetDiffEntry(Action.CREATE, new),
                IncrementalSetDiffEntry(Action.DELETE, removed),
                IncrementalSetDiffEntry(Action.UPDATE, updated),
            ]
        )
        self.assertEqual(expected, observed)


if __name__ == "__main__":
    unittest.main()
