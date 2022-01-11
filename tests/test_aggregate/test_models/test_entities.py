import unittest
from operator import (
    attrgetter,
)
from unittest.mock import (
    patch,
)
from uuid import (
    UUID,
    uuid4,
)

from minos.aggregate import (
    Action,
    Entity,
    EntitySet,
    IncrementalSetDiff,
    IncrementalSetDiffEntry,
)
from minos.common import (
    NULL_UUID,
    DeclarativeModel,
    Model,
)


class _Entity(Entity):
    """For testing purposes."""

    name: str


class TestEntity(unittest.TestCase):
    def test_default(self):
        entity = Entity()
        self.assertIsInstance(entity, DeclarativeModel)
        self.assertIsNot(entity.uuid, NULL_UUID)
        self.assertIsInstance(entity.uuid, UUID)

    def test_uuid(self):
        uuid = uuid4()
        entity = Entity(uuid=uuid)
        self.assertIsInstance(entity, DeclarativeModel)
        self.assertIsNot(entity.uuid, NULL_UUID)
        self.assertEqual(uuid, entity.uuid)


class TestEntitySet(unittest.TestCase):
    def test_data(self):
        raw = {_Entity("John"), _Entity("Michael")}

        entities = EntitySet(raw)
        self.assertEqual({str(v.uuid): v for v in raw}, entities.data)

    def test_eq_true(self):
        raw = {_Entity("John"), _Entity("Michael")}
        observed = EntitySet(raw)
        self.assertEqual(EntitySet(raw), observed)
        self.assertEqual(raw, observed)
        self.assertEqual({str(v.uuid): v for v in raw}, observed)

    def test_eq_false(self):
        raw = {_Entity("John"), _Entity("Michael")}
        observed = EntitySet(raw)
        other = {_Entity("Charlie")}
        self.assertNotEqual(EntitySet(other), observed)
        self.assertNotEqual(other, observed)
        self.assertNotEqual({str(v.uuid): v for v in other}, observed)
        self.assertNotEqual(list(raw), observed)

    def test_len(self):
        raw = {_Entity("John"), _Entity("Michael")}

        entities = EntitySet(raw)
        self.assertEqual(2, len(entities))

    def test_iter(self):
        raw = {_Entity("John"), _Entity("Michael")}

        entities = EntitySet(raw)
        self.assertEqual(raw, set(entities))

    def test_contains(self):
        raw = [_Entity("John")]

        entities = EntitySet(raw)

        self.assertIn(raw[0], entities)
        self.assertNotIn(_Entity("Charlie"), entities)
        self.assertNotIn(1234, entities)

    def test_add(self):
        raw = _Entity("John")

        entities = EntitySet()
        entities.add(raw)

        self.assertEqual({raw}, entities)

    def test_get(self):
        raw = _Entity("John")

        entities = EntitySet()
        entities.add(raw)
        self.assertEqual(raw, entities.get(raw.uuid))

    def test_remove(self):
        raw = [_Entity("John"), _Entity("Michael")]

        entities = EntitySet(raw)
        entities.remove(raw[1])

        self.assertEqual({raw[0]}, entities)

    def test_diff(self):
        raw = [_Entity("John"), _Entity("Michael")]
        entities = EntitySet(raw)

        observed = entities.diff(EntitySet([raw[0]]))
        expected = IncrementalSetDiff([IncrementalSetDiffEntry(Action.CREATE, raw[1])])

        self.assertEqual(observed, expected)

    def test_data_cls(self):
        raw = [_Entity("John"), _Entity("Michael")]
        entities = EntitySet(raw)
        self.assertEqual(_Entity, entities.data_cls)

    def test_from_avro(self):
        values = {_Entity("John"), _Entity("Michael")}
        expected = EntitySet(values)
        schema = [
            {
                "logicalType": "minos.aggregate.models.entities.EntitySet",
                "type": "map",
                "values": _Entity.avro_schema[0],
            },
        ]
        data = {str(v.uuid): v.avro_data for v in values}

        observed = Model.from_avro(schema, data)
        self.assertEqual(expected, observed)

    def test_avro_schema(self):
        with patch("minos.common.AvroSchemaEncoder.generate_random_str", return_value="hello"):
            expected = [
                {
                    "logicalType": "minos.aggregate.models.entities.EntitySet",
                    "type": "map",
                    "values": _Entity.avro_schema[0],
                },
            ]
            observed = EntitySet({_Entity("John"), _Entity("Michael")}).avro_schema
        self.assertEqual(expected, observed)

    def test_avro_data(self):
        values = {_Entity("John"), _Entity("Michael")}
        expected = {str(v.uuid): v.avro_data for v in values}
        observed = EntitySet(values).avro_data
        self.assertEqual(expected, observed)

    def test_avro_bytes(self):
        expected = EntitySet({_Entity("John"), _Entity("Michael")})
        self.assertEqual(expected, Model.from_avro_bytes(expected.avro_bytes))


class TestEntitySetDiff(unittest.TestCase):
    def setUp(self) -> None:
        self.raw = [_Entity("John"), _Entity("Michael")]
        self.old = EntitySet(self.raw)

        self.clone = [_Entity(name=entity.name, uuid=entity.uuid) for entity in self.raw]

    def test_from_difference_create(self):
        entities = EntitySet(self.clone)
        new = _Entity("Charlie")
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
        new = _Entity("Charlie")
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
