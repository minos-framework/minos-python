import unittest
from uuid import (
    UUID,
    uuid4,
)

from minos.common import (
    NULL_UUID,
    Action,
    DeclarativeModel,
    Entity,
    EntitySet,
    EntitySetDiff,
    EntitySetDiffEntry,
)
from tests.utils import (
    FakeEntity,
)


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
        raw = {FakeEntity("John"), FakeEntity("Michael")}

        entities = EntitySet(raw)
        self.assertEqual({str(v.uuid): v for v in raw}, entities.data)

    def test_eq_true(self):
        raw = {FakeEntity("John"), FakeEntity("Michael")}
        observed = EntitySet(raw)
        self.assertEqual(EntitySet(raw), observed)
        self.assertEqual(raw, observed)
        self.assertEqual({str(v.uuid): v for v in raw}, observed)

    def test_eq_false(self):
        raw = {FakeEntity("John"), FakeEntity("Michael")}
        observed = EntitySet(raw)
        other = {FakeEntity("Charlie")}
        self.assertNotEqual(EntitySet(other), observed)
        self.assertNotEqual(other, observed)
        self.assertNotEqual({str(v.uuid): v for v in other}, observed)
        self.assertNotEqual(list(raw), observed)

    def test_len(self):
        raw = {FakeEntity("John"), FakeEntity("Michael")}

        entities = EntitySet(raw)
        self.assertEqual(2, len(entities))

    def test_iter(self):
        raw = {FakeEntity("John"), FakeEntity("Michael")}

        entities = EntitySet(raw)
        self.assertEqual(raw, set(entities))

    def test_contains(self):
        raw = [FakeEntity("John")]

        entities = EntitySet(raw)

        self.assertIn(raw[0], entities)
        self.assertNotIn(FakeEntity("Charlie"), entities)
        self.assertNotIn(1234, entities)

    def test_add(self):
        raw = FakeEntity("John")

        entities = EntitySet()
        entities.add(raw)

        self.assertEqual({raw}, entities)

    def test_get(self):
        raw = FakeEntity("John")

        entities = EntitySet()
        entities.add(raw)
        self.assertEqual(raw, entities.get(raw.uuid))

    def test_remove(self):
        raw = [FakeEntity("John"), FakeEntity("Michael")]

        entities = EntitySet(raw)
        entities.remove(raw[1])

        self.assertEqual({raw[0]}, entities)

    def test_avro_serialization(self):
        base = EntitySet({FakeEntity("John"), FakeEntity("Michael")})
        recovered = EntitySet.from_avro_bytes(base.avro_bytes)
        self.assertEqual(base, recovered)

    def test_diff(self):
        raw = [FakeEntity("John"), FakeEntity("Michael")]
        entities = EntitySet(raw)

        observed = entities.diff(EntitySet([raw[0]]))
        expected = EntitySetDiff([EntitySetDiffEntry(Action.CREATE, raw[1])])

        self.assertEqual(observed, expected)


class TestEntitySetDiff(unittest.TestCase):
    def setUp(self) -> None:
        self.raw = [FakeEntity("John"), FakeEntity("Michael")]
        self.old = EntitySet(self.raw)

        self.clone = [FakeEntity(name=entity.name, uuid=entity.uuid) for entity in self.raw]

    def test_from_difference_create(self):
        entities = EntitySet(self.clone)
        new = FakeEntity("Charlie")
        entities.add(new)

        observed = EntitySetDiff.from_difference(entities, self.old)
        expected = EntitySetDiff([EntitySetDiffEntry(Action.CREATE, new)])
        self.assertEqual(expected, observed)

    def test_from_difference_delete(self):
        entities = EntitySet(self.clone)
        removed = self.clone[1]
        entities.remove(removed)

        observed = EntitySetDiff.from_difference(entities, self.old)
        expected = EntitySetDiff([EntitySetDiffEntry(Action.DELETE, removed)])
        self.assertEqual(expected, observed)

    def test_from_difference_update(self):
        entities = EntitySet(self.clone)
        updated = entities.get(self.clone[0].uuid)
        updated.name = "Ryan"

        observed = EntitySetDiff.from_difference(entities, self.old)
        expected = EntitySetDiff([EntitySetDiffEntry(Action.UPDATE, updated)])
        self.assertEqual(expected, observed)

    def test_from_difference_combined(self):
        entities = EntitySet(self.clone)
        new = FakeEntity("Charlie")
        entities.add(new)

        removed = self.clone[1]
        entities.remove(removed)

        updated = entities.get(self.clone[0].uuid)
        updated.name = "Ryan"

        observed = EntitySetDiff.from_difference(entities, self.old)

        expected = EntitySetDiff(
            [
                EntitySetDiffEntry(Action.CREATE, new),
                EntitySetDiffEntry(Action.DELETE, removed),
                EntitySetDiffEntry(Action.UPDATE, updated),
            ]
        )
        self.assertEqual(expected, observed)


if __name__ == "__main__":
    unittest.main()
