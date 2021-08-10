"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import unittest
from uuid import (
    UUID,
    uuid4,
)

from minos.common import (
    NULL_UUID,
    DeclarativeModel,
    Entity,
    EntitySet,
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
        self.assertEqual(raw, entities)

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


if __name__ == "__main__":
    unittest.main()
