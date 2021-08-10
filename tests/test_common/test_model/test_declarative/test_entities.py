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
    DeclarativeModel,
    Entity,
    EntitySet,
)

NULL_UUID = UUID("00000000-0000-0000-0000-000000000000")


class FakeEntity(Entity):
    """For testing purposes."""

    name: str


class TestEvent(unittest.TestCase):
    def test_default(self):
        entity = Entity()
        self.assertIsInstance(entity, DeclarativeModel)

    def test_uuid(self):
        entity = Entity(uuid=uuid4())
        self.assertIsInstance(entity, DeclarativeModel)
        self.assertIsNot(entity.uuid, NULL_UUID)


class TestEntitySet(unittest.TestCase):
    def test_constructor(self):
        raw = {FakeEntity("John"), FakeEntity("Michael")}

        entities = EntitySet(raw)
        self.assertEqual({str(v.uuid): v for v in raw}, entities.data)


if __name__ == "__main__":
    unittest.main()
