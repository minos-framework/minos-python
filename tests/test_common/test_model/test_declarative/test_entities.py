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
)


class TestEvent(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
