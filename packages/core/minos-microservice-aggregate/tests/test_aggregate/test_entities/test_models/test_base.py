import unittest
from uuid import (
    UUID,
    uuid4,
)

from minos.aggregate import (
    Entity,
)
from minos.common import (
    NULL_UUID,
    DeclarativeModel,
)
from tests.utils import (
    OrderItem,
)


class TestEntity(unittest.TestCase):
    def test_subclass(self):
        self.assertTrue(issubclass(Entity, DeclarativeModel))

    def test_default(self):
        entity = OrderItem("foo")
        self.assertIsInstance(entity, DeclarativeModel)
        self.assertIsNot(entity.uuid, NULL_UUID)
        self.assertIsInstance(entity.uuid, UUID)
        self.assertEqual("foo", entity.name)

    def test_uuid(self):
        uuid = uuid4()
        entity = OrderItem("foo", uuid=uuid)
        self.assertIsInstance(entity, DeclarativeModel)
        self.assertIsNot(entity.uuid, NULL_UUID)
        self.assertEqual(uuid, entity.uuid)
        self.assertEqual("foo", entity.name)


if __name__ == "__main__":
    unittest.main()
