import unittest
from uuid import (
    uuid4,
)

from tests.utils import (
    Product,
)


class TestExternalEntity(unittest.TestCase):
    def test_values(self):
        uuid = uuid4()
        product = Product(uuid, 3, "apple", 3028)

        self.assertEqual(uuid, product.uuid)
        self.assertEqual(3, product.version)
        self.assertEqual("apple", product.title)
        self.assertEqual(3028, product.quantity)


if __name__ == "__main__":
    unittest.main()
