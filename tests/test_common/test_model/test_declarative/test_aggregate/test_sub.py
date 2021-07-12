"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import unittest
from uuid import (
    uuid4,
)

from minos.common import (
    SubAggregate,
)


class Product(SubAggregate):
    """For testing purposes."""

    title: str
    quantity: int


class TestSubAggregate(unittest.TestCase):
    def test_values(self):
        uuid = uuid4()
        product = Product(uuid, 3, "apple", 3028)

        self.assertEqual(uuid, product.uuid)
        self.assertEqual(3, product.version)
        self.assertEqual("apple", product.title)
        self.assertEqual(3028, product.quantity)


if __name__ == "__main__":
    unittest.main()
