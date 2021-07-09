"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import unittest
from uuid import uuid4

from minos.common import (
    InMemoryRepository,
    InMemorySnapshot,
    SubAggregate,
)
from minos.common.model.declarative.aggregate.model import NULL_UUID
from tests.aggregate_classes import Car
from tests.utils import FakeBroker
from uuid import UUID


class Product(SubAggregate):
    id: int
    title: str
    quantity: int


class TestAggregate(unittest.IsolatedAsyncioTestCase):
    async def test_instance(self):
        product = Product(id=1234, title="apple", quantity=3028)

        self.assertTrue(isinstance(product, SubAggregate))

    async def test_values(self):
        product = Product(id=1234, title="apple", quantity=3028)

        self.assertTrue(isinstance(product.uuid, UUID))
        self.assertEqual(1234, product.id)
        self.assertEqual("apple", product.title)
        self.assertEqual(3028, product.quantity)
