"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""

import unittest

from minos.common import (
    MinosRepository,
    PostgreSqlMinosRepository,
)
from tests.aggregate_classes import Car


class TestPostgreSqlMinosRepository(unittest.IsolatedAsyncioTestCase):
    def test_constructor(self):
        repository = PostgreSqlMinosRepository("localhost", 5432, "postgres", "aiopg", "secret")
        self.assertIsInstance(repository, MinosRepository)
        self.assertEqual("localhost", repository.host)
        self.assertEqual("postgres", repository.database)
        self.assertEqual("aiopg", repository.user)
        self.assertEqual("secret", repository.password)

    @unittest.skip
    async def test_connection(self):
        with PostgreSqlMinosRepository("localhost", 5432, "postgres", "aiopg", "secret") as repository:
            car = await Car.create(doors=3, color="blue", _repository=repository)
            await car.update(color="red")
            await car.update(doors=5)

            another = await Car.get_one(car.id, _repository=repository)
            self.assertEqual(car, another)

            await car.delete()


if __name__ == "__main__":
    unittest.main()
