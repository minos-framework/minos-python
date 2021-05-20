"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import sys
import unittest

from minos.common import (
    MinosDependencyInjector,
    MinosRepositoryDeletedAggregateException,
    PostgreSqlMinosRepository,
)
from minos.common.testing import (
    PostgresAsyncTestCase,
)
from tests.aggregate_classes import (
    Car,
)
from tests.utils import (
    BASE_PATH,
)


class TestAggregateWithConfig(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.injector = MinosDependencyInjector(self.config, repository_cls=PostgreSqlMinosRepository)
        await self.injector.wire(modules=[sys.modules[__name__]])

    async def asyncTearDown(self):
        await self.injector.unwire()
        await super().asyncTearDown()

    async def test_update(self):
        car = await Car.create(doors=3, color="blue")

        await car.update(color="red")
        self.assertEqual(Car(1, 2, 3, "red"), car)
        self.assertEqual(car, await Car.get_one(car.id))

        await car.update(doors=5)
        self.assertEqual(Car(1, 3, 5, "red"), car)
        self.assertEqual(car, await Car.get_one(car.id))

        await car.delete()
        with self.assertRaises(MinosRepositoryDeletedAggregateException):
            await Car.get_one(car.id)

        car = await Car.create(doors=3, color="blue")
        await Car.update(car.id, color="red")
        self.assertEqual(Car(2, 2, 3, "red"), await Car.get_one(car.id))

        await Car.delete(car.id)
        with self.assertRaises(MinosRepositoryDeletedAggregateException):
            await Car.get_one(car.id)


if __name__ == "__main__":
    unittest.main()
