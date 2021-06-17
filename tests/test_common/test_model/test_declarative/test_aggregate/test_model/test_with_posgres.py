"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import sys
import unittest

from dependency_injector import (
    containers,
    providers,
)

from minos.common import (
    MinosRepositoryDeletedAggregateException,
    PostgreSqlRepository,
    PostgreSqlSnapshot,
)
from minos.common.testing import (
    PostgresAsyncTestCase,
)
from tests.aggregate_classes import (
    Car,
)
from tests.utils import (
    BASE_PATH,
    FakeBroker,
)


class TestAggregateWithPostgres(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.container = containers.DynamicContainer()
        self.container.event_broker = providers.Object(FakeBroker())
        self.container.repository = providers.Singleton(PostgreSqlRepository.from_config, config=self.config)
        self.container.snapshot = providers.Singleton(
            PostgreSqlSnapshot.from_config, config=self.config, repository=self.container.repository
        )
        await self.container.repository().setup()
        await self.container.snapshot().setup()
        self.container.wire(modules=[sys.modules[__name__]])

    async def asyncTearDown(self):
        self.container.unwire()
        await self.container.repository().destroy()
        await self.container.snapshot().destroy()
        await super().asyncTearDown()

    async def test_update(self):
        car = await Car.create(doors=3, color="blue")

        await car.update(color="red")
        self.assertEqual(Car(3, "red", id=1, version=2), car)
        self.assertEqual(car, await Car.get_one(car.id))

        await car.update(doors=5)
        self.assertEqual(Car(5, "red", id=1, version=3), car)
        self.assertEqual(car, await Car.get_one(car.id))

        await car.delete()
        with self.assertRaises(MinosRepositoryDeletedAggregateException):
            await Car.get_one(car.id)

        car = await Car.create(doors=3, color="blue")
        await car.update(color="red")
        self.assertEqual(Car(3, "red", id=2, version=2), await Car.get_one(car.id))

        await car.delete()
        with self.assertRaises(MinosRepositoryDeletedAggregateException):
            await Car.get_one(car.id)


if __name__ == "__main__":
    unittest.main()
