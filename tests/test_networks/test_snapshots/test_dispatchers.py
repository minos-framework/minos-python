"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""

import unittest
from datetime import datetime

from minos.common import (
    MinosConfigException,
    MinosRepositoryEntry,
    PostgreSqlMinosRepository,
)
from minos.common.testing import (
    PostgresAsyncTestCase,
)

from minos.networks import (
    MinosSnapshotDispatcher, MinosSnapshotEntry,
)
from tests.aggregate_classes import (
    Car,
)
from tests.utils import (
    BASE_PATH,
)


class TestMinosSnapshotDispatcher(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def test_type(self):
        self.assertTrue(issubclass(MinosSnapshotDispatcher, object))

    def test_from_config(self):
        dispatcher = MinosSnapshotDispatcher.from_config(config=self.config)
        self.assertEqual(self.config.repository.host, dispatcher.host)
        self.assertEqual(self.config.repository.port, dispatcher.port)
        self.assertEqual(self.config.repository.database, dispatcher.database)
        self.assertEqual(self.config.repository.user, dispatcher.user)
        self.assertEqual(self.config.repository.password, dispatcher.password)
        self.assertEqual(0, dispatcher.offset)

    def test_from_config_raises(self):
        with self.assertRaises(MinosConfigException):
            MinosSnapshotDispatcher.from_config()

    async def test_dispatch_select(self):
        await self._populate()
        with self.config:
            dispatcher = MinosSnapshotDispatcher.from_config()
            await dispatcher.setup()
            await dispatcher.dispatch()
            observed = await dispatcher.select()

        expected = [
            MinosSnapshotEntry.from_aggregate(Car(2, 2, 3, "blue")),
            MinosSnapshotEntry.from_aggregate(Car(3, 1, 3, "blue")),
        ]
        self.assertEqual(len(expected), len(observed))
        for exp, obs in zip(expected, observed):
            self.assertEqual(exp.aggregate, obs.aggregate)
            self.assertIsInstance(obs.created_at, datetime)
            self.assertIsInstance(obs.updated_at, datetime)

    async def _populate(self):
        with self.config:
            car = Car(1, 1, 3, "blue")
            # noinspection PyTypeChecker
            aggregate_name: str = car.classname
            repository = PostgreSqlMinosRepository.from_config()
            await repository.setup()
            await repository.insert(MinosRepositoryEntry(1, aggregate_name, 1, car.avro_bytes))
            await repository.update(MinosRepositoryEntry(1, aggregate_name, 2, car.avro_bytes))
            await repository.insert(MinosRepositoryEntry(2, aggregate_name, 1, car.avro_bytes))
            await repository.update(MinosRepositoryEntry(1, aggregate_name, 3, car.avro_bytes))
            await repository.delete(MinosRepositoryEntry(1, aggregate_name, 4))
            await repository.update(MinosRepositoryEntry(2, aggregate_name, 2, car.avro_bytes))
            await repository.insert(MinosRepositoryEntry(3, aggregate_name, 1, car.avro_bytes))


if __name__ == "__main__":
    unittest.main()
