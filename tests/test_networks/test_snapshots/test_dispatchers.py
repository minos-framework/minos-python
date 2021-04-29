"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""

import unittest

from minos.common import (
    MinosConfigException,
)
from minos.common.testing import (
    PostgresAsyncTestCase,
)

from minos.networks import (
    MinosSnapshotDispatcher,
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


if __name__ == "__main__":
    unittest.main()
