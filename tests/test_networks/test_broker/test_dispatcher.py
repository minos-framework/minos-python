import unittest

from minos.common import (
    Aggregate,
)
from minos.common.testing import (
    PostgresAsyncTestCase,
)
from minos.networks import (
    MinosQueueDispatcher,
)
from tests.utils import (
    BASE_PATH,
)


class AggregateTest(Aggregate):
    test: int


class TestQueueDispatcher(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yaml"

    def test_from_config(self):
        dispatcher = MinosQueueDispatcher.from_config(config=self.config)
        self.assertIsInstance(dispatcher, MinosQueueDispatcher)


if __name__ == "__main__":
    unittest.main()
