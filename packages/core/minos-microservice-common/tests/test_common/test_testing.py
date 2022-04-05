import unittest
import warnings

from minos.common import (
    Config,
    DependencyInjector,
)
from minos.common.testing import (
    MinosTestCase,
    PostgresAsyncTestCase,
)
from tests.utils import (
    CONFIG_FILE_PATH,
)


class TestMinosTestCase(unittest.IsolatedAsyncioTestCase):
    def test_config(self):
        test_case = MyMinosTestCase()
        test_case.setUp()
        self.assertIsInstance(test_case.config, Config)

    def test_injector(self):
        test_case = MyMinosTestCase()
        test_case.setUp()
        self.assertIsInstance(test_case.injector, DependencyInjector)


class MyMinosTestCase(MinosTestCase):
    CONFIG_FILE_PATH = CONFIG_FILE_PATH


class TestPostgresAsyncTestCase(unittest.IsolatedAsyncioTestCase):
    def test_repository_db(self):
        test_case = MyPostgresAsyncTestCase()
        test_case.setUp()
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            # noinspection PyDeprecation
            self.assertEqual(
                {
                    k: v
                    for k, v in test_case.base_config.get_database_by_name("aggregate").items()
                    if k not in {"database"}
                },
                {k: v for k, v in test_case.repository_db.items() if k not in {"database"}},
            )
            # noinspection PyDeprecation
            self.assertNotEqual(
                test_case.base_config.get_database_by_name("aggregate")["database"],
                test_case.repository_db["database"],
            )

    def test_broker_queue_db(self):
        test_case = MyPostgresAsyncTestCase()
        test_case.setUp()
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            # noinspection PyDeprecation
            self.assertEqual(
                {
                    k: v
                    for k, v in test_case.base_config.get_database_by_name("broker").items()
                    if k not in {"database"}
                },
                {k: v for k, v in test_case.broker_queue_db.items() if k not in {"database"}},
            )
            # noinspection PyDeprecation
            self.assertNotEqual(
                test_case.base_config.get_database_by_name("broker")["database"],
                test_case.broker_queue_db["database"],
            )

    def test_snapshot_db(self):
        test_case = MyPostgresAsyncTestCase()
        test_case.setUp()
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            # noinspection PyDeprecation
            self.assertEqual(
                {
                    k: v
                    for k, v in test_case.base_config.get_database_by_name("aggregate").items()
                    if k not in {"database"}
                },
                {k: v for k, v in test_case.snapshot_db.items() if k not in {"database"}},
            )
            # noinspection PyDeprecation
            self.assertNotEqual(
                test_case.base_config.get_database_by_name("aggregate")["database"],
                test_case.broker_queue_db["database"],
            )


class MyPostgresAsyncTestCase(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = CONFIG_FILE_PATH


if __name__ == "__main__":
    unittest.main()
