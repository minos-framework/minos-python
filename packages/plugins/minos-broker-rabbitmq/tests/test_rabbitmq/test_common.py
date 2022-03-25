import unittest

from minos.common import (
    Config,
)
from minos.plugins.rabbitmq import (
    RabbitMQBrokerBuilderMixin,
)
from tests.utils import (
    CONFIG_FILE_PATH,
)


class TestRabbitMQBrokerBuilderMixin(unittest.IsolatedAsyncioTestCase):
    def test_constructor(self):
        mixin = RabbitMQBrokerBuilderMixin()

        config = Config(CONFIG_FILE_PATH)
        mixin.with_config(config)

        common_config = config.get_interface_by_name("broker")["common"]

        expected = {
            "host": common_config["host"],
            "port": common_config["port"],
        }
        self.assertEqual(expected, mixin.kwargs)


if __name__ == "__main__":
    unittest.main()
