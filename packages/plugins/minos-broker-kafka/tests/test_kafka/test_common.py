import unittest

from kafka.errors import (
    KafkaError,
)

from minos.common import (
    Config,
)
from minos.plugins.kafka import (
    KafkaBrokerBuilderMixin,
    KafkaCircuitBreakerMixin,
)
from tests.utils import (
    CONFIG_FILE_PATH,
)


class TestKafkaCircuitBreakerMixin(unittest.IsolatedAsyncioTestCase):
    def test_constructor(self):
        mixin = KafkaCircuitBreakerMixin()
        self.assertEqual((KafkaError,), mixin.circuit_breaker_exceptions)


class TestKafkaBrokerBuilderMixin(unittest.IsolatedAsyncioTestCase):
    def test_constructor(self):
        mixin = KafkaBrokerBuilderMixin()

        config = Config(CONFIG_FILE_PATH)
        mixin.with_config(config)

        common_config = config.get_interface_by_name("broker")["common"]

        expected = {
            "group_id": config.get_name(),
            "broker_host": common_config["host"],
            "broker_port": common_config["port"],
        }
        self.assertEqual(expected, mixin.kwargs)


if __name__ == "__main__":
    unittest.main()
