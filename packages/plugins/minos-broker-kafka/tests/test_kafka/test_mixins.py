import unittest

from kafka.errors import (
    KafkaError,
)

from minos.plugins.kafka import (
    KafkaCircuitBreakerMixin,
)


class TestKafkaCircuitBreakerMixin(unittest.IsolatedAsyncioTestCase):
    def test_constructor(self):
        mixin = KafkaCircuitBreakerMixin()
        self.assertEqual((KafkaError,), mixin.circuit_breaker_exceptions)


if __name__ == "__main__":
    unittest.main()
