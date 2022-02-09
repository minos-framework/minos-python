import unittest

from aiomisc import (
    CircuitBreaker,
)
from aiomisc.circuit_breaker import (
    CircuitBreakerStates,
)

from minos.common import (
    CircuitBreakerMixin,
    Object,
)


class TestCircuitBreakerMixin(unittest.IsolatedAsyncioTestCase):
    def test_is_subclass(self):
        self.assertTrue(issubclass(CircuitBreakerMixin, Object))

    def test_constructor(self):
        mixin = CircuitBreakerMixin()
        self.assertIsInstance(mixin.circuit_breaker, CircuitBreaker)
        self.assertEqual((Exception,), mixin.circuit_breaker_exceptions)

    async def test_with_circuit_breaker(self):
        mixin = CircuitBreakerMixin(circuit_breaker_exceptions=[TypeError], circuit_breaker_time=0.1)

        async def _fn(*args, **kwargs):
            if mixin.circuit_breaker.state == CircuitBreakerStates.RECOVERING:
                return 56
            raise TypeError()

        self.assertEqual(56, await mixin.with_circuit_breaker(_fn))


if __name__ == "__main__":
    unittest.main()
