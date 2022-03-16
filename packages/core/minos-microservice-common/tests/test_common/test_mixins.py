import unittest
from unittest.mock import (
    PropertyMock,
    patch,
)

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
            if mixin.is_circuit_breaker_recovering:
                return 56
            raise TypeError()

        self.assertEqual(56, await mixin.with_circuit_breaker(_fn))

    def test_is_circuit_breaker_passing(self):
        mixin = CircuitBreakerMixin()
        with patch(
            "aiomisc.CircuitBreaker.state", new_callable=PropertyMock, return_value=CircuitBreakerStates.PASSING
        ):
            self.assertTrue(mixin.is_circuit_breaker_passing)
        with patch("aiomisc.CircuitBreaker.state", new_callable=PropertyMock, return_value=CircuitBreakerStates.BROKEN):
            self.assertFalse(mixin.is_circuit_breaker_passing)

    def test_is_circuit_breaker_broken(self):
        mixin = CircuitBreakerMixin()
        with patch("aiomisc.CircuitBreaker.state", new_callable=PropertyMock, return_value=CircuitBreakerStates.BROKEN):
            self.assertTrue(mixin.is_circuit_breaker_broken)
        with patch(
            "aiomisc.CircuitBreaker.state", new_callable=PropertyMock, return_value=CircuitBreakerStates.PASSING
        ):
            self.assertFalse(mixin.is_circuit_breaker_broken)

    def test_is_circuit_breaker_recovering(self):
        mixin = CircuitBreakerMixin()
        with patch(
            "aiomisc.CircuitBreaker.state", new_callable=PropertyMock, return_value=CircuitBreakerStates.RECOVERING
        ):
            self.assertTrue(mixin.is_circuit_breaker_recovering)
        with patch(
            "aiomisc.CircuitBreaker.state", new_callable=PropertyMock, return_value=CircuitBreakerStates.PASSING
        ):
            self.assertFalse(mixin.is_circuit_breaker_recovering)


if __name__ == "__main__":
    unittest.main()
