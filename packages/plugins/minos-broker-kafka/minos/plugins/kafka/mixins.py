import logging
from asyncio import (
    sleep,
)
from collections.abc import (
    Awaitable,
    Callable,
)
from inspect import (
    isawaitable,
)
from typing import (
    TypeVar,
    Union,
)

from aiomisc import (
    CircuitBreaker,
    CircuitBroken,
)
from kafka.errors import (
    KafkaError,
)

logger = logging.getLogger(__name__)
R = TypeVar("R")


class KafkaCircuitBreakerMixin:
    """Kafka Circuit Breaker Mixin class."""

    def __init__(self, circuit_breaker_error_rate: float = 0.2, circuit_breaker_time: Union[int, float] = 3, **kwargs):
        self._circuit_breaker = CircuitBreaker(
            error_ratio=circuit_breaker_error_rate, response_time=circuit_breaker_time, exceptions=[KafkaError]
        )

    async def _with_circuit_breaker(self, fn: Callable[[], Union[Awaitable[R], R]]) -> R:
        while True:
            try:
                with self._circuit_breaker.context():
                    ans = fn()
                    if isawaitable(ans):
                        ans = await ans
                    return ans
            except CircuitBroken:
                await sleep(self._circuit_breaker_timeout)
            except KafkaError as exc:
                logger.warning(f"A kafka exception was raised: {exc!r}")
                await sleep(self._exception_timeout)

    @property
    def _circuit_breaker_timeout(self) -> float:
        return self._circuit_breaker.response_time * 0.5

    @property
    def _exception_timeout(self) -> float:
        return self._circuit_breaker.response_time * 0.05

    @property
    def circuit_breaker(self) -> CircuitBreaker:
        """Get the circuit breaker.

        :return: A ``CircuitBreaker`` instance.
        """
        return self._circuit_breaker
