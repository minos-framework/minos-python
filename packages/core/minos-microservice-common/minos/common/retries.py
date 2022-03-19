import logging
from asyncio import (
    sleep,
)
from collections.abc import (
    Awaitable,
    Callable,
    Iterable,
)
from inspect import (
    isawaitable,
)
from typing import (
    Generic,
    TypeVar,
    Union,
)

from aiomisc import (
    CircuitBreaker,
    CircuitBroken,
)
from aiomisc.circuit_breaker import (
    CircuitBreakerStates,
)

from .object import (
    Object,
)

logger = logging.getLogger(__name__)
R = TypeVar("R")
E = TypeVar("E")


class CircuitBreakerMixin(Generic[E], Object):
    """Circuit Breaker Mixin class."""

    def __init__(
        self,
        circuit_breaker_error_rate: float = 0.2,
        circuit_breaker_time: Union[int, float] = 3,
        circuit_breaker_exceptions: Iterable[type[Exception]] = (Exception,),
        **kwargs,
    ):
        super().__init__(**kwargs)

        self._circuit_breaker_error_rate = circuit_breaker_error_rate
        self._circuit_breaker_time = circuit_breaker_time
        self._circuit_breaker_exceptions = tuple(circuit_breaker_exceptions)

        self._circuit_breaker = CircuitBreaker(
            error_ratio=self._circuit_breaker_error_rate,
            response_time=self._circuit_breaker_time,
            exceptions=self._circuit_breaker_exceptions,
        )

    async def with_circuit_breaker(self, fn: Callable[[], Union[Awaitable[R], R]]) -> R:
        """Execute the given function with circuit breaker.

        :param fn: The function to be executed.
        :return: The return of the given function.
        """
        while True:
            try:
                with self._circuit_breaker.context():
                    ans = fn()
                    if isawaitable(ans):
                        ans = await ans
                    return ans
            except CircuitBroken:
                await sleep(self._circuit_breaker_broken_timeout)
            except self._circuit_breaker_exceptions as exc:
                logger.warning(f"An exception was raised: {exc!r}")
                await sleep(self._circuit_breaker_exception_timeout)

    @property
    def _circuit_breaker_broken_timeout(self) -> float:
        return self._circuit_breaker_time * 0.5

    @property
    def _circuit_breaker_exception_timeout(self) -> float:
        return self._circuit_breaker_time * 0.05

    @property
    def circuit_breaker(self) -> CircuitBreaker:
        """Get the circuit breaker.

        :return: A ``CircuitBreaker`` instance.
        """
        return self._circuit_breaker

    @property
    def is_circuit_breaker_passing(self) -> bool:
        """Check if circuit breaker is passing.

        :return: A ``bool`` instance.
        """
        return self._circuit_breaker.state == CircuitBreakerStates.PASSING

    @property
    def is_circuit_breaker_broken(self) -> bool:
        """Check if circuit breaker is passing.

        :return: A ``bool`` instance.
        """
        return self._circuit_breaker.state == CircuitBreakerStates.BROKEN

    @property
    def is_circuit_breaker_recovering(self) -> bool:
        """Check if circuit breaker is passing.

        :return: A ``bool`` instance.
        """
        return self._circuit_breaker.state == CircuitBreakerStates.RECOVERING

    @property
    def circuit_breaker_exceptions(self) -> tuple[type[Exception]]:
        """Get the circuit breaker exceptions.

        :return: A tuple of ``Exception`` types.
        """
        return self._circuit_breaker_exceptions
