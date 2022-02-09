from collections.abc import (
    Iterable,
)

from kafka.errors import (
    KafkaError,
)

from minos.common import (
    CircuitBreakerMixin,
)


class KafkaCircuitBreakerMixin(CircuitBreakerMixin):
    """Kafka Circuit Breaker Mixin class."""

    def __init__(self, *args, circuit_breaker_exceptions: Iterable[type] = tuple(), **kwargs):
        super().__init__(*args, circuit_breaker_exceptions=(KafkaError, *circuit_breaker_exceptions), **kwargs)
