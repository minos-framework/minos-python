from __future__ import (
    annotations,
)

from collections.abc import (
    Iterable,
)

from kafka.errors import (
    KafkaError,
)

from minos.common import (
    Builder,
    CircuitBreakerMixin,
    Config,
)


class KafkaCircuitBreakerMixin(CircuitBreakerMixin):
    """Kafka Circuit Breaker Mixin class."""

    def __init__(self, *args, circuit_breaker_exceptions: Iterable[type] = tuple(), **kwargs):
        super().__init__(*args, circuit_breaker_exceptions=(KafkaError, *circuit_breaker_exceptions), **kwargs)


class KafkaBrokerBuilderMixin(Builder):
    """Kafka Broker Builder Mixin class."""

    def with_config(self, config: Config):
        """Set config.

        :param config: The config to be set.
        :return: This method return the builder instance.
        """
        broker_config = config.get_interface_by_name("broker")
        common_config = broker_config.get("common", dict())

        self.kwargs |= {
            "group_id": config.get_name(),
            "host": common_config.get("host"),
            "port": common_config.get("port"),
        }
        return super().with_config(config)
