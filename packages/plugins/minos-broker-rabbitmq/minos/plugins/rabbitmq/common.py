from __future__ import (
    annotations,
)

from minos.common import (
    Builder,
    Config,
)


class RabbitMQBrokerBuilderMixin(Builder):
    """RabbitMQ Broker Builder Mixin class."""

    def with_config(self, config: Config):
        """Set config.

        :param config: The config to be set.
        :return: This method return the builder instance.
        """
        broker_config = config.get_interface_by_name("broker")
        common_config = broker_config.get("common", dict())

        self.kwargs |= {
            "host": common_config.get("host"),
            "port": common_config.get("port"),
            "user": common_config.get("user"),
            "password": common_config.get("password"),
        }
        return super().with_config(config)
