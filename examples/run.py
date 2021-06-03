"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from minos.common import (
    DependencyInjector,
    EntrypointLauncher,
    MinosConfig,
    PostgreSqlMinosRepository,
)
from minos.networks import (
    CommandBroker,
    CommandConsumerService,
    CommandHandlerService,
    CommandReplyBroker,
    CommandReplyConsumerService,
    CommandReplyHandlerService,
    EventBroker,
    EventConsumerService,
    EventHandlerService,
    ProducerService,
    RestService,
    SnapshotService,
)
from minos.saga import (
    SagaManager,
)

config = MinosConfig("config.yml")
interval = 0.1

injector = DependencyInjector(
    config=config,
    command_broker_cls=CommandBroker,
    command_reply_broker_cls=CommandReplyBroker,
    event_broker_cls=EventBroker,
    repository_cls=PostgreSqlMinosRepository,
    saga_manager_cls=SagaManager,
)

services = [
    CommandConsumerService(config=config),
    CommandHandlerService(config=config, interval=interval),
    CommandReplyConsumerService(config=config),
    CommandReplyHandlerService(config=config, interval=interval),
    EventConsumerService(config=config),
    EventHandlerService(config=config, interval=interval),
    RestService(config=config),
    SnapshotService(config=config, interval=interval),
    ProducerService(config=config, interval=interval),
]

async with EntrypointLauncher(injector=injector, services=services) as launcher:
    launcher.launch()
