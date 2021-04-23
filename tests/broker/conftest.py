import pytest

from minos.common.configuration.config import MinosConfig
from minos.networks.broker import MinosBrokerDatabase, BrokerDatabaseInitializer, EventBrokerQueueDispatcher
from aiomisc.service.periodic import Service
from aiokafka import AIOKafkaConsumer
from minos.common.logs import log


@pytest.fixture
def broker_config():
    return MinosConfig(path="./tests/test_config.yaml")


@pytest.fixture
async def database(broker_config):
    return await MinosBrokerDatabase().get_connection(broker_config)


@pytest.fixture
def services(broker_config):
    return [BrokerDatabaseInitializer(config=broker_config), EventBrokerQueueDispatcher(interval=0.5, delay=0, config=broker_config)]


class KafkaConsumer(Service):
    async def start(self):
        self.start_event.set()

        consumer = AIOKafkaConsumer(
            'EventBroker', "CommandBroker",
            loop=self.loop,
            bootstrap_servers='localhost:9092')
        # Get cluster layout and join group `my-group`

        await consumer.start()
        try:
            # Consume messages
            async for msg in consumer:
                log.debug("+++++++++++++++++++++++++++++++++++++++++++++++")
                log.debug(msg.topic)
                log.debug(msg.key)
                log.debug(msg.value)
                log.debug("++++++++++++++++++++++++++++++++++++++++++++++++")
                #log.debug("consumed: ", msg.topic, msg.partition, msg.offset,
                #      msg.key, msg.value, msg.timestamp)
        finally:
            # Will leave consumer group; perform autocommit if enabled.
            await consumer.stop()

        await self.stop(self)
