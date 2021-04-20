# import asyncio
# import logging
#
# import pytest
# import string
# from aiokafka import AIOKafkaProducer
# import random
#
# from aiomisc.log import basic_config
# from minos.common.configuration.config import MinosConfig
#
# from minos.networks.event import MinosEventServer
#
#
# @pytest.fixture()
# def config():
#     return MinosConfig(path='./tests/test_config.yaml')
#
#
# @pytest.fixture()
# def services(config):
#     return [MinosEventServer(conf=config)]
#
#
# async def test_producer_kafka(loop):
#     basic_config(
#         level=logging.INFO,
#         buffered=True,
#         log_format='color',
#         flush_interval=2
#     )
#
#     producer = AIOKafkaProducer(loop=loop, bootstrap_servers='localhost:9092')
#     # Get cluster layout and topic/partition allocation
#     await producer.start()
#     # Produce messages
#     string_to_send = ''.join(random.choices(string.ascii_uppercase + string.digits, k=20))
#     await producer.send_and_wait("TicketAdded", string_to_send.encode())
#     await asyncio.sleep(1)
#
#     other_string_to_send = ''.join(random.choices(string.ascii_uppercase + string.digits, k=40))
#     await producer.send_and_wait("TicketDeleted", other_string_to_send.encode())
#     await asyncio.sleep(1)
#     await producer.stop()
