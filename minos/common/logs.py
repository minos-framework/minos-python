import logging

from aiomisc.log import basic_config

basic_config(
        level=logging.DEBUG, buffered=False, log_format='color'
)
log = logging.getLogger(__name__)
