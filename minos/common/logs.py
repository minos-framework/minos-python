"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""

import logging

from aiomisc.log import (
    basic_config,
)

basic_config(level=logging.DEBUG, buffered=False, log_format="color")
log = logging.getLogger(__name__)
