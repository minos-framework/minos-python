"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import re
from uuid import (
    UUID,
)

NULL_UUID = UUID("00000000-0000-0000-0000-000000000000")
UUID_REGEX = re.compile(r"\w{8}-\w{4}-\w{4}-\w{4}-\w{12}")
