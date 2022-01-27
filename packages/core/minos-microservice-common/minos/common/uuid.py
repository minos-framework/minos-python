import re
from uuid import (
    UUID,
)

NULL_UUID = UUID("00000000-0000-0000-0000-000000000000")
UUID_REGEX = re.compile(r"\w{8}-\w{4}-\w{4}-\w{4}-\w{12}")
