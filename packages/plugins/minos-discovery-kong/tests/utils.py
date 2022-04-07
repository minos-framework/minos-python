import random
import socket
import struct
from pathlib import (
    Path,
)

BASE_PATH = Path(__file__).parent
CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"
TEST_HOST = socket.inet_ntoa(struct.pack(">I", random.randint(1, 0xFFFFFFFF)))
