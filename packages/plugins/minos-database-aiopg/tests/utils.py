from pathlib import (
    Path,
)

from minos.common.testing import (
    MinosTestCase,
)

BASE_PATH = Path(__file__).parent
CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"


class CommonTestCase(MinosTestCase):
    def get_config_file_path(self) -> Path:
        return CONFIG_FILE_PATH
