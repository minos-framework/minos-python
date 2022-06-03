from pathlib import (
    Path,
)

from minos.common.testing import (
    DatabaseMinosTestCase,
)

BASE_PATH = Path(__file__).parent
CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"


class SqlAlchemyTestCase(DatabaseMinosTestCase):
    """For testing purposes."""

    def get_config_file_path(self) -> Path:
        """For testing purposes."""
        return CONFIG_FILE_PATH

    def get_injections(self):
        """For testing purposes."""
        return []
