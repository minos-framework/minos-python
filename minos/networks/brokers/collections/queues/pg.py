from __future__ import (
    annotations,
)

import logging
from abc import (
    ABC,
)

from minos.common import (
    PostgreSqlMinosDatabase,
)

from .abc import (
    BrokerRepository,
)

logger = logging.getLogger(__name__)


class PostgreSqlBrokerRepository(BrokerRepository, PostgreSqlMinosDatabase, ABC):
    """PostgreSql Broker Publisher Repository class."""
