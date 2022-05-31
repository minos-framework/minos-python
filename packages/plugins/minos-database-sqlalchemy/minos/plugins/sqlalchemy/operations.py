from typing import (
    Optional,
)

from minos.common import (
    DatabaseOperation,
)


class SqlAlchemyDatabaseOperation(DatabaseOperation):
    """TODO"""

    def __init__(self, expression, stream: Optional[bool] = None, *args, **kwargs):
        if stream is None:
            stream = True
        super().__init__(*args, **kwargs)
        self.expression = expression
        self.stream = stream
