from typing import (
    Optional,
)

from sqlalchemy import (
    text,
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

        if isinstance(expression, str):
            expression = text(expression)

        self.expression = expression
        self.stream = stream
