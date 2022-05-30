from minos.common import (
    DatabaseOperation,
)


class SqlAlchemyDatabaseOperation(DatabaseOperation):
    """TODO"""

    def __init__(self, expression, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.expression = expression
