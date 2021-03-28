class MinosException(Exception):
    """Exception class for import packages or modules"""
    __slots__ = ('_message')

    def __init__(self, error_message: str):
        self._message = error_message

    def __str__(self) -> str:
        """represent in a string format the error message passed during the instantiation"""
        return self._message


class MinosImportException(MinosException): pass
class MinosProtocolException(MinosException): pass
class MinosMessageException(MinosException): pass
class MinosConfigException(MinosException): pass
