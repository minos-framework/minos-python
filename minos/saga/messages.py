from minos.common import (
    ModelType,
)


# noinspection PyPep8Naming
def SagaRequest(*args, **kwargs) -> ModelType:
    """Build a `SagaRequest` model type.

    :param args: Additional positional parameters.
    :param kwargs: Additional named parameters.
    :return: A ``ModelType`` instance.
    """
    return ModelType.build("SagaRequest", *args, **kwargs)


# noinspection PyPep8Naming
def SagaResponse(*args, **kwargs) -> ModelType:
    """Build a `SagaResponse` model type.

    :param args: Additional positional parameters.
    :param kwargs: Additional named parameters.
    :return: A ``ModelType`` instance.
    """
    return ModelType.build("SagaResponse", *args, **kwargs)
