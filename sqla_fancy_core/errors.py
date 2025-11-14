class FancyError(Exception):
    """Custom error for FancyEngineWrapper."""

    pass


class AtomicContextError(FancyError):
    """Error raised when ax() is called outside of an atomic context."""

    def __init__(self) -> None:
        super().__init__("ax() must be called within the atomic() context manager")


class NotInTransactionError(FancyError):
    """Error raised when tx() is called on a connection not in a transaction."""

    def __init__(self) -> None:
        super().__init__("tx() requires the connection to be in an active transaction")


class UnexpectedAsyncConnectionError(TypeError, FancyError):
    """Error raised when an async connection is used in a sync context."""

    def __init__(self) -> None:
        super().__init__("An async connection was provided in a synchronous context")


class UnsupportedEngineTypeError(TypeError, FancyError):
    """Error raised when an unsupported engine type is provided."""

    def __init__(self) -> None:
        super().__init__("Unsupported engine type provided to the decorator")


class AtomicInsideNonAtomicError(FancyError):
    """Error raised when atomic() is called inside a non_atomic() context."""

    def __init__(self) -> None:
        super().__init__("atomic() cannot be called inside a non_atomic() context")
