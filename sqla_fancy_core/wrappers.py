"""Some wrappers for fun times with SQLAlchemy core."""

from contextlib import asynccontextmanager, contextmanager
from contextvars import ContextVar
from typing import Any, Optional, TypeVar, overload

from sqlalchemy import Connection, CursorResult, Engine, Executable
from sqlalchemy.engine.interfaces import (
    CoreExecuteOptionsParameter,
    _CoreAnyExecuteParams,
)
from sqlalchemy.ext.asyncio import (
    AsyncConnection,
    AsyncEngine,
)
from sqlalchemy.sql.selectable import TypedReturnsRows

from sqla_fancy_core.errors import (
    AtomicContextError,
    AtomicInsideNonAtomicError,
    NotInTransactionError,
    UnexpectedAsyncConnectionError,
)

_T = TypeVar("_T", bound=Any)


class FancyEngineWrapper:
    """A wrapper around SQLAlchemy Engine with additional features."""

    __CONN: ContextVar[Optional[Connection]] = ContextVar(  # type: ignore
        "__CONN", default=None
    )

    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    @contextmanager
    def non_atomic(self):
        """A context manager that provides a non-transactional connection."""
        non_atomic_txn_conn = self.__CONN.get()
        if non_atomic_txn_conn is not None:
            yield non_atomic_txn_conn
        else:
            with self.engine.connect() as connection:
                token = self.__CONN.set(connection)
                try:
                    yield connection
                finally:
                    self.__CONN.reset(token)

    @contextmanager
    def atomic(self):
        """A context manager that provides a transactional connection."""
        atomic_txn_conn = self.__CONN.get()
        if atomic_txn_conn is not None:
            if atomic_txn_conn.in_transaction():
                yield atomic_txn_conn
            else:
                raise AtomicInsideNonAtomicError()
        else:
            with self.engine.begin() as connection:
                token = self.__CONN.set(connection)
                try:
                    yield connection
                finally:
                    self.__CONN.reset(token)

    @overload
    def nax(
        self,
        statement: TypedReturnsRows[_T],
        parameters: Optional[_CoreAnyExecuteParams] = None,
        *,
        execution_options: Optional[CoreExecuteOptionsParameter] = None,
    ) -> CursorResult[_T]: ...
    @overload
    def nax(
        self,
        statement: Executable,
        parameters: Optional[_CoreAnyExecuteParams] = None,
        *,
        execution_options: Optional[CoreExecuteOptionsParameter] = None,
    ) -> CursorResult[Any]: ...
    def nax(
        self,
        statement: Executable,
        parameters: Optional[_CoreAnyExecuteParams] = None,
        *,
        execution_options: Optional[CoreExecuteOptionsParameter] = None,
    ) -> CursorResult[Any]:
        """Execute the query within the atomic/non-atomic context and return the result.

        If not within a atomic/non-atomic context, a new connection is created.
        """
        connection = self.__CONN.get()
        if connection:
            return connection.execute(
                statement, parameters, execution_options=execution_options
            )
        else:
            with self.engine.connect() as connection:
                return connection.execute(
                    statement, parameters, execution_options=execution_options
                )

    @overload
    def ax(
        self,
        statement: TypedReturnsRows[_T],
        parameters: Optional[_CoreAnyExecuteParams] = None,
        *,
        execution_options: Optional[CoreExecuteOptionsParameter] = None,
    ) -> CursorResult[_T]: ...
    @overload
    def ax(
        self,
        statement: Executable,
        parameters: Optional[_CoreAnyExecuteParams] = None,
        *,
        execution_options: Optional[CoreExecuteOptionsParameter] = None,
    ) -> CursorResult[Any]: ...
    def ax(
        self,
        statement: Executable,
        parameters: Optional[_CoreAnyExecuteParams] = None,
        *,
        execution_options: Optional[CoreExecuteOptionsParameter] = None,
    ) -> CursorResult[Any]:
        """Execute the query within the atomic context and return the result.

        It must be called within the `atomic` context manager. Else an error is raised.
        """
        connection = self.__CONN.get()
        if connection:
            if connection.in_transaction():
                return connection.execute(
                    statement, parameters, execution_options=execution_options
                )
            else:
                raise NotInTransactionError()
        else:
            raise AtomicContextError()

    @overload
    def x(
        self,
        connection: Optional[Connection],
        statement: TypedReturnsRows[_T],
        parameters: Optional[_CoreAnyExecuteParams] = None,
        *,
        execution_options: Optional[CoreExecuteOptionsParameter] = None,
    ) -> CursorResult[_T]: ...
    @overload
    def x(
        self,
        connection: Optional[Connection],
        statement: Executable,
        parameters: Optional[_CoreAnyExecuteParams] = None,
        *,
        execution_options: Optional[CoreExecuteOptionsParameter] = None,
    ) -> CursorResult[Any]: ...
    def x(
        self,
        connection: Optional[Connection],
        statement: Executable,
        parameters: Optional[_CoreAnyExecuteParams] = None,
        *,
        execution_options: Optional[CoreExecuteOptionsParameter] = None,
    ) -> CursorResult[Any]:
        """Connect to the database, execute the query, and return the result.

        If a connection is provided, use it; otherwise, use the atomic/non-atomic context
        or create a new one.
        """
        connection = connection or self.__CONN.get()
        if connection:
            return connection.execute(
                statement, parameters, execution_options=execution_options
            )
        else:
            with self.engine.connect() as connection:
                return connection.execute(
                    statement, parameters, execution_options=execution_options
                )

    @overload
    def tx(
        self,
        connection: Optional[Connection],
        statement: TypedReturnsRows[_T],
        parameters: Optional[_CoreAnyExecuteParams] = None,
        *,
        execution_options: Optional[CoreExecuteOptionsParameter] = None,
    ) -> CursorResult[_T]: ...
    @overload
    def tx(
        self,
        connection: Optional[Connection],
        statement: Executable,
        parameters: Optional[_CoreAnyExecuteParams] = None,
        *,
        execution_options: Optional[CoreExecuteOptionsParameter] = None,
    ) -> CursorResult[Any]: ...
    def tx(
        self,
        connection: Optional[Connection],
        statement: Executable,
        parameters: Optional[_CoreAnyExecuteParams] = None,
        *,
        execution_options: Optional[CoreExecuteOptionsParameter] = None,
    ) -> CursorResult[Any]:
        """Begin a transaction, execute the query, and return the result.

        If a connection is provided, use it; otherwise, use the atomic
        context or create a new one.
        """
        connection = connection or self.__CONN.get()
        if connection:
            if connection.in_transaction():
                return connection.execute(
                    statement, parameters, execution_options=execution_options
                )
            else:
                raise NotInTransactionError()
        else:
            with self.engine.begin() as connection:
                return connection.execute(
                    statement, parameters, execution_options=execution_options
                )

    @overload
    def atx(
        self,
        statement: TypedReturnsRows[_T],
        parameters: Optional[_CoreAnyExecuteParams] = None,
        *,
        execution_options: Optional[CoreExecuteOptionsParameter] = None,
    ) -> CursorResult[_T]: ...
    @overload
    def atx(
        self,
        statement: Executable,
        parameters: Optional[_CoreAnyExecuteParams] = None,
        *,
        execution_options: Optional[CoreExecuteOptionsParameter] = None,
    ) -> CursorResult[Any]: ...
    def atx(
        self,
        statement: Executable,
        parameters: Optional[_CoreAnyExecuteParams] = None,
        *,
        execution_options: Optional[CoreExecuteOptionsParameter] = None,
    ) -> CursorResult[Any]:
        """If within an atomic context, execute the query there; else, create a new transaction."""

        conn = self.__CONN.get()
        if conn:
            if conn.in_transaction():
                return conn.execute(
                    statement, parameters, execution_options=execution_options
                )
            else:
                raise NotInTransactionError()
        else:
            with self.engine.begin() as conn:
                return conn.execute(
                    statement, parameters, execution_options=execution_options
                )


class AsyncFancyEngineWrapper:
    """A wrapper around SQLAlchemy AsyncEngine with additional features."""

    __CONN: ContextVar[Optional[AsyncConnection]] = ContextVar(  # type: ignore
        "__CONN", default=None
    )

    def __init__(self, engine: AsyncEngine) -> None:
        self.engine = engine

    @asynccontextmanager
    async def non_atomic(self):
        """An async context manager that provides a non-transactional connection."""
        non_atomic_txn_conn = self.__CONN.get()
        if non_atomic_txn_conn is not None:
            yield non_atomic_txn_conn
        else:
            async with self.engine.connect() as connection:
                token = self.__CONN.set(connection)
                try:
                    yield connection
                finally:
                    self.__CONN.reset(token)

    @asynccontextmanager
    async def atomic(self):
        """An async context manager that provides a transactional connection."""
        atomic_txn_conn = self.__CONN.get()
        if atomic_txn_conn is not None:
            if atomic_txn_conn.in_transaction():
                yield atomic_txn_conn
            else:
                raise AtomicInsideNonAtomicError()
        else:
            async with self.engine.begin() as connection:
                token = self.__CONN.set(connection)
                try:
                    yield connection
                finally:
                    self.__CONN.reset(token)

    @overload
    async def nax(
        self,
        statement: TypedReturnsRows[_T],
        parameters: Optional[_CoreAnyExecuteParams] = None,
        *,
        execution_options: Optional[CoreExecuteOptionsParameter] = None,
    ) -> CursorResult[_T]: ...
    @overload
    async def nax(
        self,
        statement: Executable,
        parameters: Optional[_CoreAnyExecuteParams] = None,
        *,
        execution_options: Optional[CoreExecuteOptionsParameter] = None,
    ) -> CursorResult[Any]: ...
    async def nax(
        self,
        statement: Executable,
        parameters: Optional[_CoreAnyExecuteParams] = None,
        *,
        execution_options: Optional[CoreExecuteOptionsParameter] = None,
    ) -> CursorResult[Any]:
        """Execute the query within the atomic/non_atomic context and return the result.

        If not within a atomic/non-atomic context, a new connection is created.
        """
        connection = self.__CONN.get()
        if connection:
            return await connection.execute(
                statement, parameters, execution_options=execution_options
            )
        else:
            async with self.engine.connect() as connection:
                return await connection.execute(
                    statement, parameters, execution_options=execution_options
                )

    @overload
    async def ax(
        self,
        statement: TypedReturnsRows[_T],
        parameters: Optional[_CoreAnyExecuteParams] = None,
        *,
        execution_options: Optional[CoreExecuteOptionsParameter] = None,
    ) -> CursorResult[_T]: ...
    @overload
    async def ax(
        self,
        statement: Executable,
        parameters: Optional[_CoreAnyExecuteParams] = None,
        *,
        execution_options: Optional[CoreExecuteOptionsParameter] = None,
    ) -> CursorResult[Any]: ...
    async def ax(
        self,
        statement: Executable,
        parameters: Optional[_CoreAnyExecuteParams] = None,
        *,
        execution_options: Optional[CoreExecuteOptionsParameter] = None,
    ) -> CursorResult[Any]:
        """Execute the query within the atomic context and return the result.

        It must be called within the `atomic` context manager. Else an error is raised.
        """
        connection = self.__CONN.get()
        if connection:
            if connection.in_transaction():
                return await connection.execute(
                    statement, parameters, execution_options=execution_options
                )
            else:
                raise NotInTransactionError()
        else:
            raise AtomicContextError()

    @overload
    async def x(
        self,
        connection: Optional[AsyncConnection],
        statement: TypedReturnsRows[_T],
        parameters: Optional[_CoreAnyExecuteParams] = None,
        *,
        execution_options: Optional[CoreExecuteOptionsParameter] = None,
    ) -> CursorResult[_T]: ...
    @overload
    async def x(
        self,
        connection: Optional[AsyncConnection],
        statement: Executable,
        parameters: Optional[_CoreAnyExecuteParams] = None,
        *,
        execution_options: Optional[CoreExecuteOptionsParameter] = None,
    ) -> CursorResult[Any]: ...
    async def x(
        self,
        connection: Optional[AsyncConnection],
        statement: Executable,
        parameters: Optional[_CoreAnyExecuteParams] = None,
        *,
        execution_options: Optional[CoreExecuteOptionsParameter] = None,
    ) -> CursorResult[Any]:
        """Connect to the database, execute the query, and return the result.

        If a connection is provided, use it; otherwise, use the atomic/non-atomic context
        or create a new one.
        """
        connection = connection or self.__CONN.get()
        if connection:
            return await connection.execute(
                statement, parameters, execution_options=execution_options
            )
        else:
            async with self.engine.connect() as connection:
                return await connection.execute(
                    statement, parameters, execution_options=execution_options
                )

    @overload
    async def tx(
        self,
        connection: Optional[AsyncConnection],
        statement: TypedReturnsRows[_T],
        parameters: Optional[_CoreAnyExecuteParams] = None,
        *,
        execution_options: Optional[CoreExecuteOptionsParameter] = None,
    ) -> CursorResult[_T]: ...
    @overload
    async def tx(
        self,
        connection: Optional[AsyncConnection],
        statement: Executable,
        parameters: Optional[_CoreAnyExecuteParams] = None,
        *,
        execution_options: Optional[CoreExecuteOptionsParameter] = None,
    ) -> CursorResult[Any]: ...
    async def tx(
        self,
        connection: Optional[AsyncConnection],
        statement: Executable,
        parameters: Optional[_CoreAnyExecuteParams] = None,
        *,
        execution_options: Optional[CoreExecuteOptionsParameter] = None,
    ) -> CursorResult[Any]:
        """Execute the query within a transaction and return the result.

        If a connection is provided, use it; otherwise, use the atomic
        context or create a new one.
        """
        connection = connection or self.__CONN.get()
        if connection:
            if connection.in_transaction():
                return await connection.execute(
                    statement, parameters, execution_options=execution_options
                )
            else:
                raise NotInTransactionError()
        else:
            async with self.engine.begin() as connection:
                return await connection.execute(
                    statement, parameters, execution_options=execution_options
                )

    @overload
    async def atx(
        self,
        statement: TypedReturnsRows[_T],
        parameters: Optional[_CoreAnyExecuteParams] = None,
        *,
        execution_options: Optional[CoreExecuteOptionsParameter] = None,
    ) -> CursorResult[_T]: ...
    @overload
    async def atx(
        self,
        statement: Executable,
        parameters: Optional[_CoreAnyExecuteParams] = None,
        *,
        execution_options: Optional[CoreExecuteOptionsParameter] = None,
    ) -> CursorResult[Any]: ...
    async def atx(
        self,
        statement: Executable,
        parameters: Optional[_CoreAnyExecuteParams] = None,
        *,
        execution_options: Optional[CoreExecuteOptionsParameter] = None,
    ) -> CursorResult[Any]:
        """If within an atomic context, execute the query there; else, create a new transaction."""

        connection = self.__CONN.get()
        if connection:
            if connection.in_transaction():
                return await connection.execute(
                    statement, parameters, execution_options=execution_options
                )
            else:
                raise NotInTransactionError()
        else:
            async with self.engine.begin() as connection:
                return await connection.execute(
                    statement, parameters, execution_options=execution_options
                )


@overload
def fancy(obj: Engine, /) -> FancyEngineWrapper: ...
@overload
def fancy(obj: AsyncEngine, /) -> AsyncFancyEngineWrapper: ...
def fancy(obj, /):
    """Fancy engine wrapper makes the following syntax possible: ::

    import sqlalchemy as sa

    fancy_engine = fancy(sa.create_engine("sqlite:///:memory:"))

    def handler(conn: sa.Connection | None = None):
        # Execute a query outside of a transaction
        result = fancy_engine.x(conn, sa.select(...))

        # Execute a query within a transaction
        result = fancy_engine.tx(conn, sa.insert(...))

    # Using an explicit connection:
    with fancy_engine.engine.connect() as conn:
        handler(conn=conn)

    # Using a dependency injection system:
        handler(conn=dependency(transaction))  # Uses the provided transaction connection

    # Or without a given connection (e.g. in IPython shell):
        handler()
    """
    if isinstance(obj, Engine):
        return FancyEngineWrapper(obj)
    elif isinstance(obj, AsyncEngine):
        return AsyncFancyEngineWrapper(obj)
    else:
        raise UnexpectedAsyncConnectionError()
