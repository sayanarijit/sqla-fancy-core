"""Some wrappers for fun times with SQLAlchemy core."""

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

_T = TypeVar("_T", bound=Any)


class FancyEngineWrapper:
    """A wrapper around SQLAlchemy Engine with additional features."""

    def __init__(self, engine: Engine) -> None:
        self.engine = engine

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

        If a connection is provided, use it; otherwise, create a new one.
        """
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

        If a connection is provided, use it; otherwise, create a new one.
        """
        if connection:
            if connection.in_transaction():
                # Transaction is already active
                return connection.execute(
                    statement, parameters, execution_options=execution_options
                )
            else:
                with connection.begin():
                    return connection.execute(
                        statement, parameters, execution_options=execution_options
                    )
        else:
            with self.engine.begin() as connection:
                return connection.execute(
                    statement, parameters, execution_options=execution_options
                )


class AsyncFancyEngineWrapper:
    """A wrapper around SQLAlchemy AsyncEngine with additional features."""

    def __init__(self, engine: AsyncEngine) -> None:
        self.engine = engine

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

        If a connection is provided, use it; otherwise, create a new one.
        """
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

        If a connection is provided, use it; otherwise, create a new one.
        """
        if connection:
            if connection.in_transaction():
                return await connection.execute(
                    statement, parameters, execution_options=execution_options
                )
            else:
                async with connection.begin():
                    return await connection.execute(
                        statement, parameters, execution_options=execution_options
                    )
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
        raise TypeError("Unsupported input type for fancy()")
