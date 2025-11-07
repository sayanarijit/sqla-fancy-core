"""Some decorators for fun times with SQLAlchemy core."""

import functools
import inspect
from typing import Union, get_args, get_origin

try:
    from typing import Annotated
except ImportError:
    Annotated = None

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

EngineType = Union[sa.engine.Engine, AsyncEngine]


def transact(engine: EngineType):
    """A decorator that provides a transactional context.

    If the decorated function is called with a connection object, that
    connection is used. Otherwise, a new transaction is started from the
    engine, and the new connection is passed to the function. The connection
    argument is identified by its type annotation.

    Example: ::
        @transact(engine)
        def create_user(conn: sa.Connection, name: str):
            conn.execute(...)

        # This will create a new transaction
        create_user(name="test")

        # This will use the existing connection
        with engine.connect() as conn:
            create_user(conn, name="existing")

    """
    is_async = isinstance(engine, AsyncEngine)

    def decorator(func):
        if is_async and not inspect.iscoroutinefunction(func):
            raise TypeError("Async engine requires an async function.")
        if not is_async and inspect.iscoroutinefunction(func):
            raise TypeError("Sync engine requires a sync function.")

        sig = inspect.signature(func)

        conn_param_name = None
        conn_type = AsyncConnection if is_async else sa.Connection

        try:
            type_hints = inspect.get_annotations(func)
            for param_name, param_type in type_hints.items():
                if param_type is conn_type:
                    conn_param_name = param_name
                    break
                if Annotated and get_origin(param_type) is Annotated:
                    actual_type = get_args(param_type)[0]
                    if actual_type is conn_type:
                        conn_param_name = param_name
                        break
        except Exception:
            pass

        if conn_param_name is None:
            # Fallback for when get_annotations fails or doesn't find it
            for param in sig.parameters.values():
                param_type = param.annotation
                if param_type is conn_type:
                    conn_param_name = param.name
                    break
                if Annotated and get_origin(param_type) is Annotated:
                    actual_type = get_args(param_type)[0]
                    if actual_type is conn_type:
                        conn_param_name = param.name
                        break

        if conn_param_name is None:
            raise TypeError(
                f"{func.__name__} must have an argument typed as "
                f"{'AsyncConnection' if is_async else 'Connection'}."
            )

        conn_param = sig.parameters[conn_param_name]

        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            bound_args = sig.bind_partial(*args, **kwargs)
            if conn_param.name in bound_args.arguments:
                conn = bound_args.arguments[conn_param.name]
                if conn.in_transaction():
                    with conn.begin_nested():
                        return func(*args, **kwargs)
                else:
                    with conn.begin():
                        return func(*args, **kwargs)
            else:
                # A connection was not provided, so create one.
                with engine.begin() as conn:  # type: ignore
                    kwargs[conn_param.name] = conn
                    return func(*args, **kwargs)

        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            bound_args = sig.bind_partial(*args, **kwargs)
            if conn_param.name in bound_args.arguments:
                conn = bound_args.arguments[conn_param.name]
                if conn.in_transaction():
                    async with conn.begin_nested():
                        return await func(*args, **kwargs)
                else:
                    async with conn.begin():
                        return await func(*args, **kwargs)
            else:
                # A connection was not provided, so create one.
                async with engine.begin() as conn:  # type: ignore
                    kwargs[conn_param.name] = conn
                    return await func(*args, **kwargs)

        return async_wrapper if is_async else sync_wrapper

    return decorator
