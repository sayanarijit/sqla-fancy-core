"""Some decorators for fun times with SQLAlchemy core."""

import functools
import inspect
from typing import Union, overload

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

EngineType = Union[sa.Engine, AsyncEngine]


class _Injectable:
    def __init__(self, engine: EngineType):
        self.engine = engine


@overload
def Inject(engine: sa.Engine) -> sa.Connection: ...
@overload
def Inject(engine: AsyncEngine) -> AsyncConnection: ...
def Inject(engine: EngineType):  # type: ignore
    """A marker class for dependency injection."""
    return _Injectable(engine)


def transact(func):
    """A decorator that provides a transactional context.

    If the decorated function is called with a connection object, that
    connection is used. Otherwise, a new transaction is started from the
    engine, and the new connection is injected to the function.

    Example: ::
        @transact
        def create_user(name: str, conn: sa.Connection = Inject(engine)):
            conn.execute(...)

        # This will create a new transaction
        create_user("test")

        # This will use the existing connection
        with engine.connect() as conn:
            create_user(name="existing", conn=conn)
    """

    # Find the parameter with value Inject
    sig = inspect.signature(func)
    inject_param_name = None
    for name, param in sig.parameters.items():
        if param.default is not inspect.Parameter.empty and isinstance(
            param.default, _Injectable
        ):
            inject_param_name = name
            break
    if inject_param_name is None:
        return func  # No injection needed

    engine = sig.parameters[inject_param_name].default.engine
    is_async = isinstance(engine, AsyncEngine)

    if is_async:

        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            conn = kwargs.get(inject_param_name)
            if isinstance(conn, AsyncConnection):
                if conn.in_transaction():
                    return await func(*args, **kwargs)
                else:
                    async with conn.begin():
                        return await func(*args, **kwargs)
            else:
                async with engine.begin() as conn:
                    kwargs[inject_param_name] = conn
                    return await func(*args, **kwargs)

        return async_wrapper

    else:

        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            conn = kwargs.get(inject_param_name)
            if isinstance(conn, sa.Connection):
                if conn.in_transaction():
                    return func(*args, **kwargs)
                else:
                    with conn.begin():
                        return func(*args, **kwargs)
            else:
                with engine.begin() as conn:
                    kwargs[inject_param_name] = conn
                    return func(*args, **kwargs)

        return sync_wrapper


def connect(func):
    """A decorator that provides a connection context.

    If the decorated function is called with a connection object, that
    connection is used. Otherwise, a new connection is created from the
    engine, and the new connection is injected to the function.

    Example: ::
        @connect
        def get_user_count(conn: sa.Connection = Inject(engine)) -> int:
            return conn.execute(...).scalar_one()

        # This will create a new connection
        count = get_user_count()

        # This will use the existing connection
        with engine.connect() as conn:
            count = get_user_count(conn)
    """

    # Find the parameter with value Inject
    sig = inspect.signature(func)
    inject_param_name = None
    for name, param in sig.parameters.items():
        if param.default is not inspect.Parameter.empty and isinstance(
            param.default, _Injectable
        ):
            inject_param_name = name
            break
    if inject_param_name is None:
        return func  # No injection needed

    engine = sig.parameters[inject_param_name].default.engine
    is_async = isinstance(engine, AsyncEngine)

    if is_async:

        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            conn = kwargs.get(inject_param_name)
            if isinstance(conn, AsyncConnection):
                return await func(*args, **kwargs)
            else:
                async with engine.connect() as conn:
                    kwargs[inject_param_name] = conn
                    return await func(*args, **kwargs)

        return async_wrapper

    else:

        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            conn = kwargs.get(inject_param_name)
            if isinstance(conn, sa.Connection):
                return func(*args, **kwargs)
            else:
                with engine.connect() as conn:
                    kwargs[inject_param_name] = conn
                    return func(*args, **kwargs)

        return sync_wrapper
