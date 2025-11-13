"""Tests for custom error classes and error scenarios."""

import pytest
import pytest_asyncio
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import create_async_engine

from sqla_fancy_core import TableBuilder, fancy
from sqla_fancy_core.decorators import Inject, connect
from sqla_fancy_core.errors import (
    AtomicContextError,
    NotInTransactionError,
    UnexpectedAsyncConnectionError,
    UnsupportedEngineTypeError,
)

tb = TableBuilder()


class Counter:
    id = tb.auto_id()
    Table = tb("counter")


q_insert = sa.insert(Counter.Table)
q_count = sa.select(sa.func.count()).select_from(Counter.Table)


@pytest.fixture
def sync_engine():
    """Provides a synchronous in-memory SQLite engine."""
    engine = sa.create_engine("sqlite:///:memory:")
    tb.metadata.create_all(engine)
    yield engine
    tb.metadata.drop_all(engine)
    engine.dispose()


@pytest_asyncio.fixture
async def async_engine():
    """Provides an asynchronous in-memory SQLite engine."""
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(tb.metadata.create_all)
    yield engine
    async with engine.begin() as conn:
        await conn.run_sync(tb.metadata.drop_all)
    await engine.dispose()


# AtomicContextError tests
def test_ax_raises_atomic_context_error_when_outside_atomic(sync_engine):
    """Test that ax() raises AtomicContextError when called outside atomic context."""
    fancy_engine = fancy(sync_engine)
    with pytest.raises(
        AtomicContextError,
        match="ax\\(\\) must be called within the atomic\\(\\) context manager",
    ):
        fancy_engine.ax(q_insert)


@pytest.mark.asyncio
async def test_async_ax_raises_atomic_context_error_when_outside_atomic(async_engine):
    """Test that async ax() raises AtomicContextError when called outside atomic context."""
    fancy_engine = fancy(async_engine)
    with pytest.raises(
        AtomicContextError,
        match="ax\\(\\) must be called within the atomic\\(\\) context manager",
    ):
        await fancy_engine.ax(q_insert)


# NotInTransactionError tests
def test_tx_raises_not_in_transaction_error(sync_engine):
    """Test that tx() raises NotInTransactionError when connection is not in a transaction."""
    fancy_engine = fancy(sync_engine)
    with sync_engine.connect() as conn:
        assert not conn.in_transaction()
        with pytest.raises(
            NotInTransactionError,
            match="tx\\(\\) requires the connection to be in an active transaction",
        ):
            fancy_engine.tx(conn, q_insert)


@pytest.mark.asyncio
async def test_async_tx_raises_not_in_transaction_error(async_engine):
    """Test that async tx() raises NotInTransactionError when connection is not in a transaction."""
    fancy_engine = fancy(async_engine)
    async with async_engine.connect() as conn:
        assert not conn.in_transaction()
        with pytest.raises(
            NotInTransactionError,
            match="tx\\(\\) requires the connection to be in an active transaction",
        ):
            await fancy_engine.tx(conn, q_insert)


# UnexpectedAsyncConnectionError tests
@pytest.mark.asyncio
async def test_sync_function_rejects_async_connection_with_connect_decorator(
    sync_engine, async_engine
):
    """Test that @connect decorated sync function rejects async connection."""
    fancy_engine = fancy(sync_engine)

    @connect
    def get_count(conn: sa.Connection = Inject(fancy_engine)) -> int:
        return conn.execute(q_count).scalar_one()

    # Try to pass an async connection to a sync function
    async with async_engine.connect() as async_conn:
        with pytest.raises(
            UnexpectedAsyncConnectionError,
            match="An async connection was provided in a synchronous context",
        ):
            get_count(conn=async_conn)


# UnsupportedEngineTypeError tests
def test_inject_with_invalid_engine_type():
    """Test that Inject with unsupported engine type raises error on decoration."""

    class UnsupportedEngine:
        pass

    unsupported = UnsupportedEngine()

    with pytest.raises(
        UnsupportedEngineTypeError,
        match="Unsupported engine type provided to the decorator",
    ):

        @connect
        def test_func(conn=Inject(unsupported)):  # type: ignore
            pass

        test_func()


# Error inheritance tests
def test_fancy_error_is_base_exception():
    """Test that all custom errors inherit from Exception via FancyError."""
    from sqla_fancy_core.errors import FancyError

    assert issubclass(AtomicContextError, FancyError)
    assert issubclass(NotInTransactionError, FancyError)
    assert issubclass(UnexpectedAsyncConnectionError, FancyError)
    assert issubclass(UnsupportedEngineTypeError, FancyError)


def test_type_errors_inherit_from_typeerror():
    """Test that type-related errors also inherit from TypeError."""
    assert issubclass(UnexpectedAsyncConnectionError, TypeError)
    assert issubclass(UnsupportedEngineTypeError, TypeError)


def test_error_messages():
    """Test that error messages are descriptive."""
    assert (
        str(AtomicContextError())
        == "ax() must be called within the atomic() context manager"
    )
    assert (
        str(NotInTransactionError())
        == "tx() requires the connection to be in an active transaction"
    )
    assert (
        str(UnexpectedAsyncConnectionError())
        == "An async connection was provided in a synchronous context"
    )
    assert (
        str(UnsupportedEngineTypeError())
        == "Unsupported engine type provided to the decorator"
    )
