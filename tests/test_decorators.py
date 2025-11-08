"""Tests for decorator functionality with fancy wrappers and edge cases."""

import pytest
import pytest_asyncio
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncConnection, create_async_engine

from sqla_fancy_core.decorators import Inject, connect, transact
from sqla_fancy_core.wrappers import fancy

# Define a simple table for testing
metadata = sa.MetaData()
users = sa.Table(
    "users",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
    sa.Column("name", sa.String),
)


@pytest.fixture
def sync_engine():
    """Provides a synchronous in-memory SQLite engine."""
    engine = sa.create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=sa.pool.StaticPool,
    )
    metadata.create_all(engine)
    yield engine
    metadata.drop_all(engine)
    engine.dispose()


@pytest_asyncio.fixture
async def async_engine():
    """Provides an asynchronous in-memory SQLite engine."""
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(metadata.create_all)
    yield engine
    async with engine.begin() as conn:
        await conn.run_sync(metadata.drop_all)
    await engine.dispose()


# Tests for @connect with FancyEngineWrapper
def test_connect_with_fancy_wrapper(sync_engine):
    """Test @connect decorator with FancyEngineWrapper."""
    fancy_engine = fancy(sync_engine)

    @connect
    def get_user_count(conn=Inject(fancy_engine)):
        return conn.execute(sa.select(sa.func.count()).select_from(users)).scalar_one()

    # Should work without explicit connection
    assert get_user_count() == 0

    # Should work with explicit connection
    with sync_engine.connect() as conn:
        assert get_user_count(conn=conn) == 0


def test_connect_with_fancy_wrapper_and_data(sync_engine):
    """Test @connect decorator with FancyEngineWrapper after inserting data."""
    fancy_engine = fancy(sync_engine)

    @connect
    def get_user_count(conn=Inject(fancy_engine)):
        return conn.execute(sa.select(sa.func.count()).select_from(users)).scalar_one()

    # Insert some data
    with sync_engine.begin() as conn:
        conn.execute(sa.insert(users).values(name="Alice"))
        conn.execute(sa.insert(users).values(name="Bob"))

    assert get_user_count() == 2


# Tests for @transact with FancyEngineWrapper
def test_transact_with_fancy_wrapper(sync_engine):
    """Test @transact decorator with FancyEngineWrapper."""
    fancy_engine = fancy(sync_engine)

    @transact
    def create_user(name: str, conn=Inject(fancy_engine)):
        conn.execute(sa.insert(users).values(name=name))

    @connect
    def get_user_count(conn=Inject(fancy_engine)):
        return conn.execute(sa.select(sa.func.count()).select_from(users)).scalar_one()

    # Create user without explicit transaction
    create_user("Alice")
    assert get_user_count() == 1

    # Create user with explicit transaction
    with sync_engine.begin() as conn:
        create_user("Bob", conn=conn)

    assert get_user_count() == 2


def test_transact_with_fancy_wrapper_rollback(sync_engine):
    """Test @transact decorator with FancyEngineWrapper and rollback."""
    fancy_engine = fancy(sync_engine)

    @transact
    def create_user(name: str, conn=Inject(fancy_engine)):
        conn.execute(sa.insert(users).values(name=name))

    @connect
    def get_user_count(conn=Inject(fancy_engine)):
        return conn.execute(sa.select(sa.func.count()).select_from(users)).scalar_one()

    # Test that rollback works
    try:
        with sync_engine.begin() as conn:
            create_user("Alice", conn=conn)
            raise ValueError("Test rollback")
    except ValueError:
        pass

    # User should not be created due to rollback
    assert get_user_count() == 0


def test_transact_reuses_existing_transaction(sync_engine):
    """Test that @transact reuses an existing transaction."""
    fancy_engine = fancy(sync_engine)

    call_count = {"begin": 0}

    @transact
    def create_user(name: str, conn=Inject(fancy_engine)):
        conn.execute(sa.insert(users).values(name=name))

    @connect
    def get_user_count(conn=Inject(fancy_engine)):
        return conn.execute(sa.select(sa.func.count()).select_from(users)).scalar_one()

    # When called with a connection already in a transaction,
    # it should not start a new transaction
    with sync_engine.begin() as conn:
        assert conn.in_transaction()
        create_user("Alice", conn=conn)
        create_user("Bob", conn=conn)

    assert get_user_count() == 2


def test_transact_starts_transaction_for_connection_without_transaction(sync_engine):
    """Test that @transact starts a transaction if connection is not in one."""
    fancy_engine = fancy(sync_engine)

    @transact
    def create_user(name: str, conn=Inject(fancy_engine)):
        assert conn.in_transaction(), "Connection should be in a transaction"
        conn.execute(sa.insert(users).values(name=name))

    @connect
    def get_user_count(conn=Inject(fancy_engine)):
        return conn.execute(sa.select(sa.func.count()).select_from(users)).scalar_one()

    # Pass a connection that is NOT in a transaction
    with sync_engine.connect() as conn:
        assert not conn.in_transaction()
        create_user("Alice", conn=conn)

    assert get_user_count() == 1


# Tests for async @connect with AsyncFancyEngineWrapper
@pytest.mark.asyncio
async def test_async_connect_with_fancy_wrapper(async_engine):
    """Test async @connect decorator with AsyncFancyEngineWrapper."""
    fancy_engine = fancy(async_engine)

    @connect
    async def get_user_count(conn=Inject(fancy_engine)):
        result = await conn.execute(sa.select(sa.func.count()).select_from(users))
        return result.scalar_one()

    # Should work without explicit connection
    assert await get_user_count() == 0

    # Should work with explicit connection
    async with async_engine.connect() as conn:
        assert await get_user_count(conn=conn) == 0


@pytest.mark.asyncio
async def test_async_connect_with_fancy_wrapper_and_data(async_engine):
    """Test async @connect decorator with AsyncFancyEngineWrapper after inserting data."""
    fancy_engine = fancy(async_engine)

    @connect
    async def get_user_count(conn=Inject(fancy_engine)):
        result = await conn.execute(sa.select(sa.func.count()).select_from(users))
        return result.scalar_one()

    # Insert some data
    async with async_engine.begin() as conn:
        await conn.execute(sa.insert(users).values(name="Alice"))
        await conn.execute(sa.insert(users).values(name="Bob"))

    assert await get_user_count() == 2


# Tests for async @transact with AsyncFancyEngineWrapper
@pytest.mark.asyncio
async def test_async_transact_with_fancy_wrapper(async_engine):
    """Test async @transact decorator with AsyncFancyEngineWrapper."""
    fancy_engine = fancy(async_engine)

    @transact
    async def create_user(name: str, conn=Inject(fancy_engine)):
        await conn.execute(sa.insert(users).values(name=name))

    @connect
    async def get_user_count(conn=Inject(fancy_engine)):
        result = await conn.execute(sa.select(sa.func.count()).select_from(users))
        return result.scalar_one()

    # Create user without explicit transaction
    await create_user("Alice")
    assert await get_user_count() == 1

    # Create user with explicit transaction
    async with async_engine.begin() as conn:
        await create_user("Bob", conn=conn)

    assert await get_user_count() == 2


@pytest.mark.asyncio
async def test_async_transact_with_fancy_wrapper_rollback(async_engine):
    """Test async @transact decorator with AsyncFancyEngineWrapper and rollback."""
    fancy_engine = fancy(async_engine)

    @transact
    async def create_user(name: str, conn=Inject(fancy_engine)):
        await conn.execute(sa.insert(users).values(name=name))

    @connect
    async def get_user_count(conn=Inject(fancy_engine)):
        result = await conn.execute(sa.select(sa.func.count()).select_from(users))
        return result.scalar_one()

    # Test that rollback works
    try:
        async with async_engine.begin() as conn:
            await create_user("Alice", conn=conn)
            raise ValueError("Test rollback")
    except ValueError:
        pass

    # User should not be created due to rollback
    assert await get_user_count() == 0


@pytest.mark.asyncio
async def test_async_transact_reuses_existing_transaction(async_engine):
    """Test that async @transact reuses an existing transaction."""
    fancy_engine = fancy(async_engine)

    @transact
    async def create_user(name: str, conn=Inject(fancy_engine)):
        await conn.execute(sa.insert(users).values(name=name))

    @connect
    async def get_user_count(conn=Inject(fancy_engine)):
        result = await conn.execute(sa.select(sa.func.count()).select_from(users))
        return result.scalar_one()

    # When called with a connection already in a transaction,
    # it should not start a new transaction
    async with async_engine.begin() as conn:
        assert conn.in_transaction()
        await create_user("Alice", conn=conn)
        await create_user("Bob", conn=conn)

    assert await get_user_count() == 2


@pytest.mark.asyncio
async def test_async_transact_starts_transaction_for_connection_without_transaction(
    async_engine,
):
    """Test that async @transact starts a transaction if connection is not in one."""
    fancy_engine = fancy(async_engine)

    @transact
    async def create_user(name: str, conn=Inject(fancy_engine)):
        assert conn.in_transaction(), "Connection should be in a transaction"
        await conn.execute(sa.insert(users).values(name=name))

    @connect
    async def get_user_count(conn=Inject(fancy_engine)):
        result = await conn.execute(sa.select(sa.func.count()).select_from(users))
        return result.scalar_one()

    # Pass a connection that is NOT in a transaction
    async with async_engine.connect() as conn:
        assert not conn.in_transaction()
        await create_user("Alice", conn=conn)

    assert await get_user_count() == 1


# Error handling tests
@pytest.mark.asyncio
async def test_sync_decorator_rejects_async_connection(sync_engine, async_engine):
    """Test that sync decorator properly rejects AsyncConnection."""
    fancy_engine = fancy(sync_engine)

    @connect
    def get_user_count(conn=Inject(fancy_engine)):
        return conn.execute(sa.select(sa.func.count()).select_from(users)).scalar_one()

    # Try to pass an actual AsyncConnection to a sync function
    async with async_engine.connect() as async_conn:
        with pytest.raises(TypeError, match="AsyncConnection cannot be used in sync function"):
            get_user_count(conn=async_conn)


@pytest.mark.asyncio
async def test_sync_transact_rejects_async_connection(sync_engine, async_engine):
    """Test that sync @transact properly rejects AsyncConnection."""
    fancy_engine = fancy(sync_engine)

    @transact
    def create_user(name: str, conn=Inject(fancy_engine)):
        conn.execute(sa.insert(users).values(name=name))

    # Try to pass an actual AsyncConnection to a sync function
    async with async_engine.connect() as async_conn:
        with pytest.raises(TypeError, match="AsyncConnection cannot be used in sync function"):
            create_user("Alice", conn=async_conn)


def test_inject_with_unsupported_engine_type():
    """Test that decorators reject unsupported engine types."""

    class UnsupportedEngine:
        pass

    unsupported = UnsupportedEngine()

    with pytest.raises(TypeError, match="Unsupported engine type"):

        @connect
        def test_func(conn=Inject(unsupported)):  # type: ignore
            pass

        test_func()


def test_transact_with_unsupported_engine_type():
    """Test that @transact rejects unsupported engine types."""

    class UnsupportedEngine:
        pass

    unsupported = UnsupportedEngine()

    with pytest.raises(TypeError, match="Unsupported engine type"):

        @transact
        def test_func(conn=Inject(unsupported)):  # type: ignore
            pass

        test_func()


# Test decorator without Inject parameter
def test_connect_without_inject_parameter():
    """Test that @connect works on functions without Inject parameter."""

    @connect
    def simple_function():
        return "hello"

    # Should just return the function as-is without wrapping
    assert simple_function() == "hello"


def test_transact_without_inject_parameter():
    """Test that @transact works on functions without Inject parameter."""

    @transact
    def simple_function():
        return "hello"

    # Should just return the function as-is without wrapping
    assert simple_function() == "hello"


# Integration test combining both decorators
def test_integration_connect_and_transact(sync_engine):
    """Test integration of @connect and @transact decorators."""
    fancy_engine = fancy(sync_engine)

    @transact
    def create_user(name: str, conn=Inject(fancy_engine)):
        conn.execute(sa.insert(users).values(name=name))

    @connect
    def get_users(conn=Inject(fancy_engine)):
        result = conn.execute(sa.select(users.c.name).order_by(users.c.name))
        return [row[0] for row in result]

    @transact
    def create_multiple_users(names: list[str], conn=Inject(fancy_engine)):
        for name in names:
            create_user(name, conn=conn)

    # Create users using nested decorators
    create_multiple_users(["Alice", "Bob", "Charlie"])

    # Verify all users were created
    assert get_users() == ["Alice", "Bob", "Charlie"]


@pytest.mark.asyncio
async def test_async_integration_connect_and_transact(async_engine):
    """Test async integration of @connect and @transact decorators."""
    fancy_engine = fancy(async_engine)

    @transact
    async def create_user(name: str, conn=Inject(fancy_engine)):
        await conn.execute(sa.insert(users).values(name=name))

    @connect
    async def get_users(conn=Inject(fancy_engine)):
        result = await conn.execute(sa.select(users.c.name).order_by(users.c.name))
        return [row[0] for row in result]

    @transact
    async def create_multiple_users(names: list[str], conn=Inject(fancy_engine)):
        for name in names:
            await create_user(name, conn=conn)

    # Create users using nested decorators
    await create_multiple_users(["Alice", "Bob", "Charlie"])

    # Verify all users were created
    assert await get_users() == ["Alice", "Bob", "Charlie"]
