import pytest
import pytest_asyncio
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import create_async_engine

from sqla_fancy_core import TableBuilder, fancy
from sqla_fancy_core.errors import AtomicContextError, NotInTransactionError

tb = TableBuilder()


class Counter:
    id = tb.auto_id()
    Table = tb("counter")


q_insert = sa.insert(Counter.Table)
q_count = sa.select(sa.func.count()).select_from(Counter.Table)


@pytest_asyncio.fixture
async def fancy_engine():
    eng = fancy(create_async_engine("sqlite+aiosqlite:///:memory:"))
    async with eng.engine.begin() as conn:
        await conn.run_sync(tb.metadata.create_all)
    try:
        yield eng
    finally:
        async with eng.engine.begin() as conn:
            await conn.run_sync(tb.metadata.drop_all)
        await eng.engine.dispose()


@pytest_asyncio.fixture
async def fancy_engine_postgres():
    eng = fancy(create_async_engine("postgresql+asyncpg://test:test@localhost/test"))
    async with eng.engine.begin() as conn:
        await conn.run_sync(tb.metadata.create_all)
    try:
        yield eng
    finally:
        async with eng.engine.begin() as conn:
            await conn.run_sync(tb.metadata.drop_all)
        await eng.engine.dispose()


@pytest.mark.asyncio
async def test_ax_raises_outside_atomic(fancy_engine):
    with pytest.raises(AtomicContextError):
        await fancy_engine.ax(q_count)


@pytest.mark.asyncio
async def test_ax_inside_atomic_commits_on_exit(fancy_engine):
    assert (await fancy_engine.x(None, q_count)).scalar_one() == 0
    async with fancy_engine.atomic() as conn:
        await fancy_engine.ax(q_insert)
        await fancy_engine.ax(q_insert)
        assert (await fancy_engine.ax(q_count)).scalar_one() == 2
        assert conn.in_transaction() is True
    assert (await fancy_engine.x(None, q_count)).scalar_one() == 2


@pytest.mark.asyncio
async def test_nested_atomic_reuses_same_connection(fancy_engine):
    async with fancy_engine.atomic() as conn1:
        async with fancy_engine.atomic() as conn2:
            assert conn1 is conn2
            await fancy_engine.ax(q_insert)
            assert (await fancy_engine.ax(q_count)).scalar_one() == 1
    assert (await fancy_engine.x(None, q_count)).scalar_one() == 1


@pytest.mark.asyncio
async def test_tx_uses_atomic_connection_when_inside(fancy_engine):
    assert (await fancy_engine.x(None, q_count)).scalar_one() == 0
    async with fancy_engine.atomic() as conn:
        await fancy_engine.tx(None, q_insert)
        assert (await fancy_engine.tx(conn, q_count)).scalar_one() == 1
        assert conn.in_transaction() is True
    assert (await fancy_engine.x(None, q_count)).scalar_one() == 1


@pytest.mark.asyncio
async def test_atomic_rollback_on_exception(fancy_engine):
    assert (await fancy_engine.x(None, q_count)).scalar_one() == 0
    with pytest.raises(RuntimeError):
        async with fancy_engine.atomic():
            await fancy_engine.ax(q_insert)
            assert (await fancy_engine.ax(q_count)).scalar_one() == 1
            raise RuntimeError("boom")
    assert (await fancy_engine.x(None, q_count)).scalar_one() == 0


@pytest.mark.asyncio
async def test_atx_outside_atomic_commits(fancy_engine):
    assert (await fancy_engine.x(None, q_count)).scalar_one() == 0
    await fancy_engine.atx(q_insert)
    assert (await fancy_engine.x(None, q_count)).scalar_one() == 1


@pytest.mark.asyncio
async def test_atx_inside_atomic_reuses_same_connection(fancy_engine):
    async with fancy_engine.atomic() as conn:
        await fancy_engine.atx(q_insert)
        assert (await fancy_engine.ax(q_count)).scalar_one() == 1
        assert conn.in_transaction() is True
    assert (await fancy_engine.x(None, q_count)).scalar_one() == 1


@pytest.mark.asyncio
async def test_multiple_atx_calls_outside_atomic(fancy_engine):
    """Test that multiple atx() calls outside atomic each create and commit their own transactions."""
    assert (await fancy_engine.x(None, q_count)).scalar_one() == 0
    await fancy_engine.atx(q_insert)
    assert (await fancy_engine.x(None, q_count)).scalar_one() == 1
    await fancy_engine.atx(q_insert)
    assert (await fancy_engine.x(None, q_count)).scalar_one() == 2
    await fancy_engine.atx(q_insert)
    assert (await fancy_engine.x(None, q_count)).scalar_one() == 3


@pytest.mark.asyncio
async def test_atomic_isolation_from_other_connections(fancy_engine_postgres):
    """Test that changes inside atomic are not visible to other connections until committed."""
    assert (await fancy_engine_postgres.x(None, q_count)).scalar_one() == 0

    async with fancy_engine_postgres.atomic():
        await fancy_engine_postgres.ax(q_insert)
        assert (await fancy_engine_postgres.ax(q_count)).scalar_one() == 1
        # A new connection outside the atomic context shouldn't see the uncommitted insert
        # Create an explicit new connection to test isolation
        async with fancy_engine_postgres.engine.connect() as new_conn:
            assert (await fancy_engine_postgres.x(new_conn, q_count)).scalar_one() == 0

    # After commit, new connections should see it
    assert (await fancy_engine_postgres.x(None, q_count)).scalar_one() == 1


@pytest.mark.asyncio
async def test_nested_atomic_commits_at_outermost_level(fancy_engine):
    """Test that nested atomic contexts only commit when the outermost context exits."""
    assert (await fancy_engine.x(None, q_count)).scalar_one() == 0

    async with fancy_engine.atomic():
        await fancy_engine.ax(q_insert)
        async with fancy_engine.atomic():
            await fancy_engine.ax(q_insert)
            assert (await fancy_engine.ax(q_count)).scalar_one() == 2
            # Still in transaction
        # Inner context exited, but still in outer transaction
        await fancy_engine.ax(q_insert)
        assert (await fancy_engine.ax(q_count)).scalar_one() == 3

    # Now committed
    assert (await fancy_engine.x(None, q_count)).scalar_one() == 3


@pytest.mark.asyncio
async def test_atomic_with_explicit_rollback_raises_exception(fancy_engine):
    """Test that explicitly calling rollback in atomic context still allows exception to propagate."""
    assert (await fancy_engine.x(None, q_count)).scalar_one() == 0

    with pytest.raises(RuntimeError):
        async with fancy_engine.atomic() as conn:
            await fancy_engine.ax(q_insert)
            await conn.rollback()
            raise RuntimeError("explicit rollback then error")

    assert (await fancy_engine.x(None, q_count)).scalar_one() == 0


@pytest.mark.asyncio
async def test_ax_raises_not_in_transaction_after_commit(fancy_engine):
    """Test that ax() raises NotInTransactionError when connection is not in transaction."""
    assert (await fancy_engine.x(None, q_count)).scalar_one() == 0

    async with fancy_engine.atomic() as conn:
        await fancy_engine.ax(q_insert)
        assert (await fancy_engine.ax(q_count)).scalar_one() == 1
        # Manually commit the transaction
        await conn.commit()
        # Now ax() should raise NotInTransactionError
        with pytest.raises(NotInTransactionError):
            await fancy_engine.ax(q_insert)


@pytest.mark.asyncio
async def test_atx_raises_not_in_transaction_after_commit(fancy_engine):
    """Test that atx() raises NotInTransactionError when connection is not in transaction."""
    assert (await fancy_engine.x(None, q_count)).scalar_one() == 0

    async with fancy_engine.atomic() as conn:
        await fancy_engine.atx(q_insert)
        assert (await fancy_engine.atx(q_count)).scalar_one() == 1
        # Manually commit the transaction
        await conn.commit()
        # Now atx() should raise NotInTransactionError
        with pytest.raises(NotInTransactionError):
            await fancy_engine.atx(q_insert)


@pytest.mark.asyncio
async def test_tx_raises_not_in_transaction_with_committed_connection(fancy_engine):
    """Test that tx() raises NotInTransactionError when passed a non-transactional connection."""
    # Create a connection without a transaction
    async with fancy_engine.engine.connect() as conn:
        # Connection exists but is not in a transaction
        with pytest.raises(NotInTransactionError):
            await fancy_engine.tx(conn, q_insert)


@pytest.mark.asyncio
async def test_ax_works_after_nested_atomic_with_same_connection(fancy_engine):
    """Test that ax() continues to work in nested atomic contexts using the same connection."""
    assert (await fancy_engine.x(None, q_count)).scalar_one() == 0

    async with fancy_engine.atomic() as conn1:
        await fancy_engine.ax(q_insert)
        assert conn1.in_transaction() is True
        
        async with fancy_engine.atomic() as conn2:
            # Same connection should be reused
            assert conn1 is conn2
            assert conn2.in_transaction() is True
            await fancy_engine.ax(q_insert)
            assert (await fancy_engine.ax(q_count)).scalar_one() == 2
        
        # Still in transaction after nested context
        assert conn1.in_transaction() is True
        await fancy_engine.ax(q_insert)
        assert (await fancy_engine.ax(q_count)).scalar_one() == 3

    # All committed
    assert (await fancy_engine.x(None, q_count)).scalar_one() == 3


@pytest.mark.asyncio
async def test_atomic_rejects_ax_after_rollback(fancy_engine):
    """Test that ax() raises NotInTransactionError after explicit rollback."""
    assert (await fancy_engine.x(None, q_count)).scalar_one() == 0

    async with fancy_engine.atomic() as conn:
        await fancy_engine.ax(q_insert)
        assert (await fancy_engine.ax(q_count)).scalar_one() == 1
        # Explicitly rollback
        await conn.rollback()
        # Connection is no longer in transaction
        with pytest.raises(NotInTransactionError):
            await fancy_engine.ax(q_insert)
