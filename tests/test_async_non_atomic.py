"""Tests for async non_atomic() context manager and nax() method."""

import asyncio

import pytest
import pytest_asyncio
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import create_async_engine

from sqla_fancy_core import TableBuilder, fancy
from sqla_fancy_core.errors import AtomicInsideNonAtomicError, NotInTransactionError

tb = TableBuilder()


class Counter:
    id = tb.auto_id()
    Table = tb("counter")


q_insert = sa.insert(Counter.Table).returning(Counter.id)
q_count = sa.select(sa.func.count()).select_from(Counter.Table)


@pytest_asyncio.fixture
async def fancy_engine():
    eng = fancy(
        create_async_engine(
            "sqlite+aiosqlite:///:memory:",
            pool_size=3,
            poolclass=sa.AsyncAdaptedQueuePool,
        )
    )
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
async def test_nax_without_context_creates_new_connection(fancy_engine):
    """Test that nax() creates a new connection when not in non_atomic context."""
    count = await fancy_engine.nax(q_count)
    assert count.scalar_one() == 0
    # Insert without commit should not persist
    await fancy_engine.nax(q_insert)
    # Since we're not in a transaction, changes are auto-committed
    count = await fancy_engine.nax(q_count)
    assert count.scalar_one() == 0


@pytest.mark.asyncio
async def test_nax_inside_non_atomic_reuses_connection(fancy_engine):
    """Test that nax() reuses the same connection within non_atomic context."""
    count = await fancy_engine.x(None, q_count)
    assert count.scalar_one() == 0

    async with fancy_engine.non_atomic() as conn:
        # Multiple nax() calls share the same connection
        await fancy_engine.nax(q_insert)
        await fancy_engine.nax(q_insert)
        # The connection starts an autobegin transaction on first execute in SQLite
        assert conn.in_transaction() is True
        # Within the same connection/transaction, we can see the inserts
        count = await fancy_engine.nax(q_count)
        assert count.scalar_one() == 2

    # No explicit commit, so changes are rolled back
    count = await fancy_engine.x(None, q_count)
    assert count.scalar_one() == 0


@pytest.mark.asyncio
async def test_nested_non_atomic_reuses_same_connection(fancy_engine):
    """Test that nested non_atomic contexts reuse the same connection."""
    async with fancy_engine.non_atomic() as conn1:
        async with fancy_engine.non_atomic() as conn2:
            assert conn1 is conn2
            await fancy_engine.nax(q_insert)
            # Within the same connection, we can see the insert
            count = await fancy_engine.nax(q_count)
            assert count.scalar_one() == 1

    # No explicit commit, so nothing persists
    count = await fancy_engine.x(None, q_count)
    assert count.scalar_one() == 0


@pytest.mark.asyncio
async def test_non_atomic_with_explicit_transaction(fancy_engine):
    """Test that we can start a transaction within non_atomic context."""
    count = await fancy_engine.x(None, q_count)
    assert count.scalar_one() == 0

    async with fancy_engine.non_atomic() as conn:
        async with conn.begin():
            await conn.execute(q_insert)
            count = await conn.execute(q_count)
            assert count.scalar_one() == 1
        # After commit
        count = await conn.execute(q_count)
        assert count.scalar_one() == 1

    count = await fancy_engine.x(None, q_count)
    assert count.scalar_one() == 1


@pytest.mark.asyncio
async def test_x_uses_non_atomic_connection_when_inside(fancy_engine):
    """Test that x(None, ...) reuses the non_atomic connection when inside that context."""
    count = await fancy_engine.x(None, q_count)
    assert count.scalar_one() == 0

    async with fancy_engine.non_atomic() as conn:
        # x(None, ...) should reuse the non_atomic connection
        await fancy_engine.x(None, q_insert)
        # Within the same connection, we can see the insert
        count = await fancy_engine.x(None, q_count)
        assert count.scalar_one() == 1
        # SQLite starts an implicit transaction on first execute
        assert conn.in_transaction() is True

    # No explicit commit, so nothing persists
    count = await fancy_engine.x(None, q_count)
    assert count.scalar_one() == 0


@pytest.mark.asyncio
async def test_tx_raises_error_on_non_transactional_connection(fancy_engine):
    """Test that tx() raises NotInTransactionError when connection is not in transaction."""
    async with fancy_engine.non_atomic() as conn:
        # Execute a query first to trigger autobegin
        await fancy_engine.nax(q_count)
        # Now we're in an implicit transaction
        assert conn.in_transaction() is True
        # Commit to end the transaction
        await conn.commit()
        # Now we're not in a transaction
        assert conn.in_transaction() is False
        # tx() should raise error
        with pytest.raises(NotInTransactionError):
            await fancy_engine.tx(conn, q_insert)


@pytest.mark.asyncio
async def test_tx_works_with_explicit_transaction_in_non_atomic(fancy_engine):
    """Test that tx() works when we explicitly start a transaction in non_atomic."""
    count = await fancy_engine.x(None, q_count)
    assert count.scalar_one() == 0

    async with fancy_engine.non_atomic() as conn:
        async with conn.begin():
            assert conn.in_transaction() is True
            await fancy_engine.tx(conn, q_insert)
            count = await fancy_engine.tx(conn, q_count)
            assert count.scalar_one() == 1
        # After commit
        count = await conn.execute(q_count)
        assert count.scalar_one() == 1

    count = await fancy_engine.x(None, q_count)
    assert count.scalar_one() == 1


@pytest.mark.asyncio
async def test_non_atomic_rollback_reverts_changes(fancy_engine):
    """Test that rollback in non_atomic reverts uncommitted changes."""
    count = await fancy_engine.x(None, q_count)
    assert count.scalar_one() == 0

    async with fancy_engine.non_atomic() as conn:
        await fancy_engine.nax(q_insert)
        count = await fancy_engine.nax(q_count)
        assert count.scalar_one() == 1
        await conn.rollback()  # Rollback the implicit transaction
        # After rollback, count should be 0
        count = await fancy_engine.nax(q_count)
        assert count.scalar_one() == 0

    count = await fancy_engine.x(None, q_count)
    assert count.scalar_one() == 0


@pytest.mark.asyncio
async def test_non_atomic_commit_persists_changes(fancy_engine):
    """Test that commit in non_atomic persists changes."""
    count = await fancy_engine.x(None, q_count)
    assert count.scalar_one() == 0

    async with fancy_engine.non_atomic() as conn:
        await fancy_engine.nax(q_insert)
        await conn.commit()  # Commit the implicit transaction
        # After commit, the insert is visible
        count = await fancy_engine.nax(q_count)
        assert count.scalar_one() == 1

    # After commit, the change persists
    count = await fancy_engine.x(None, q_count)
    assert count.scalar_one() == 1


@pytest.mark.asyncio
async def test_multiple_nax_calls_without_context(fancy_engine):
    """Test that multiple nax() calls outside context each create new connections."""
    count = await fancy_engine.nax(q_count)
    assert count.scalar_one() == 0
    await fancy_engine.nax(q_insert)
    # Without commit, nothing persists
    count = await fancy_engine.nax(q_count)
    assert count.scalar_one() == 0
    await fancy_engine.nax(q_insert)
    count = await fancy_engine.nax(q_count)
    assert count.scalar_one() == 0


@pytest.mark.asyncio
async def test_non_atomic_and_atomic_dont_interfere(fancy_engine):
    """Test that non_atomic and atomic contexts don't interfere with each other."""
    count = await fancy_engine.x(None, q_count)
    assert count.scalar_one() == 0

    # Use atomic to commit one insert
    async with fancy_engine.atomic():
        await fancy_engine.ax(q_insert)

    count = await fancy_engine.x(None, q_count)
    assert count.scalar_one() == 1

    # Use non_atomic without commit - shouldn't persist
    async with fancy_engine.non_atomic():
        await fancy_engine.nax(q_insert)
        count = await fancy_engine.nax(q_count)
        assert count.scalar_one() == 2

    # Only the atomic insert should persist
    count = await fancy_engine.x(None, q_count)
    assert count.scalar_one() == 1


@pytest.mark.asyncio
async def test_deeply_nested_non_atomic(fancy_engine):
    """Test that deeply nested non_atomic contexts all share the same connection."""
    async with fancy_engine.non_atomic() as conn1:
        await fancy_engine.nax(q_insert)
        async with fancy_engine.non_atomic() as conn2:
            assert conn1 is conn2
            await fancy_engine.nax(q_insert)
            async with fancy_engine.non_atomic() as conn3:
                assert conn1 is conn3
                await fancy_engine.nax(q_insert)
                count = await fancy_engine.nax(q_count)
                assert count.scalar_one() == 3

    # Nothing committed
    count = await fancy_engine.x(None, q_count)
    assert count.scalar_one() == 0


@pytest.mark.asyncio
async def test_atomic_non_atomic_run_asynchronously(fancy_engine_postgres):
    """Test that atomic and non_atomic contexts run asynchronously without blocking."""

    async def atomic_task():
        async with fancy_engine_postgres.atomic():
            return (await fancy_engine_postgres.ax(q_insert)).scalar_one()

    async def non_atomic_task():
        async with fancy_engine_postgres.non_atomic():
            return (await fancy_engine_postgres.nax(q_insert)).scalar_one()

    async def fragile_atomic_task():
        try:
            async with fancy_engine_postgres.atomic():
                await fancy_engine_postgres.ax(q_insert)
                raise RuntimeError("boom")
        except RuntimeError:
            pass

    await asyncio.gather(
        non_atomic_task(),
        atomic_task(),
        fragile_atomic_task(),
        non_atomic_task(),
        atomic_task(),
        fragile_atomic_task(),
        non_atomic_task(),
        atomic_task(),
        fragile_atomic_task(),
    )

    assert (await fancy_engine_postgres.x(None, q_count)).scalar_one() == 3


@pytest.mark.asyncio
async def test_atomic_inside_non_atomic_requires_transaction(fancy_engine):
    """Test that atomic() inside non_atomic() requires an active transaction."""
    assert (await fancy_engine.x(None, q_count)).scalar_one() == 0

    # Attempting to use atomic() inside non_atomic() without a transaction should fail
    with pytest.raises(AtomicInsideNonAtomicError):
        async with fancy_engine.non_atomic():
            async with fancy_engine.atomic():
                pass

    # But it works if we start an explicit transaction first
    async with fancy_engine.non_atomic() as conn1:
        async with conn1.begin():
            # Now atomic() should reuse the same connection
            async with fancy_engine.atomic() as conn2:
                assert conn1 is conn2
                assert conn2.in_transaction() is True
                await fancy_engine.ax(q_insert)
                assert (await fancy_engine.ax(q_count)).scalar_one() == 1
            # After atomic exits, still in non_atomic's explicit transaction
            assert conn1.in_transaction() is True

    # The explicit transaction commit should have persisted the change
    assert (await fancy_engine.x(None, q_count)).scalar_one() == 1


@pytest.mark.asyncio
async def test_non_atomic_inside_atomic_reuses_connection(fancy_engine):
    """Test that non_atomic() inside atomic() reuses the same connection."""
    assert (await fancy_engine.x(None, q_count)).scalar_one() == 0

    async with fancy_engine.atomic() as conn1:
        assert conn1.in_transaction() is True
        # Start non_atomic context - should reuse the same connection
        async with fancy_engine.non_atomic() as conn2:
            assert conn1 is conn2
            assert conn2.in_transaction() is True
            await fancy_engine.nax(q_insert)
            assert (await fancy_engine.nax(q_count)).scalar_one() == 1
        # Still in transaction after non_atomic exits
        assert conn1.in_transaction() is True
        await fancy_engine.ax(q_insert)
        assert (await fancy_engine.ax(q_count)).scalar_one() == 2

    # Both inserts should be committed
    assert (await fancy_engine.x(None, q_count)).scalar_one() == 2


@pytest.mark.asyncio
async def test_nax_works_in_atomic_context(fancy_engine):
    """Test that nax() works correctly within atomic() context."""
    assert (await fancy_engine.x(None, q_count)).scalar_one() == 0

    async with fancy_engine.atomic():
        await fancy_engine.nax(q_insert)
        await fancy_engine.ax(q_insert)
        # Both should be visible in the same transaction
        assert (await fancy_engine.nax(q_count)).scalar_one() == 2

    # Both should be committed
    assert (await fancy_engine.x(None, q_count)).scalar_one() == 2


@pytest.mark.asyncio
async def test_ax_fails_in_non_atomic_without_explicit_transaction(fancy_engine):
    """Test that ax() raises error in non_atomic() without explicit transaction started."""
    from sqla_fancy_core.errors import AtomicContextError

    # ax() outside any context should raise AtomicContextError
    with pytest.raises(AtomicContextError):
        await fancy_engine.ax(q_insert)

    # ax() in non_atomic without transaction raises NotInTransactionError
    # because the connection exists but is not in a transaction
    with pytest.raises(NotInTransactionError):
        async with fancy_engine.non_atomic():
            await fancy_engine.ax(q_insert)


@pytest.mark.asyncio
async def test_mixed_nesting_atomic_non_atomic_atomic(fancy_engine):
    """Test complex nesting: atomic -> non_atomic -> atomic."""
    assert (await fancy_engine.x(None, q_count)).scalar_one() == 0

    async with fancy_engine.atomic() as conn1:
        await fancy_engine.ax(q_insert)
        assert (await fancy_engine.ax(q_count)).scalar_one() == 1

        async with fancy_engine.non_atomic() as conn2:
            assert conn1 is conn2
            await fancy_engine.nax(q_insert)
            assert (await fancy_engine.nax(q_count)).scalar_one() == 2

            async with fancy_engine.atomic() as conn3:
                assert conn1 is conn3
                await fancy_engine.ax(q_insert)
                assert (await fancy_engine.ax(q_count)).scalar_one() == 3

            assert (await fancy_engine.nax(q_count)).scalar_one() == 3

        assert (await fancy_engine.ax(q_count)).scalar_one() == 3

    # All should be committed
    assert (await fancy_engine.x(None, q_count)).scalar_one() == 3


@pytest.mark.asyncio
async def test_rollback_in_non_atomic_inside_atomic_affects_all(fancy_engine):
    """Test that rollback in nested non_atomic affects the outer atomic transaction."""
    assert (await fancy_engine.x(None, q_count)).scalar_one() == 0

    async with fancy_engine.atomic() as conn1:
        await fancy_engine.ax(q_insert)
        assert (await fancy_engine.ax(q_count)).scalar_one() == 1

        async with fancy_engine.non_atomic() as conn2:
            assert conn1 is conn2
            await fancy_engine.nax(q_insert)
            assert (await fancy_engine.nax(q_count)).scalar_one() == 2
            # Rollback should affect the entire transaction
            await conn2.rollback()
            # After rollback, connection is not in transaction
            assert conn2.in_transaction() is False

        # After rollback, ax should fail because not in transaction
        with pytest.raises(NotInTransactionError):
            await fancy_engine.ax(q_insert)

    # Nothing should be committed due to rollback
    assert (await fancy_engine.x(None, q_count)).scalar_one() == 0


@pytest.mark.asyncio
async def test_commit_in_non_atomic_inside_atomic_commits_early(fancy_engine):
    """Test that commit in nested non_atomic commits the outer atomic transaction."""
    assert (await fancy_engine.x(None, q_count)).scalar_one() == 0

    async with fancy_engine.atomic() as conn1:
        await fancy_engine.ax(q_insert)
        assert (await fancy_engine.ax(q_count)).scalar_one() == 1

        async with fancy_engine.non_atomic() as conn2:
            assert conn1 is conn2
            await fancy_engine.nax(q_insert)
            assert (await fancy_engine.nax(q_count)).scalar_one() == 2
            # Explicit commit
            await conn2.commit()
            # After commit, connection is not in transaction
            assert conn2.in_transaction() is False

        # After commit, ax should fail because not in transaction
        with pytest.raises(NotInTransactionError):
            await fancy_engine.ax(q_insert)

    # The committed changes should persist
    assert (await fancy_engine.x(None, q_count)).scalar_one() == 2
