import pytest
import pytest_asyncio
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import create_async_engine

from sqla_fancy_core import TableBuilder, fancy
from sqla_fancy_core.errors import NotInTransactionError

tb = TableBuilder()


class Counter:
    id = tb.auto_id()
    Table = tb("counter")


q_insert = sa.insert(Counter.Table)
q_count = sa.select(sa.func.count()).select_from(Counter.Table)


@pytest_asyncio.fixture
async def fancy_engine():
    fancy_engine = fancy(create_async_engine("sqlite+aiosqlite:///:memory:"))
    async with fancy_engine.engine.begin() as conn:
        await conn.run_sync(tb.metadata.create_all)
    yield fancy_engine
    async with fancy_engine.engine.begin() as conn:
        await conn.run_sync(tb.metadata.drop_all)
    await fancy_engine.engine.dispose()


@pytest.mark.asyncio
async def test_tx_auto_commits_without_explicit_connection(fancy_engine):
    """Test that tx(None, ...) auto-commits when no connection is provided."""
    assert (await fancy_engine.x(None, q_count)).scalar_one() == 0
    await fancy_engine.tx(None, q_insert)
    assert (await fancy_engine.x(None, q_count)).scalar_one() == 1
    assert (await fancy_engine.tx(None, q_count)).scalar_one() == 1


@pytest.mark.asyncio
async def test_no_commit_default_rollback(fancy_engine):
    assert (await fancy_engine.x(None, q_count)).scalar_one() == 0
    await fancy_engine.x(None, q_insert)
    assert (await fancy_engine.x(None, q_count)).scalar_one() == 0

    async with fancy_engine.engine.connect() as conn:
        assert (await fancy_engine.x(conn, q_count)).scalar_one() == 0
        await fancy_engine.x(conn, q_insert)
        assert (await fancy_engine.x(conn, q_count)).scalar_one() == 1
    assert (await fancy_engine.x(None, q_count)).scalar_one() == 0


@pytest.mark.asyncio
async def test_explicit_commit(fancy_engine):
    async with fancy_engine.engine.connect() as conn:
        assert (await fancy_engine.x(conn, q_count)).scalar_one() == 0
        await fancy_engine.x(conn, q_insert)
        assert (await fancy_engine.x(conn, q_count)).scalar_one() == 1
        await conn.commit()
        assert (await fancy_engine.x(conn, q_count)).scalar_one() == 1
    assert (await fancy_engine.x(None, q_count)).scalar_one() == 1


@pytest.mark.asyncio
async def test_explicit_rollback(fancy_engine):
    async with fancy_engine.engine.connect() as conn:
        assert (await fancy_engine.x(conn, q_count)).scalar_one() == 0
        await fancy_engine.x(conn, q_insert)
        assert (await fancy_engine.x(conn, q_count)).scalar_one() == 1
        await conn.rollback()
        assert (await fancy_engine.x(conn, q_count)).scalar_one() == 0
    assert (await fancy_engine.x(None, q_count)).scalar_one() == 0


@pytest.mark.asyncio
async def test_transaction_context_manager_commit(fancy_engine):
    assert (await fancy_engine.x(None, q_count)).scalar_one() == 0
    async with fancy_engine.engine.begin() as txn:
        await fancy_engine.tx(txn, q_insert)
        assert (await fancy_engine.tx(txn, q_count)).scalar_one() == 1
        assert (await fancy_engine.tx(txn, q_count)).scalar_one() == 1
    assert (await fancy_engine.x(None, q_count)).scalar_one() == 1
    assert (await fancy_engine.tx(None, q_count)).scalar_one() == 1


@pytest.mark.asyncio
async def test_transaction_context_manager_rollback(fancy_engine):
    assert (await fancy_engine.x(None, q_count)).scalar_one() == 0
    try:
        async with fancy_engine.engine.begin() as txn:
            assert (await fancy_engine.tx(txn, q_count)).scalar_one() == 0
            await fancy_engine.tx(txn, q_insert)
            assert (await fancy_engine.tx(txn, q_count)).scalar_one() == 1
            raise Exception("Trigger rollback")
    except Exception:
        pass
    assert (await fancy_engine.x(None, q_count)).scalar_one() == 0
    assert (await fancy_engine.tx(None, q_count)).scalar_one() == 0


@pytest.mark.asyncio
async def test_x_with_none_creates_new_connection_each_time(fancy_engine):
    """Test that x(None, ...) creates a new connection for each call."""
    assert (await fancy_engine.x(None, q_count)).scalar_one() == 0
    await fancy_engine.x(None, q_insert)
    # Without commit, nothing should persist
    assert (await fancy_engine.x(None, q_count)).scalar_one() == 0


@pytest.mark.asyncio
async def test_multiple_tx_calls_with_none(fancy_engine):
    """Test that tx(None, ...) auto-commits each time."""
    assert (await fancy_engine.x(None, q_count)).scalar_one() == 0
    await fancy_engine.tx(None, q_insert)
    assert (await fancy_engine.x(None, q_count)).scalar_one() == 1
    await fancy_engine.tx(None, q_insert)
    assert (await fancy_engine.x(None, q_count)).scalar_one() == 2
    await fancy_engine.tx(None, q_insert)
    assert (await fancy_engine.x(None, q_count)).scalar_one() == 3


@pytest.mark.asyncio
async def test_x_and_tx_with_explicit_connection_see_same_state(fancy_engine):
    """Test that x() and tx() with the same connection see the same uncommitted state."""
    async with fancy_engine.engine.begin() as conn:
        await fancy_engine.tx(conn, q_insert)
        # x() with same connection should see uncommitted insert
        assert (await fancy_engine.x(conn, q_count)).scalar_one() == 1
        await fancy_engine.x(conn, q_insert)
        # tx() with same connection should see both inserts
        assert (await fancy_engine.tx(conn, q_count)).scalar_one() == 2

    # Both should be committed
    assert (await fancy_engine.x(None, q_count)).scalar_one() == 2


@pytest.mark.asyncio
async def test_tx_raises_error_on_non_transactional_connection(fancy_engine):
    """Test that tx() raises NotInTransactionError when connection is not in a transaction."""
    async with fancy_engine.engine.connect() as conn:
        # Connection exists but is not in a transaction
        assert conn.in_transaction() is False
        with pytest.raises(NotInTransactionError):
            await fancy_engine.tx(conn, q_insert)
