import pytest
import pytest_asyncio
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import create_async_engine

from sqla_fancy_core import TableBuilder, fancy

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
async def test_insert(fancy_engine):
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
