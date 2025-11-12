import pytest
import pytest_asyncio
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import create_async_engine

from sqla_fancy_core import TableBuilder, fancy
from sqla_fancy_core.wrappers import AtomicContextError

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
