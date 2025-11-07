import pytest
import pytest_asyncio
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncConnection, create_async_engine

from sqla_fancy_core.decorators import transact

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
    engine = sa.create_engine("sqlite:///:memory:")
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


def test_decorator_sync_commit(sync_engine):
    """Test that the sync decorator commits a successful transaction."""

    @transact(sync_engine)
    def create_user(conn: sa.Connection, name: str):
        assert isinstance(conn, sa.Connection)
        conn.execute(sa.insert(users).values(name=name))

    create_user(name="testuser")  # type: ignore

    with sync_engine.begin() as conn:
        count = conn.execute(sa.select(sa.func.count()).select_from(users)).scalar_one()
        assert count == 1
        name = conn.execute(sa.select(users.c.name)).scalar_one()
        assert name == "testuser"

        create_user(conn, name="anotheruser")

        count = conn.execute(sa.select(sa.func.count()).select_from(users)).scalar_one()
        assert count == 2
        name = conn.execute(sa.select(users.c.name).where(users.c.id == 2)).scalar_one()
        assert name == "anotheruser"

    with sync_engine.begin() as conn:
        count = conn.execute(sa.select(sa.func.count()).select_from(users)).scalar_one()
        assert count == 2
        name = conn.execute(sa.select(users.c.name).where(users.c.id == 1)).scalar_one()
        assert name == "testuser"
        name = conn.execute(sa.select(users.c.name).where(users.c.id == 2)).scalar_one()
        assert name == "anotheruser"


def test_decorator_sync_rollback(sync_engine):
    """Test that the sync decorator rolls back a failed transaction."""

    @transact(sync_engine)
    def create_user_and_fail(conn: sa.Connection, name: str):
        conn.execute(sa.insert(users).values(name=name))
        raise ValueError("Triggering rollback")

    with pytest.raises(ValueError, match="Triggering rollback"):
        create_user_and_fail(name="testuser")  # type: ignore

    with sync_engine.begin() as conn:
        count = conn.execute(sa.select(sa.func.count()).select_from(users)).scalar_one()
        assert count == 0


#
@pytest.mark.asyncio
async def test_decorator_async_commit(async_engine):
    """Test that the async decorator commits a successful transaction."""

    @transact(async_engine)
    async def create_user(name: str, conn: AsyncConnection):
        assert isinstance(conn, AsyncConnection)
        await conn.execute(sa.insert(users).values(name=name))

    await create_user(name="testuser")  # type: ignore

    async with async_engine.begin() as conn:
        count = await conn.execute(sa.select(sa.func.count()).select_from(users))
        count = count.scalar_one()
        assert count == 1
        name = await conn.execute(sa.select(users.c.name))
        name = name.scalar_one()
        assert name == "testuser"

        await create_user(name="anotheruser", conn=conn)

        count = await conn.execute(sa.select(sa.func.count()).select_from(users))
        count = count.scalar_one()
        assert count == 2
        name = await conn.execute(sa.select(users.c.name).where(users.c.id == 2))
        name = name.scalar_one()
        assert name == "anotheruser"

    async with async_engine.begin() as conn:
        count = await conn.execute(sa.select(sa.func.count()).select_from(users))
        count = count.scalar_one()
        assert count == 2
        name = await conn.execute(sa.select(users.c.name).where(users.c.id == 1))
        name = name.scalar_one()
        assert name == "testuser"
        name = await conn.execute(sa.select(users.c.name).where(users.c.id == 2))
        name = name.scalar_one()
        assert name == "anotheruser"


@pytest.mark.asyncio
async def test_decorator_async_rollback(async_engine):
    """Test that the async decorator rolls back a failed transaction."""

    @transact(async_engine)
    async def create_user_and_fail(conn: AsyncConnection, name: str):
        await conn.execute(sa.insert(users).values(name=name))
        raise ValueError("Triggering rollback")

    with pytest.raises(ValueError, match="Triggering rollback"):
        await create_user_and_fail(name="testuser")  # type: ignore

    async with async_engine.begin() as conn:
        count = await conn.execute(sa.select(sa.func.count()).select_from(users))
        count = count.scalar_one()
        assert count == 0
