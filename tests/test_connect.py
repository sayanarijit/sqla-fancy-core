from typing import Annotated

import pytest
import pytest_asyncio
import sqlalchemy as sa
from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncConnection, create_async_engine

from sqla_fancy_core.decorators import Inject, connect

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


def test_decorator_sync_connect(sync_engine):
    @connect
    def get_user_count(conn=Inject(sync_engine)):
        # Simple select count(*) using SQLAlchemy Core
        result = conn.execute(sa.select(sa.func.count()).select_from(users))
        return result.scalar_one()

    # Initially table empty
    assert get_user_count() == 0

    # Insert a couple of rows using provided engine connection in context
    with sync_engine.connect() as conn:
        conn.execute(users.insert().values(name="alice"))
        conn.execute(users.insert().values(name="bob"))
        conn.commit()

    assert get_user_count() == 2


def test_rollback_on_no_commit(sync_engine):
    @connect
    def create_user(name: str, conn=Inject(sync_engine)):
        conn.execute(sa.insert(users).values(name=name))
        # Note: No commit here

    create_user(name="charlie")
    create_user(name="david")

    # Since there was no commit, the users should not be present
    with sync_engine.connect() as conn:
        count = conn.execute(sa.select(sa.func.count()).select_from(users)).scalar_one()
        assert count == 0


def test_fastapi_integration(sync_engine):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    app = FastAPI()

    def get_connection():
        with sync_engine.connect() as conn:
            yield conn

    @connect
    @app.get("/user-count")
    def get_user_count(
        conn: Annotated[sa.Connection, Depends(get_connection)] = Inject(sync_engine),
    ):
        result = conn.execute(sa.select(sa.func.count()).select_from(users))
        return result.scalar_one()

    assert get_user_count() == 0

    client = TestClient(app)
    response = client.get("/user-count")
    assert response.status_code == 200
    assert response.json() == 0


@pytest.mark.asyncio
async def test_decorator_async_connect(async_engine):
    @connect
    async def get_user_count(conn=Inject(async_engine)):
        result = await conn.execute(sa.select(sa.func.count()).select_from(users))
        return result.scalar_one()

    assert await get_user_count() == 0

    async with async_engine.connect() as conn:
        await conn.execute(users.insert().values(name="eve"))
        await conn.execute(users.insert().values(name="frank"))
        await conn.commit()

    assert await get_user_count() == 2


@pytest.mark.asyncio
async def test_async_rollback_on_no_commit(async_engine):
    @connect
    async def create_user(name: str, conn=Inject(async_engine)):
        await conn.execute(sa.insert(users).values(name=name))
        # Note: No commit here

    await create_user(name="grace")
    await create_user(name="heidi")

    async with async_engine.connect() as conn:
        result = await conn.execute(sa.select(sa.func.count()).select_from(users))
        count = result.scalar_one()
        assert count == 0


@pytest.mark.asyncio
async def test_async_fastapi_integration(async_engine):
    from fastapi import FastAPI
    from starlette.testclient import TestClient as AsyncTestClient

    app = FastAPI()

    async def get_connection():
        async with async_engine.connect() as conn:
            yield conn

    @connect
    @app.get("/user-count")
    async def get_user_count(
        conn: Annotated[AsyncConnection, Depends(get_connection)] = Inject(
            async_engine
        ),
    ):
        result = await conn.execute(sa.select(sa.func.count()).select_from(users))
        return result.scalar_one()

    assert await get_user_count() == 0

    client = AsyncTestClient(app)
    response = client.get("/user-count")
    assert response.status_code == 200
    assert response.json() == 0
