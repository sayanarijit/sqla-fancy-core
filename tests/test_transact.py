from typing import Annotated

import pytest
import pytest_asyncio
import sqlalchemy as sa
from fastapi import Form
from sqlalchemy.ext.asyncio import AsyncConnection, create_async_engine
from sqlalchemy.pool import StaticPool

from sqla_fancy_core.decorators import Inject, transact

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
    """Provides a synchronous in-memory SQLite engine shared across threads."""
    engine = sa.create_engine(
        "sqlite+pysqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
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


def test_decorator_sync_commit(sync_engine):
    """Test that the sync decorator commits a successful transaction."""

    @transact
    def create_user(name: str, conn: sa.Connection = Inject(sync_engine)):
        assert isinstance(conn, sa.Connection)
        conn.execute(sa.insert(users).values(name=name))

    create_user("testuser")

    with sync_engine.begin() as conn:
        count = conn.execute(sa.select(sa.func.count()).select_from(users)).scalar_one()
        # Two users should exist: one from direct call, one from FastAPI request
        assert count == 1
        name = conn.execute(sa.select(users.c.name)).scalar_one()
        assert name == "testuser"

        create_user(name="anotheruser", conn=conn)

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

    @transact
    def create_user_and_fail(name: str, conn: sa.Connection = Inject(sync_engine)):
        conn.execute(sa.insert(users).values(name=name))
        raise ValueError("Triggering rollback")

    with pytest.raises(ValueError, match="Triggering rollback"):
        create_user_and_fail(name="testuser")

    with sync_engine.begin() as conn:
        count = conn.execute(sa.select(sa.func.count()).select_from(users)).scalar_one()
        assert count == 0


def test_batch_commit_rollback(sync_engine):
    """Test multiple transactions with commits and rollbacks."""

    @transact
    def create_user(name: str, conn: sa.Connection = Inject(sync_engine)):
        conn.execute(sa.insert(users).values(name=name))

    def batch_add_users(names: list[str]):
        with sync_engine.begin() as conn:
            for name in names:
                create_user(conn=conn, name=name)
            raise ValueError("Triggering rollback")

    with sync_engine.begin() as conn:
        try:
            batch_add_users(["user1", "user2"])
        except ValueError:
            pass

    create_user("user3")

    with sync_engine.begin() as conn:
        count = conn.execute(sa.select(sa.func.count()).select_from(users)).scalar_one()
        assert count == 1
        names = (
            conn.execute(sa.select(users.c.name).order_by(users.c.id)).scalars().all()
        )
        assert names == ["user3"]


#
@pytest.mark.asyncio
async def test_decorator_async_commit(async_engine):
    """Test that the async decorator commits a successful transaction."""

    @transact
    async def create_user(name: str, conn: AsyncConnection = Inject(async_engine)):
        assert isinstance(conn, AsyncConnection)
        await conn.execute(sa.insert(users).values(name=name))

    await create_user(name="testuser")

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

    @transact
    async def create_user_and_fail(
        name: str, conn: AsyncConnection = Inject(async_engine)
    ):
        await conn.execute(sa.insert(users).values(name=name))
        raise ValueError("Triggering rollback")

    with pytest.raises(ValueError, match="Triggering rollback"):
        await create_user_and_fail(name="testuser")

    async with async_engine.begin() as conn:
        count = await conn.execute(sa.select(sa.func.count()).select_from(users))
        count = count.scalar_one()
        assert count == 0


def test_fastapi_dependency_injection(sync_engine):
    """Test that the decorator works well with dependency injection frameworks."""

    from fastapi import Depends, FastAPI
    from fastapi.testclient import TestClient

    app = FastAPI()

    def get_transaction():
        metadata.create_all(sync_engine)
        with sync_engine.begin() as conn:
            yield conn

    @transact
    @app.post("/create-user")
    def create_user(
        name: Annotated[str, Form(...)],
        conn: Annotated[sa.Connection, Depends(get_transaction)] = Inject(sync_engine),
    ):
        assert isinstance(conn, sa.Connection)
        conn.execute(sa.insert(users).values(name=name))

    testapp = TestClient(app)

    create_user(name="outside fastapi")
    testapp.post("/create-user", data={"name": "inside fastapi"})

    with sync_engine.begin() as conn:
        count = conn.execute(sa.select(sa.func.count()).select_from(users)).scalar_one()
        assert count == 2
        name = conn.execute(sa.select(users.c.name).where(users.c.id == 1)).scalar_one()
        assert name == "outside fastapi"
        name = conn.execute(sa.select(users.c.name).where(users.c.id == 2)).scalar_one()
        assert name == "inside fastapi"


@pytest.mark.asyncio
async def test_dependency_injection_async(async_engine):
    """Test that the async decorator works well with dependency injection frameworks."""

    from fastapi import Depends, FastAPI, Form
    from starlette.testclient import TestClient as AsyncTestClient

    app = FastAPI()

    async def get_transaction():
        async with async_engine.begin() as conn:
            yield conn

    @transact
    @app.post("/create-user")
    async def create_user(
        name: Annotated[str, Form(...)],
        conn: Annotated[AsyncConnection, Depends(get_transaction)] = Inject(
            async_engine
        ),
    ):
        assert isinstance(conn, AsyncConnection)
        await conn.execute(sa.insert(users).values(name=name))

    testapp = AsyncTestClient(app)

    await create_user(name="outside fastapi")
    testapp.post("/create-user", data={"name": "inside fastapi"})

    async with async_engine.begin() as conn:
        count = await conn.execute(sa.select(sa.func.count()).select_from(users))
        count = count.scalar_one()
        assert count == 2
        name = await conn.execute(sa.select(users.c.name).where(users.c.id == 1))
        name = name.scalar_one()
        assert name == "outside fastapi"
        name = await conn.execute(sa.select(users.c.name).where(users.c.id == 2))
        name = name.scalar_one()
        assert name == "inside fastapi"
