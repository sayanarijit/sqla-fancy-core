# sqla-fancy-core

A collection of type-safe, async friendly, and un-opinionated enhancements to SQLAlchemy Core that works well with mordern web servers.

**Why?**

- ORMs are magical, but it's not always a feature. Sometimes, we crave for familiar.
- SQLAlchemy Core is powerful but `table.c.column` breaks static type checking and has runtime overhead. This library provides a better way to define tables while keeping all of SQLAlchemy's flexibility. See [Table Builder](#table-builder).
- The idea of sessions can feel too magical and opinionated. This library removes the magic and opinions and takes you to back to familiar transactions's territory, providing multiple un-opinionated APIs to deal with it. See [Wrappers](#fancy-engine-wrappers) and [Decorators](#decorators-inject-connect-transact).

**Demos:**

- [FastAPI - sqla-fancy-core example app](https://github.com/sayanarijit/fastapi-sqla-fancy-core-example-app).

## Table builder

Define tables with static column references

**Example:**

Define tables:

```python
import sqlalchemy as sa
from sqla_fancy_core import TableBuilder

tb = TableBuilder()

class Author:
    id = tb.auto_id()
    name = tb.string("name")
    created_at = tb.created_at()
    updated_at = tb.updated_at()

    Table = tb("author")
```

For complex scenarios, define columns explicitly:

```python
class Book:
    id = tb(sa.Column("id", sa.Integer, primary_key=True, autoincrement=True))
    title = tb(sa.Column("title", sa.String(255), nullable=False))
    author_id = tb(sa.Column("author_id", sa.Integer, sa.ForeignKey(Author.id)))
    created_at = tb(
        sa.Column(
            "created_at",
            sa.DateTime,
            nullable=False,
            server_default=sa.func.now(),
        )
    )
    updated_at = tb(
        sa.Column(
            "updated_at",
            sa.DateTime,
            nullable=False,
            server_default=sa.func.now(),
            onupdate=sa.func.now(),
        )
    )

    Table = tb(sa.Table("book", sa.MetaData()))
```

Create tables:

```python
from sqlalchemy.ext.asyncio import create_async_engine

# Create the engine
engine = create_async_engine("sqlite+aiosqlite:///:memory:")

# Create the tables
async with engine.begin() as conn:
    await conn.run_sync(tb.metadata.create_all)
```

Perform CRUD operations:

```python
async with engine.begin() as txn:
    # Insert author
    qry = (
        sa.insert(Author.Table)
        .values({Author.name: "John Doe"})
        .returning(Author.id)
    )
    author = (await txn.execute(qry)).mappings().one()
    author_id = author[Author.id]
    assert author_id == 1

    # Insert book
    qry = (
        sa.insert(Book.Table)
        .values({Book.title: "My Book", Book.author_id: author_id})
        .returning(Book.id)
    )
    book = (await txn.execute(qry)).mappings().one()
    assert book[Book.id] == 1

    # Query the data
    qry = sa.select(Author.name, Book.title).join(
        Book.Table,
        Book.author_id == Author.id,
    )
    result = (await txn.execute(qry)).all()
    assert result == [("John Doe", "My Book")], result
```

## Fancy Engine Wrappers

Simplify connection and transaction management. The `fancy()` function wraps a SQLAlchemy engine and provides:

- `x(conn, query)`: Execute query with optional connection
- `tx(conn, query)`: Execute query in transaction, uses the given connection if present
- `atomic()`: Context manager for transaction scope
- `ax(query)`: Execute inside `atomic()` context (raises `AtomicContextError` outside)
- `atx(query)`: Auto-transactional (reuses `atomic()` if present, or creates new transaction)

### Basic Examples

**Sync Example:**

```python
import sqlalchemy as sa
from sqla_fancy_core import fancy

engine = sa.create_engine("sqlite:///:memory:")
fancy_engine = fancy(engine)

def get_data(conn: sa.Connection | None = None):
    return fancy_engine.tx(conn, sa.select(sa.literal(1))).scalar_one()

# Without an explicit transaction
assert get_data() == 1

# With an explicit transaction
with engine.begin() as conn:
    assert get_data(conn) == 1
```

**Async Example:**

```python
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import create_async_engine
from sqla_fancy_core import fancy

async def main():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    fancy_engine = fancy(engine)

    async def get_data(conn: sa.AsyncConnection | None = None):
        result = await fancy_engine.x(conn, sa.select(sa.literal(1)))
        return result.scalar_one()

    # Without an explicit connection
    assert await get_data() == 1

    # With an explicit connection
    async with engine.connect() as conn:
        assert await get_data(conn) == 1
```

### Using the atomic() Context Manager

Group operations in a single transaction without passing around the Connection/AsyncConnection instance. Nested `atomic()` contexts share the outer connection.

**Sync Example:**

```python
import sqlalchemy as sa
from sqla_fancy_core import fancy, TableBuilder

tb = TableBuilder()

class User:
    id = tb.auto_id()
    name = tb.string("name")
    Table = tb("users")

engine = sa.create_engine("sqlite:///:memory:")
tb.metadata.create_all(engine)
fancy_engine = fancy(engine)

# Group operations in one transaction
with fancy_engine.atomic():
    fancy_engine.ax(sa.insert(User.Table).values(name="Alice"))
    fancy_engine.ax(sa.insert(User.Table).values(name="Bob"))
    result = fancy_engine.ax(sa.select(sa.func.count()).select_from(User.Table))
    count = result.scalar_one()
    assert count == 2
```

**Async Example:**

```python
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import create_async_engine
from sqla_fancy_core import fancy, TableBuilder

tb = TableBuilder()

class User:
    id = tb.auto_id()
    name = tb.string("name")
    Table = tb("users")

async def main():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
    await conn.run_sync(tb.metadata.create_all)

    fancy_engine = fancy(engine)

    async with fancy_engine.atomic():
        await fancy_engine.ax(sa.insert(User.Table).values(name="Alice"))
        await fancy_engine.ax(sa.insert(User.Table).values(name="Bob"))
        result = await fancy_engine.ax(sa.select(sa.func.count()).select_from(User.Table))
        count = result.scalar_one()
        assert count == 2
```

## Decorators: Inject, connect, transact

Keep functions connection-agnostic with decorator-based injection.

**Components:**

- `Inject(engine)`: Marks parameter for connection injection
- `@connect`: Ensures live connection (no transaction by default)
- `@transact`: Ensures transactional connection

Use `@connect` for read-only operations. Use `@transact` for writes.

### Sync examples

```python
import sqlalchemy as sa
from sqla_fancy_core.decorators import Inject, connect, transact

engine = sa.create_engine("sqlite:///:memory:")
metadata = sa.MetaData()
users = sa.Table(
    "users",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
    sa.Column("name", sa.String),
)
metadata.create_all(engine)

@connect
def get_user_count(conn=Inject(engine)):
    return conn.execute(sa.select(sa.func.count()).select_from(users)).scalar_one()

assert get_user_count() == 0

@transact
def create_user(name: str, conn=Inject(engine)):
    conn.execute(sa.insert(users).values(name=name))

# Without an explicit transaction
create_user("alice")
assert get_user_count() == 1

# With an explicit transaction
with engine.begin() as txn:
    create_user("bob", conn=txn)
    assert get_user_count(conn=txn) == 2
```

### Async examples

```python
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import create_async_engine, AsyncConnection
from sqla_fancy_core.decorators import Inject, connect, transact

engine = create_async_engine("sqlite+aiosqlite:///:memory:")
metadata = sa.MetaData()
users = sa.Table(
    "users",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
    sa.Column("name", sa.String),
)

async with engine.begin() as conn:
    await conn.run_sync(metadata.create_all)

@connect
async def get_user_count(conn=Inject(engine)):
    result = await conn.execute(sa.select(sa.func.count()).select_from(users))
    return result.scalar_one()

@transact
async def create_user(name: str, conn=Inject(engine)):
    await conn.execute(sa.insert(users).values(name=name))

# Without an explicit transaction
assert await get_user_count() == 0
await create_user("carol")
assert await get_user_count() == 1

# With an explicit transaction
async with engine.connect() as conn:
    await create_user("dave", conn=conn)
    assert await get_user_count(conn=conn) == 2
```

Also works with dependency injection frameworks like FastAPI:

```python
from typing import Annotated
from fastapi import Depends, FastAPI, Form
import sqlalchemy as sa
from sqla_fancy_core.decorators import Inject, transact

app = FastAPI()

def get_transaction():
    with engine.begin() as conn:
        yield conn

@transact
@app.post("/create-user")
def create_user(
    name: Annotated[str, Form(...)],
    conn: Annotated[sa.Connection, Depends(get_transaction)] = Inject(engine),
):
    conn.execute(sa.insert(users).values(name=name))

# Works outside FastAPI too — starts its own transaction
create_user(name="outside fastapi")
```

Async with FastAPI:

```python
from typing import Annotated
from fastapi import Depends, FastAPI, Form
from sqlalchemy.ext.asyncio import AsyncConnection
import sqlalchemy as sa
from sqla_fancy_core.decorators import Inject, transact

app = FastAPI()

async def get_transaction():
    async with engine.begin() as conn:
        yield conn

@transact
@app.post("/create-user")
async def create_user(
    name: Annotated[str, Form(...)],
    conn: Annotated[AsyncConnection, Depends(get_transaction)] = Inject(engine),
):
    await conn.execute(sa.insert(users).values(name=name))
```

## With Pydantic Validation

If you like to define validation logic in the column itself, this is one way to do it:

```python
from typing import Any
import sqlalchemy as sa
from pydantic import BaseModel, Field
import pytest

from sqla_fancy_core import TableBuilder

tb = TableBuilder()

def field(col, default: Any = ...) -> Field:
    return col.info["kwargs"]["field"](default)

# Define a table
class User:
    name = tb(
        sa.Column("name", sa.String),
        field=lambda default: Field(default, max_length=5),
    )
    Table = tb("author")

# Define a pydantic schema
class CreateUser(BaseModel):
    name: str = field(User.name)

# Define a pydantic schema
class UpdateUser(BaseModel):
    name: str | None = field(User.name, None)

assert CreateUser(name="John").model_dump() == {"name": "John"}
assert UpdateUser(name="John").model_dump() == {"name": "John"}
assert UpdateUser().model_dump(exclude_unset=True) == {}

with pytest.raises(ValueError):
    CreateUser()
with pytest.raises(ValueError):
    UpdateUser(name="John Doe")
```

## Target audience

Production. For folks who prefer query maker over ORM, looking for a robust sync/async driver integration, wanting to keep code readable and secure.

## Comparison with other projects:

**Peewee**: No type hints. Also, no official async support.

**Piccolo**: Tight integration with drivers. Very opinionated. Not as flexible or mature as sqlalchemy core.

**Pypika**: Doesn’t prevent sql injection by default. Hence can be considered insecure.

**Raw string queries with placeholders**: sacrifices code readability, and prone to sql injection if one forgets to use placeholders.

**Other ORMs**: They are full blown ORMs, not query makers.
