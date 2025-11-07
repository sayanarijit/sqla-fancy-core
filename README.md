# sqla-fancy-core

There are plenty of ORMs to choose from in Python world, but not many sql query makers for folks who prefer to stay close to the original SQL syntax, without sacrificing security and code readability. The closest, most mature and most flexible query maker you can find is SQLAlchemy core.

But the syntax of defining tables and making queries has a lot of scope for improvement. For example, the `table.c.column` syntax is too dynamic, unreadable, and probably has performance impact too. It also doesn’t play along with static type checkers and linting tools.

So here I present one attempt at getting the best out of SQLAlchemy core by changing the way we define tables.

The table factory class it exposes, helps define tables in a way that eliminates the above drawbacks. Moreover, you can subclass it to add your preferred global defaults for columns (e.g. not null as default). Or specify custom column types with consistent naming (e.g. created_at).

## Basic Usage

First, let's define a table using the `TableFactory`.

```python
import sqlalchemy as sa
from sqla_fancy_core import TableFactory

tf = TableFactory()

class Author:
    id = tf.auto_id()
    name = tf.string("name")
    created_at = tf.created_at()
    updated_at = tf.updated_at()

    Table = tf("author")
```

The `TableFactory` provides a convenient way to define columns with common attributes. For more complex scenarios, you can define tables without losing type hints:

```python
class Book:
    id = tf(sa.Column("id", sa.Integer, primary_key=True, autoincrement=True))
    title = tf(sa.Column("title", sa.String(255), nullable=False))
    author_id = tf(sa.Column("author_id", sa.Integer, sa.ForeignKey(Author.id)))
    created_at = tf(
        sa.Column(
            "created_at",
            sa.DateTime,
            nullable=False,
            server_default=sa.func.now(),
        )
    )
    updated_at = tf(
        sa.Column(
            "updated_at",
            sa.DateTime,
            nullable=False,
            server_default=sa.func.now(),
            onupdate=sa.func.now(),
        )
    )

    Table = tf(sa.Table("book", sa.MetaData()))
```

Now, let's create an engine and the tables.

```python
from sqlalchemy.ext.asyncio import create_async_engine

# Create the engine
engine = create_async_engine("sqlite+aiosqlite:///:memory:")

# Create the tables
async with engine.begin() as conn:
    await conn.run_sync(tf.metadata.create_all)
```

With the tables created, you can perform CRUD operations.

### CRUD Operations

Here's how you can interact with the database using the defined tables.

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

`sqla-fancy-core` provides `fancy` engine wrappers that simplify database interactions by automatically managing connections and transactions. The `fancy` function wraps a SQLAlchemy `Engine` or `AsyncEngine` and returns a wrapper object with two primary methods:

- `x(conn, query)`: Executes a query. It uses the provided `conn` if available, otherwise it creates a new connection.
- `tx(conn, query)`: Executes a query within a transaction. It uses the provided `conn` if available, otherwise it creates a new connection and begins a transaction.

This is particularly useful for writing connection-agnostic query functions.

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

    # Without an explicit transaction
    assert await get_data() == 1

    # With an explicit transaction
    async with engine.connect() as conn:
        assert await get_data(conn) == 1
```

## Decorators: Inject, connect, transact

When writing plain SQLAlchemy Core code, you often pass connections around and manage transactions manually. The decorators in `sqla-fancy-core` help you keep functions connection-agnostic and composable, while remaining explicit and safe.

At the heart of it is `Inject(engine)`, a tiny marker used as a default parameter value to tell decorators where to inject a connection.

- `Inject(engine)`: marks which parameter should receive a connection derived from the given engine.
- `@connect`: ensures the injected parameter is a live connection. If you passed a connection explicitly, it will use that one as-is. Otherwise, it will open a new connection for the call and close it afterwards. No transaction is created by default.
- `@transact`: ensures the injected parameter is inside a transaction. If you pass a connection already in a transaction, it reuses it; if you pass a connection outside a transaction, it starts one; if you pass nothing, it opens a new connection and begins a transaction for the duration of the call.

All three work both for sync and async engines. The signatures remain the same — you only change the default value to `Inject(engine)`.

### Quick reference

- Prefer `@connect` for read-only operations or when you want to control commit/rollback yourself.
- Prefer `@transact` to wrap a function in a transaction automatically and consistently.
- You can still pass `conn=...` explicitly to either decorator to reuse an existing connection/transaction.

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

# 1) Ensure a connection is available (no implicit transaction)
@connect
def get_user_count(conn=Inject(engine)):
    return conn.execute(sa.select(sa.func.count()).select_from(users)).scalar_one()

assert get_user_count() == 0

# 2) Wrap in a transaction automatically
@transact
def create_user(name: str, conn=Inject(engine)):
    conn.execute(sa.insert(users).values(name=name))

create_user("alice")
assert get_user_count() == 1

# 3) Reuse an explicit connection or transaction
with engine.begin() as txn:
    create_user("bob", conn=txn)
    assert get_user_count(conn=txn) == 2

assert get_user_count() == 2
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

assert await get_user_count() == 0
await create_user("carol")
assert await get_user_count() == 1

async with engine.connect() as conn:
    await create_user("dave", conn=conn)
    assert await get_user_count(conn=conn) == 2
```

### Works with dependency injection frameworks

These decorators pair nicely with frameworks like FastAPI. You can keep a single function that works both inside DI (with an injected connection) and outside it (self-managed).

Sync example with FastAPI:

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

Async example with FastAPI:

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

Notes:

- `@connect` never starts a transaction by itself; `@transact` ensures one.
- Passing an explicit `conn` always wins — the decorators simply adapt to what you give them.
- The injection marker keeps your function signatures clean and type-checker friendly.

## With Pydantic Validation

You can integrate `sqla-fancy-core` with Pydantic for data validation.

```python
from typing import Any
import sqlalchemy as sa
from pydantic import BaseModel, Field
import pytest

from sqla_fancy_core import TableFactory

tf = TableFactory()

def field(col, default: Any = ...) -> Field:
    return col.info["kwargs"]["field"](default)

# Define a table
class User:
    name = tf(
        sa.Column("name", sa.String),
        field=lambda default: Field(default, max_length=5),
    )
    Table = tf("author")

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
