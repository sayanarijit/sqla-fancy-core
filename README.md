# sqla-fancy-core

There are plenty of ORMs to choose from in Python world, but not many sql query makers for folks who prefer to stay close to the original SQL syntax, without sacrificing security and code readability. The closest, most mature and most flexible query maker you can find is SQLAlchemy core.

But the syntax of defining tables and making queries has a lot of scope for improvement. For example, the `table.c.column` syntax is too dynamic, unreadable, and probably has performance impact too. It also doesn’t play along with static type checkers and linting tools.

So here I present one attempt at getting the best out of SQLAlchemy core by changing the way we define tables.

The table factory class it exposes, helps define tables in a way that eliminates the above drawbacks. Moreover, you can subclass it to add your preferred global defaults for columns (e.g. not null as default). Or specify custom column types with consistent naming (e.g. created_at).

### Basic Usage

```python
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import create_async_engine

from sqla_fancy_core import TableFactory

tf = TableFactory()

# Define a table
class Author:
    id = tf.auto_id()
    name = tf.string("name")
    created_at = tf.created_at()
    updated_at = tf.updated_at()

    Table = tf("author")

# Or define it without losing type hints
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

# Create the engine
engine = create_async_engine("sqlite+aiosqlite:///:memory:")

# Create the tables
async with engine.begin() as conn:
    await conn.run_sync(tf.metadata.create_all)

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

### Fancy Engine Wrappers

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
    return fancy_engine.tx(conn, sa.text("SELECT 1")).scalar_one()

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
        result = await fancy_engine.x(conn, sa.text("SELECT 1"))
        return await result.scalar_one()

    # Without an explicit transaction
    assert await get_data() == 1

    # With an explicit transaction
    async with engine.connect() as conn:
        assert await get_data(conn) == 1
```

### Transaction Decorator

The `@transact` decorator further simplifies transaction management. It wraps a function and ensures that it runs within a transaction. The decorator provides a connection object as the argument typed as `Connection` or `AsyncConnection`.

**Sync Example:**

```python
import sqlalchemy as sa
from sqla_fancy_core import transact

engine = sa.create_engine("sqlite:///:memory:")

@transact(engine)
def create_user(conn: sa.Connection, name: str):
    return conn.execute(sa.text(f"INSERT INTO users (name) VALUES ('{name}')"))

# This will run in a transaction
create_user("John Doe")

# This will also run in the scoped transaction
with engine.begin() as conn:
    create_user(conn, "Jane Doe")
```

**Async Example:**

```python
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import create_async_engine
from sqla_fancy_core import transact

async def main():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")

    @transact(engine)
    async def create_user(conn: sa.AsyncConnection, name: str):
        return await conn.execute(sa.text(f"INSERT INTO users (name) VALUES ('{name}')"))

    # This will run in a transaction
    await create_user("John Doe")

    # This will also run in the scoped transaction
    async with engine.begin() as conn:
        await create_user(conn, "Jane Doe")
```

### With Pydantic Validation

```python
from typing import Any
import sqlalchemy as sa
from pydantic import BaseModel, Field

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

### Target audience

Production. For folks who prefer query maker over ORM, looking for a robust sync/async driver integration, wanting to keep code readable and secure.

### Comparison with other projects:

**Peewee**: Not as flexible or mature as sqlalchemy core. Also, no official async support.

**Piccolo**: Tight integration with drivers. Very opinionated. Not as flexible or mature as sqlalchemy core.

**Pypika**: Doesn’t prevent sql injection by default. Hence can be considered insecure.

**Raw string queries with placeholders**: sacrifices code readability, and prone to sql injection if one forgets to use placeholders.

**Other ORMs**: They are full blown ORMs, not query makers.
