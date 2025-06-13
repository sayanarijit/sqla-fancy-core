# sqla-fancy-core

There are plenty of ORMs to choose from in Python world, but not many sql query makers for folks who prefer to stay close to the original SQL syntax, without sacrificing security and code readability. The closest, most mature and most flexible query maker you can find is SQLAlchemy core.

But the syntax of defining tables and making queries has a lot of scope for improvement. For example, the `table.c.column` syntax is too dynamic, unreadable, and probably has performance impact too. It also doesn’t play along with static type checkers and linting tools.

So here I present one attempt at getting the best out of SQLAlchemy core by changing the way we define tables.

The table factory class it exposes, helps define tables in a way that eliminates the above drawbacks. Moreover, you can subclass it to add your preferred global defaults for columns (e.g. not null as default). Or specify custom column types with consistent naming (e.g. created_at).

### Basic Usage

```python
import sqlalchemy as sa

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

# Create the tables
engine = sa.create_engine("sqlite:///:memory:")
tf.metadata.create_all(engine)

with engine.begin() as txn:
    # Insert author
    qry = (
        sa.insert(Author.Table)
        .values({Author.name: "John Doe"})
        .returning(Author.id)
    )
    author = txn.execute(qry).mappings().first()
    author_id = author[Author.id]
    assert author_id == 1

    # Insert book
    qry = (
        sa.insert(Book.Table)
        .values({Book.title: "My Book", Book.author_id: author_id})
        .returning(Book.id)
    )
    book = txn.execute(qry).mappings().first()
    assert book[Book.id] == 1

    # Query the data
    qry = sa.select(Author.name, Book.title).join(
        Book.Table,
        Book.author_id == Author.id,
    )
    result = txn.execute(qry).all()
    assert result == [("John Doe", "My Book")], result
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

Production. For folks who prefer query maker over ORM.

### Comparison with other projects:

**Piccolo**: Tight integration with drivers. Very opinionated. Not as flexible or mature as sqlalchemy core.

**Pypika**: Doesn’t prevent sql injection by default. Hence can be considered insecure.

**Raw string queries with placeholders**: sacrifices code readability, and prone to sql injection if one forgets to use placeholders.

**Other ORMs**: They are full blown ORMs, not query makers.
