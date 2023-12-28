# sqla-fancy-core

SQLAlchemy core, but fancier.

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

# Define a table
class Book:

    id = tf.auto_id()
    title = tf.string("title")
    author_id = tf.foreign_key("author_id", Author.id)
    created_at = tf.created_at()
    updated_at = tf.updated_at()

    Table = tf("book")

# Create the tables
engine = sa.create_engine("sqlite:///:memory:")
tf.metadata.create_all(engine)

with engine.connect() as conn:
    # Insert author
    qry = (
        sa.insert(Author.Table)
        .values({Author.name: "John Doe"})
        .returning(Author.id)
    )
    author = next(conn.execute(qry).mappings())
    author_id = author[Author.id]
    assert author_id == 1

    # Insert book
    qry = (
        sa.insert(Book.Table)
        .values({Book.title: "My Book", Book.author_id: author_id})
        .returning(Book.id)
    )
    book = next(conn.execute(qry).mappings())
    assert book[Book.id] == 1

    # Query the data
    qry = sa.select(Author.name, Book.title).join(
        Book.Table,
        Book.author_id == Author.id,
    )
    result = conn.execute(qry).fetchall()
    assert result == [("John Doe", "My Book")], result
```

### With Pydantic Validation

```python

from pydantic import BaseModel, Field

from sqla_fancy_core import TableFactory

tf = TableFactory()

# Define a table
class User:
    name = tf.string("name", info={"field": Field(..., max_length=5)})
    Table = tf("author")

# Define a pydantic schema
class CreateUser(BaseModel):
    name: str = User.name.info["field"]

assert CreateUser(name="John").model_dump() == {"name": "John"}
with pytest.raises(ValueError):
    CreateUser(name="John Doe")
```
