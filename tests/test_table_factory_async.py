import pytest


@pytest.mark.asyncio
async def test_table_factory_async():
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
