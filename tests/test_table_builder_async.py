import pytest


@pytest.mark.asyncio
async def test_table_builder_async():
    import sqlalchemy as sa
    from sqlalchemy.ext.asyncio import create_async_engine

    from sqla_fancy_core import TableBuilder

    tb = TableBuilder()

    # Define a table
    class Author:
        id = tb.auto_id()
        name = tb.string("name")
        created_at = tb.created_at()
        updated_at = tb.updated_at()

        Table = tb("author")

    # Or define it without losing type hints
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

        Table = tb("book")

    # Create the engine
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")

    # Create the tables
    async with engine.begin() as conn:
        await conn.run_sync(tb.metadata.create_all)

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


@pytest.mark.asyncio
async def test_table_builder_async_multi_column_unique_constraints():
    import sqlalchemy as sa
    from sqlalchemy.ext.asyncio import create_async_engine

    from sqla_fancy_core import TableBuilder

    tb = TableBuilder()

    # Option 1: constraint defined inside the class using tb(sa.UniqueConstraint(...))
    class User:
        classroom = tb.integer("classroom")
        roll_no = tb.integer("roll_no")

        ux_classroom_roll_no = tb(sa.UniqueConstraint(classroom, roll_no))

        Table = tb("users_async")

    # Option 2: constraint passed when building the table
    class UserAlt:
        classroom = tb.integer("classroom")
        roll_no = tb.integer("roll_no")

        Table = tb("users_async_alt", sa.UniqueConstraint(classroom, roll_no))

    engine = create_async_engine("sqlite+aiosqlite:///:memory:")

    async with engine.begin() as conn:
        await conn.run_sync(tb.metadata.create_all)

    def _check(conn):
        insp = sa.inspect(conn)

        # Validate Option 1
        constraints = insp.get_unique_constraints(User.Table.name)
        cols_sets = {tuple(c["column_names"]) for c in constraints}
        assert ("classroom", "roll_no") in cols_sets or ("roll_no", "classroom") in cols_sets

        # Validate Option 2
        constraints_alt = insp.get_unique_constraints(UserAlt.Table.name)
        cols_sets_alt = {tuple(c["column_names"]) for c in constraints_alt}
        assert ("classroom", "roll_no") in cols_sets_alt or ("roll_no", "classroom") in cols_sets_alt

    async with engine.begin() as conn:
        await conn.run_sync(_check)
