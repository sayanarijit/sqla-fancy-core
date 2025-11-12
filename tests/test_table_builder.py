def test_table_builder():
    import sqlalchemy as sa

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

        Table = tb(sa.Table("book", sa.MetaData()))

    # Create the engine
    engine = sa.create_engine("sqlite:///:memory:")

    # Create the tables
    tb.metadata.create_all(engine)

    with engine.begin() as txn:
        # Insert author
        qry = (
            sa.insert(Author.Table)
            .values({Author.name: "John Doe"})
            .returning(Author.id)
        )
        author = txn.execute(qry).mappings().one()
        author_id = author[Author.id]
        assert author_id == 1

        # Insert book
        qry = (
            sa.insert(Book.Table)
            .values({Book.title: "My Book", Book.author_id: author_id})
            .returning(Book.id)
        )
        book = txn.execute(qry).mappings().one()
        assert book[Book.id] == 1

        # Query the data
        qry = sa.select(Author.name, Book.title).join(
            Book.Table,
            Book.author_id == Author.id,
        )
        result = txn.execute(qry).all()
        assert result == [("John Doe", "My Book")], result
