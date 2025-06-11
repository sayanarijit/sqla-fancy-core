def test_table_factory():
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
