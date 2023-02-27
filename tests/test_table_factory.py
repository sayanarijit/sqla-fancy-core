def test_table_factory():
    import sqlalchemy as sa

    from sqla_fancy_core import TableFactory

    metadata = sa.MetaData()

    # Set the global default values for all columns
    TableFactory.DEFAULTS = dict(nullable=False)
    TableFactory.FK_DEFAULTS = dict(ondelete="CASCADE")

    # Define a table
    class Author:
        _tf = TableFactory()

        id = _tf.auto_id()
        name = _tf.string("name")
        created_at = _tf.created_at()
        updated_at = _tf.updated_at()

        Table = _tf("author", metadata)

    # Define a table
    class Book:

        _tf = TableFactory(defaults=dict(onupdate="CASCADE"))

        id = _tf.auto_id()
        title = _tf.string("title")
        author_id = _tf.foreign_key("author_id", Author.id)
        created_at = _tf.created_at()
        updated_at = _tf.updated_at()

        Table = _tf("book", metadata)

    # Create the tables
    engine = sa.create_engine("sqlite:///:memory:")
    metadata.create_all(engine)

    with engine.connect() as conn:

        # Insert author
        qry = (
            sa.insert(Author.Table)
            .values({Author.name: "John Doe"})
            .returning(Author.id)
        )
        author = next(conn.execute(qry))
        (author_id,) = author

        assert author_id == 1

        # Insert book
        qry = (
            sa.insert(Book.Table)
            .values({Book.title: "My Book", Book.author_id: author_id})
            .returning(Book.id)
        )
        book = next(conn.execute(qry))
        (book_id,) = book
        assert book_id == 1

        # Query the data
        qry = sa.select(Author.name, Book.title,).join(
            Book.Table,
            Book.author_id == Author.id,
        )
        result = conn.execute(qry).fetchall()
        assert result == [("John Doe", "My Book")], result
