import pytest


def test_field():
    from pydantic import BaseModel, Field

    from sqla_fancy_core import TableFactory

    tf = TableFactory()

    # Define a table
    class User:
        name = tf.string("name")
        Table = tf("author")

        @staticmethod
        def name_field(default=...):
            return Field(default, max_length=5)

    # Define a pydantic schema
    class CreateUser(BaseModel):
        name: str = User.name_field()

    # Define a pydantic schema
    class UpdateUser(BaseModel):
        name: str | None = User.name_field(None)

    assert CreateUser(name="John").model_dump() == {"name": "John"}
    assert UpdateUser(name="John").model_dump() == {"name": "John"}
    assert UpdateUser().model_dump(exclude_unset=True) == {}

    with pytest.raises(ValueError):
        CreateUser()
    with pytest.raises(ValueError):
        UpdateUser(name="John Doe")
