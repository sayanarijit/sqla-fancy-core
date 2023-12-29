import pytest


def test_field():
    from pydantic import BaseModel, Field

    from sqla_fancy_core import TableFactory

    tf = TableFactory()

    def field(col, default=...):
        return col.info["field"](default)

    # Define a table
    class User:
        name = tf.string(
            "name", info={"field": lambda default: Field(default, max_length=5)}
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
