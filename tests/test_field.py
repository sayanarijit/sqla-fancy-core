from typing import Optional

import pytest


def test_field():
    from typing import Any

    import sqlalchemy as sa
    from pydantic import BaseModel, Field

    from sqla_fancy_core import TableBuilder

    tb = TableBuilder()

    def field(col, default: Any = ...) -> Field:
        return col.info["kwargs"]["field"](default)

    # Define a table
    class User:
        name = tb(
            sa.Column("name", sa.String),
            field=lambda default: Field(default, max_length=5),
        )
        Table = tb("author")

    # Define a pydantic schema
    class CreateUser(BaseModel):
        name: str = field(User.name)

    # Define a pydantic schema
    class UpdateUser(BaseModel):
        name: Optional[str] = field(User.name, None)

    assert CreateUser(name="John").model_dump() == {"name": "John"}
    assert UpdateUser(name="John").model_dump() == {"name": "John"}
    assert UpdateUser().model_dump(exclude_unset=True) == {}

    with pytest.raises(ValueError):
        CreateUser()
    with pytest.raises(ValueError):
        UpdateUser(name="John Doe")
