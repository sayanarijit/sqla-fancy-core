"""SQLAlchemy core, but fancier."""

import sqlalchemy as sa


class TableFactory:
    """A factory for creating SQLAlchemy columns with default values."""

    DEFAULTS = {}
    FK_DEFAULTS = {}

    def __init__(self, defaults: dict | None = None, fk_defaults: dict | None = None):
        """Initialize the factory with default values.

        Args:
            defaults (dict, optional): Instance scoped defaults for columns.
            fk_defaults (dict, optional): Instance scoped defaults for foreign keys.
        """
        self.c = []
        self.defaults = dict(self.DEFAULTS, **(defaults or {}))
        self.fk_defaults = dict(self.FK_DEFAULTS, **(fk_defaults or {}))

    def column(self, *args, **kwargs) -> sa.Column:
        kwargs = dict(self.defaults, **kwargs)
        col = sa.Column(*args, **kwargs)
        self.c.append(col)
        return col

    def integer(self, name: str, *args, **kwargs) -> sa.Column:
        return self.column(name, sa.Integer, *args, **kwargs)

    def string(self, name: str, *args, **kwargs) -> sa.Column:
        return self.column(name, sa.String, *args, **kwargs)

    def text(self, name: str, *args, **kwargs) -> sa.Column:
        return self.column(name, sa.Text, *args, **kwargs)

    def float(self, name: str, *args, **kwargs) -> sa.Column:
        return self.column(name, sa.Float, *args, **kwargs)

    def numeric(self, name: str, *args, **kwargs) -> sa.Column:
        return self.column(name, sa.Numeric, *args, **kwargs)

    def bigint(self, name: str, *args, **kwargs) -> sa.Column:
        return self.column(name, sa.BigInteger, *args, **kwargs)

    def smallint(self, name: str, *args, **kwargs) -> sa.Column:
        return self.column(name, sa.SmallInteger, *args, **kwargs)

    def timestamp(self, name: str, *args, **kwargs) -> sa.Column:
        return self.column(name, sa.TIMESTAMP, *args, **kwargs)

    def date(self, name: str, *args, **kwargs) -> sa.Column:
        return self.column(name, sa.Date, *args, **kwargs)

    def datetime(self, name: str, *args, **kwargs) -> sa.Column:
        return self.column(name, sa.DateTime, *args, **kwargs)

    def today(self, name: str, *args, **kwargs) -> sa.Column:
        return self.date(name, default=sa.func.now(), *args, **kwargs)

    def time(self, name: str, *args, **kwargs) -> sa.Column:
        return self.column(name, sa.Time, *args, **kwargs)

    def timenow(self, name: str, *args, **kwargs) -> sa.Column:
        return self.time(name, default=sa.func.now(), *args, **kwargs)

    def now(self, name: str, *args, **kwargs) -> sa.Column:
        return self.datetime(name, default=sa.func.now(), *args, **kwargs)

    def boolean(self, name: str, *args, **kwargs) -> sa.Column:
        return self.column(name, sa.Boolean, *args, **kwargs)

    def true(self, name: str, *args, **kwargs):
        return self.boolean(name, default=True, *args, **kwargs)

    def false(self, name: str, *args, **kwargs):
        return self.boolean(name, default=False, *args, **kwargs)

    def uuid(self, name: str, *args, **kwargs) -> sa.Column:
        return self.column(name, sa.UUID, *args, **kwargs)

    def foreign_key(self, name: str, ref: str | sa.Column, *args, **kwargs):
        fk = sa.ForeignKey(ref, **self.fk_defaults)
        return self.column(name, fk, *args, **kwargs)

    def enum(self, name: str, enum: type, *args, **kwargs) -> sa.Column:
        return self.column(name, sa.Enum(enum), *args, **kwargs)

    def json(self, name: str, *args, **kwargs) -> sa.Column:
        return self.column(name, sa.JSON, *args, **kwargs)

    def array(self, name: str, *args, **kwargs) -> sa.Column:
        return self.column(name, sa.ARRAY, *args, **kwargs)

    def array_int(self, name: str, *args, **kwargs) -> sa.Column:
        return self.array(name, sa.Integer, *args, **kwargs)

    def array_str(self, name: str, *args, **kwargs) -> sa.Column:
        return self.array(name, sa.String, *args, **kwargs)

    def array_text(self, name: str, *args, **kwargs) -> sa.Column:
        return self.array(name, sa.Text, *args, **kwargs)

    def array_float(self, name: str, *args, **kwargs) -> sa.Column:
        return self.array(name, sa.Float, *args, **kwargs)

    def array_numeric(self, name: str, *args, **kwargs) -> sa.Column:
        return self.array(name, sa.Numeric, *args, **kwargs)

    def array_bigint(self, name: str, *args, **kwargs) -> sa.Column:
        return self.array(name, sa.BigInteger, *args, **kwargs)

    def array_smallint(self, name: str, *args, **kwargs) -> sa.Column:
        return self.array(name, sa.SmallInteger, *args, **kwargs)

    def array_timestamp(self, name: str, *args, **kwargs) -> sa.Column:
        return self.array(name, sa.TIMESTAMP, *args, **kwargs)

    def array_date(self, name: str, *args, **kwargs) -> sa.Column:
        return self.array(name, sa.Date, *args, **kwargs)

    def array_datetime(self, name: str, *args, **kwargs) -> sa.Column:
        return self.array(name, sa.DateTime, *args, **kwargs)

    def array_time(self, name: str, *args, **kwargs) -> sa.Column:
        return self.array(name, sa.Time, *args, **kwargs)

    def array_boolean(self, name: str, *args, **kwargs) -> sa.Column:
        return self.array(name, sa.Boolean, *args, **kwargs)

    def array_uuid(self, name: str, *args, **kwargs) -> sa.Column:
        return self.array(name, sa.UUID, *args, **kwargs)

    def array_enum(self, name: str, enum: type, *args, **kwargs) -> sa.Column:
        return self.array(name, sa.Enum(enum), *args, **kwargs)

    def auto_id(self, name="id", *args, **kwargs) -> sa.Column:
        return self.integer(
            name, primary_key=True, index=True, autoincrement=True, *args, **kwargs
        )

    def auto_uuid(self, name="id", *args, **kwargs) -> sa.Column:
        return self.uuid(
            name,
            primary_key=True,
            index=True,
            default=sa.func.uuid_generate_v4(),
            *args,
            **kwargs
        )

    def updated_at(self, name="updated_at", *args, **kwargs) -> sa.Column:
        return self.datetime(
            name, default=sa.func.now(), onupdate=sa.func.now(), *args, **kwargs
        )

    def created_at(self, name="created_at", *args, **kwargs) -> sa.Column:
        return self.datetime(name, default=sa.func.now(), *args, **kwargs)

    def __call__(self, name, metadata, *args, **kwargs):
        return sa.Table(name, metadata, *args, *self.c, **kwargs)
