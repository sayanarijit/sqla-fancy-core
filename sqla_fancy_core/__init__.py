"""SQLAlchemy core, but fancier."""

"""SQLAlchemy core, but fancier."""

from sqla_fancy_core.builders import TableBuilder  # noqa
from sqla_fancy_core.wrappers import (  # noqa
    FancyEngineWrapper,
    AsyncFancyEngineWrapper,
    fancy,
)
from sqla_fancy_core.decorators import transact, Inject  # noqa
