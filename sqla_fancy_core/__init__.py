"""SQLAlchemy core, but fancier."""

from sqla_fancy_core.factories import TableFactory  # noqa
from sqla_fancy_core.wrappers import FancyEngineWrapper, AsyncFancyEngineWrapper, fancy  # noqa
from sqla_fancy_core.decorators import transact, Inject  # noqa
