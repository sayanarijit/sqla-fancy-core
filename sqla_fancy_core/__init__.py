"""SQLAlchemy core, but fancier."""

"""SQLAlchemy core, but fancier."""

# Prefer TableBuilder; keep TableFactory for backward compatibility via factories shim
from sqla_fancy_core.builders import TableBuilder  # noqa
from sqla_fancy_core.wrappers import (  # noqa
    FancyEngineWrapper,
    AsyncFancyEngineWrapper,
    fancy,
    FancyError,
    AtomicContextError,
)
from sqla_fancy_core.decorators import transact, Inject  # noqa
from sqla_fancy_core.wrappers import (  # noqa
    FancyEngineWrapper,
    AsyncFancyEngineWrapper,
    fancy,
    FancyError,
    AtomicContextError,
)
from sqla_fancy_core.decorators import transact, Inject  # noqa
