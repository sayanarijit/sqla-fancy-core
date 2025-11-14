import pytest
import sqlalchemy as sa

from sqla_fancy_core import TableBuilder, fancy
from sqla_fancy_core.errors import AtomicContextError, NotInTransactionError

tb = TableBuilder()


class Counter:
    id = tb.auto_id()
    Table = tb("counter")


q_insert = sa.insert(Counter.Table)
q_count = sa.select(sa.func.count()).select_from(Counter.Table)


@pytest.fixture
def fancy_engine():
    eng = fancy(sa.create_engine("sqlite:///:memory:"))
    tb.metadata.create_all(eng.engine)
    try:
        yield eng
    finally:
        tb.metadata.drop_all(eng.engine)
        eng.engine.dispose()


@pytest.fixture
def fancy_engine_postgres():
    eng = fancy(sa.create_engine("postgresql+psycopg2://test:test@localhost/test"))
    tb.metadata.create_all(eng.engine)
    try:
        yield eng
    finally:
        tb.metadata.drop_all(eng.engine)
        eng.engine.dispose()


def test_ax_raises_outside_atomic(fancy_engine):
    with pytest.raises(AtomicContextError):
        fancy_engine.ax(q_count)


def test_ax_inside_atomic_commits_on_exit(fancy_engine):
    assert fancy_engine.x(None, q_count).scalar_one() == 0
    with fancy_engine.atomic() as conn:
        # multiple ax() calls share the same connection
        fancy_engine.ax(q_insert)
        fancy_engine.ax(q_insert)
        # visibility within the same transaction
        assert fancy_engine.ax(q_count).scalar_one() == 2
        assert conn.in_transaction() is True
    # committed after context exit
    assert fancy_engine.x(None, q_count).scalar_one() == 2


def test_nested_atomic_reuses_same_connection(fancy_engine):
    with fancy_engine.atomic() as conn1:
        with fancy_engine.atomic() as conn2:
            assert conn1 is conn2
            fancy_engine.ax(q_insert)
            assert fancy_engine.ax(q_count).scalar_one() == 1
    assert fancy_engine.x(None, q_count).scalar_one() == 1


def test_tx_uses_atomic_connection_when_inside(fancy_engine):
    assert fancy_engine.x(None, q_count).scalar_one() == 0
    with fancy_engine.atomic() as conn:
        # tx(None, ...) should reuse the atomic connection/transaction
        fancy_engine.tx(None, q_insert)
        # The outer connection should see the uncommitted row
        assert fancy_engine.tx(conn, q_count).scalar_one() == 1
        assert conn.in_transaction() is True
    # committed at outer context exit
    assert fancy_engine.x(None, q_count).scalar_one() == 1


def test_atomic_rollback_on_exception(fancy_engine):
    assert fancy_engine.x(None, q_count).scalar_one() == 0
    with pytest.raises(RuntimeError):
        with fancy_engine.atomic():
            fancy_engine.ax(q_insert)
            assert fancy_engine.ax(q_count).scalar_one() == 1
            raise RuntimeError("boom")
    # rolled back
    assert fancy_engine.x(None, q_count).scalar_one() == 0


def test_atx_outside_atomic_commits(fancy_engine):
    assert fancy_engine.x(None, q_count).scalar_one() == 0
    fancy_engine.atx(q_insert)
    assert fancy_engine.x(None, q_count).scalar_one() == 1


def test_atx_inside_atomic_reuses_same_connection(fancy_engine):
    with fancy_engine.atomic() as conn:
        fancy_engine.atx(q_insert)
        # Same transactional visibility within the atomic connection
        assert fancy_engine.ax(q_count).scalar_one() == 1
        assert conn.in_transaction() is True
    assert fancy_engine.x(None, q_count).scalar_one() == 1


def test_multiple_atx_calls_outside_atomic(fancy_engine):
    """Test that multiple atx() calls outside atomic each create and commit their own transactions."""
    assert fancy_engine.x(None, q_count).scalar_one() == 0
    fancy_engine.atx(q_insert)
    assert fancy_engine.x(None, q_count).scalar_one() == 1
    fancy_engine.atx(q_insert)
    assert fancy_engine.x(None, q_count).scalar_one() == 2
    fancy_engine.atx(q_insert)
    assert fancy_engine.x(None, q_count).scalar_one() == 3


def test_atomic_isolation_from_other_connections(fancy_engine_postgres):
    """Test that changes inside atomic are not visible to other connections until committed."""
    assert fancy_engine_postgres.x(None, q_count).scalar_one() == 0

    with fancy_engine_postgres.atomic():
        fancy_engine_postgres.ax(q_insert)
        assert fancy_engine_postgres.ax(q_count).scalar_one() == 1
        # A new connection outside the atomic context shouldn't see the uncommitted insert
        # Create an explicit new connection to test isolation
        with fancy_engine_postgres.engine.connect() as new_conn:
            assert fancy_engine_postgres.x(new_conn, q_count).scalar_one() == 0

    # After commit, new connections should see it
    assert fancy_engine_postgres.x(None, q_count).scalar_one() == 1


def test_nested_atomic_commits_at_outermost_level(fancy_engine):
    """Test that nested atomic contexts only commit when the outermost context exits."""
    assert fancy_engine.x(None, q_count).scalar_one() == 0

    with fancy_engine.atomic():
        fancy_engine.ax(q_insert)
        with fancy_engine.atomic():
            fancy_engine.ax(q_insert)
            assert fancy_engine.ax(q_count).scalar_one() == 2
            # Still in transaction
        # Inner context exited, but still in outer transaction
        fancy_engine.ax(q_insert)
        assert fancy_engine.ax(q_count).scalar_one() == 3

    # Now committed
    assert fancy_engine.x(None, q_count).scalar_one() == 3


def test_atomic_with_explicit_rollback_raises_exception(fancy_engine):
    """Test that explicitly calling rollback in atomic context still allows exception to propagate."""
    assert fancy_engine.x(None, q_count).scalar_one() == 0

    with pytest.raises(RuntimeError):
        with fancy_engine.atomic() as conn:
            fancy_engine.ax(q_insert)
            conn.rollback()
            raise RuntimeError("explicit rollback then error")

    assert fancy_engine.x(None, q_count).scalar_one() == 0


def test_ax_raises_not_in_transaction_after_commit(fancy_engine):
    """Test that ax() raises NotInTransactionError when connection is not in transaction."""
    assert fancy_engine.x(None, q_count).scalar_one() == 0

    with fancy_engine.atomic() as conn:
        fancy_engine.ax(q_insert)
        assert fancy_engine.ax(q_count).scalar_one() == 1
        # Manually commit the transaction
        conn.commit()
        # Now ax() should raise NotInTransactionError
        with pytest.raises(NotInTransactionError):
            fancy_engine.ax(q_insert)


def test_atx_raises_not_in_transaction_after_commit(fancy_engine):
    """Test that atx() raises NotInTransactionError when connection is not in transaction."""
    assert fancy_engine.x(None, q_count).scalar_one() == 0

    with fancy_engine.atomic() as conn:
        fancy_engine.atx(q_insert)
        assert fancy_engine.atx(q_count).scalar_one() == 1
        # Manually commit the transaction
        conn.commit()
        # Now atx() should raise NotInTransactionError
        with pytest.raises(NotInTransactionError):
            fancy_engine.atx(q_insert)


def test_tx_raises_not_in_transaction_with_committed_connection(fancy_engine):
    """Test that tx() raises NotInTransactionError when passed a non-transactional connection."""
    # Create a connection without a transaction
    with fancy_engine.engine.connect() as conn:
        # Connection exists but is not in a transaction
        with pytest.raises(NotInTransactionError):
            fancy_engine.tx(conn, q_insert)


def test_ax_works_after_nested_atomic_with_same_connection(fancy_engine):
    """Test that ax() continues to work in nested atomic contexts using the same connection."""
    assert fancy_engine.x(None, q_count).scalar_one() == 0

    with fancy_engine.atomic() as conn1:
        fancy_engine.ax(q_insert)
        assert conn1.in_transaction() is True
        
        with fancy_engine.atomic() as conn2:
            # Same connection should be reused
            assert conn1 is conn2
            assert conn2.in_transaction() is True
            fancy_engine.ax(q_insert)
            assert fancy_engine.ax(q_count).scalar_one() == 2
        
        # Still in transaction after nested context
        assert conn1.in_transaction() is True
        fancy_engine.ax(q_insert)
        assert fancy_engine.ax(q_count).scalar_one() == 3

    # All committed
    assert fancy_engine.x(None, q_count).scalar_one() == 3


def test_atomic_rejects_ax_after_rollback(fancy_engine):
    """Test that ax() raises NotInTransactionError after explicit rollback."""
    assert fancy_engine.x(None, q_count).scalar_one() == 0

    with fancy_engine.atomic() as conn:
        fancy_engine.ax(q_insert)
        assert fancy_engine.ax(q_count).scalar_one() == 1
        # Explicitly rollback
        conn.rollback()
        # Connection is no longer in transaction
        with pytest.raises(NotInTransactionError):
            fancy_engine.ax(q_insert)
