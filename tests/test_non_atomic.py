"""Tests for non_atomic() context manager and nax() method."""

import pytest
import sqlalchemy as sa

from sqla_fancy_core import TableBuilder, fancy
from sqla_fancy_core.wrappers import NotInTransactionError

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


def test_nax_without_context_creates_new_connection(fancy_engine):
    """Test that nax() creates a new connection when not in non_atomic context."""
    assert fancy_engine.nax(q_count).scalar_one() == 0
    # Insert without commit should not persist
    fancy_engine.nax(q_insert)
    # Since we're not in a transaction, changes are auto-committed
    assert fancy_engine.nax(q_count).scalar_one() == 0


def test_nax_inside_non_atomic_reuses_connection(fancy_engine):
    """Test that nax() reuses the same connection within non_atomic context."""
    assert fancy_engine.x(None, q_count).scalar_one() == 0

    with fancy_engine.non_atomic() as conn:
        # Multiple nax() calls share the same connection
        fancy_engine.nax(q_insert)
        fancy_engine.nax(q_insert)
        # The connection starts an autobegin transaction on first execute in SQLite
        assert conn.in_transaction() is True
        # Within the same connection/transaction, we can see the inserts
        assert fancy_engine.nax(q_count).scalar_one() == 2

    # No explicit commit, so changes are rolled back
    assert fancy_engine.x(None, q_count).scalar_one() == 0


def test_nested_non_atomic_reuses_same_connection(fancy_engine):
    """Test that nested non_atomic contexts reuse the same connection."""
    with fancy_engine.non_atomic() as conn1:
        with fancy_engine.non_atomic() as conn2:
            assert conn1 is conn2
            fancy_engine.nax(q_insert)
            # Within the same connection, we can see the insert
            assert fancy_engine.nax(q_count).scalar_one() == 1

    # No explicit commit, so nothing persists
    assert fancy_engine.x(None, q_count).scalar_one() == 0


def test_non_atomic_with_explicit_transaction(fancy_engine):
    """Test that we can start a transaction within non_atomic context."""
    assert fancy_engine.x(None, q_count).scalar_one() == 0

    with fancy_engine.non_atomic() as conn:
        with conn.begin():
            conn.execute(q_insert)
            assert conn.execute(q_count).scalar_one() == 1
        # After commit
        assert conn.execute(q_count).scalar_one() == 1

    assert fancy_engine.x(None, q_count).scalar_one() == 1


def test_x_uses_non_atomic_connection_when_inside(fancy_engine):
    """Test that x(None, ...) reuses the non_atomic connection when inside that context."""
    assert fancy_engine.x(None, q_count).scalar_one() == 0

    with fancy_engine.non_atomic() as conn:
        # x(None, ...) should reuse the non_atomic connection
        fancy_engine.x(None, q_insert)
        # Within the same connection, we can see the insert
        assert fancy_engine.x(None, q_count).scalar_one() == 1
        # SQLite starts an implicit transaction on first execute
        assert conn.in_transaction() is True

    # No explicit commit, so nothing persists
    assert fancy_engine.x(None, q_count).scalar_one() == 0


def test_tx_raises_error_on_non_transactional_connection(fancy_engine):
    """Test that tx() raises NotInTransactionError when connection is not in transaction."""
    with fancy_engine.non_atomic() as conn:
        # Execute a query first to trigger autobegin
        fancy_engine.nax(q_count)
        # Now we're in an implicit transaction
        assert conn.in_transaction() is True
        # Commit to end the transaction
        conn.commit()
        # Now we're not in a transaction
        assert conn.in_transaction() is False
        # tx() should raise error
        with pytest.raises(NotInTransactionError):
            fancy_engine.tx(conn, q_insert)


def test_tx_works_with_explicit_transaction_in_non_atomic(fancy_engine):
    """Test that tx() works when we explicitly start a transaction in non_atomic."""
    assert fancy_engine.x(None, q_count).scalar_one() == 0

    with fancy_engine.non_atomic() as conn:
        with conn.begin():
            assert conn.in_transaction() is True
            fancy_engine.tx(conn, q_insert)
            assert fancy_engine.tx(conn, q_count).scalar_one() == 1
        # After commit
        assert conn.execute(q_count).scalar_one() == 1

    assert fancy_engine.x(None, q_count).scalar_one() == 1


def test_non_atomic_rollback_reverts_changes(fancy_engine):
    """Test that rollback in non_atomic reverts uncommitted changes."""
    assert fancy_engine.x(None, q_count).scalar_one() == 0

    with fancy_engine.non_atomic() as conn:
        fancy_engine.nax(q_insert)
        assert fancy_engine.nax(q_count).scalar_one() == 1
        conn.rollback()  # Rollback the implicit transaction
        # After rollback, count should be 0
        assert fancy_engine.nax(q_count).scalar_one() == 0

    assert fancy_engine.x(None, q_count).scalar_one() == 0


def test_non_atomic_commit_persists_changes(fancy_engine):
    """Test that commit in non_atomic persists changes."""
    assert fancy_engine.x(None, q_count).scalar_one() == 0

    with fancy_engine.non_atomic() as conn:
        fancy_engine.nax(q_insert)
        conn.commit()  # Commit the implicit transaction
        # After commit, the insert is visible
        assert fancy_engine.nax(q_count).scalar_one() == 1

    # After commit, the change persists
    assert fancy_engine.x(None, q_count).scalar_one() == 1


def test_multiple_nax_calls_without_context(fancy_engine):
    """Test that multiple nax() calls outside context each create new connections."""
    assert fancy_engine.nax(q_count).scalar_one() == 0
    fancy_engine.nax(q_insert)
    # Without commit, nothing persists
    assert fancy_engine.nax(q_count).scalar_one() == 0
    fancy_engine.nax(q_insert)
    assert fancy_engine.nax(q_count).scalar_one() == 0


def test_non_atomic_and_atomic_dont_interfere(fancy_engine):
    """Test that non_atomic and atomic contexts don't interfere with each other."""
    assert fancy_engine.x(None, q_count).scalar_one() == 0
    
    # Use atomic to commit one insert
    with fancy_engine.atomic():
        fancy_engine.ax(q_insert)
    
    assert fancy_engine.x(None, q_count).scalar_one() == 1
    
    # Use non_atomic without commit - shouldn't persist
    with fancy_engine.non_atomic():
        fancy_engine.nax(q_insert)
        assert fancy_engine.nax(q_count).scalar_one() == 2
    
    # Only the atomic insert should persist
    assert fancy_engine.x(None, q_count).scalar_one() == 1


def test_deeply_nested_non_atomic(fancy_engine):
    """Test that deeply nested non_atomic contexts all share the same connection."""
    with fancy_engine.non_atomic() as conn1:
        fancy_engine.nax(q_insert)
        with fancy_engine.non_atomic() as conn2:
            assert conn1 is conn2
            fancy_engine.nax(q_insert)
            with fancy_engine.non_atomic() as conn3:
                assert conn1 is conn3
                fancy_engine.nax(q_insert)
                assert fancy_engine.nax(q_count).scalar_one() == 3
    
    # Nothing committed
    assert fancy_engine.x(None, q_count).scalar_one() == 0

