"""Tests for non_atomic() context manager and nax() method."""

import pytest
import sqlalchemy as sa

from sqla_fancy_core import TableBuilder, fancy
from sqla_fancy_core.errors import AtomicInsideNonAtomicError, NotInTransactionError

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


def test_atomic_inside_non_atomic_requires_transaction(fancy_engine):
    """Test that atomic() inside non_atomic() requires an active transaction."""
    assert fancy_engine.x(None, q_count).scalar_one() == 0

    # Attempting to use atomic() inside non_atomic() without a transaction should fail
    with pytest.raises(AtomicInsideNonAtomicError):
        with fancy_engine.non_atomic():
            with fancy_engine.atomic():
                pass

    # But it works if we start an explicit transaction first
    with fancy_engine.non_atomic() as conn1:
        with conn1.begin():
            # Now atomic() should reuse the same connection
            with fancy_engine.atomic() as conn2:
                assert conn1 is conn2
                assert conn2.in_transaction() is True
                fancy_engine.ax(q_insert)
                assert fancy_engine.ax(q_count).scalar_one() == 1
            # After atomic exits, still in non_atomic's explicit transaction
            assert conn1.in_transaction() is True

    # The explicit transaction commit should have persisted the change
    assert fancy_engine.x(None, q_count).scalar_one() == 1


def test_non_atomic_inside_atomic_reuses_connection(fancy_engine):
    """Test that non_atomic() inside atomic() reuses the same connection."""
    assert fancy_engine.x(None, q_count).scalar_one() == 0

    with fancy_engine.atomic() as conn1:
        assert conn1.in_transaction() is True
        # Start non_atomic context - should reuse the same connection
        with fancy_engine.non_atomic() as conn2:
            assert conn1 is conn2
            assert conn2.in_transaction() is True
            fancy_engine.nax(q_insert)
            assert fancy_engine.nax(q_count).scalar_one() == 1
        # Still in transaction after non_atomic exits
        assert conn1.in_transaction() is True
        fancy_engine.ax(q_insert)
        assert fancy_engine.ax(q_count).scalar_one() == 2

    # Both inserts should be committed
    assert fancy_engine.x(None, q_count).scalar_one() == 2


def test_nax_works_in_atomic_context(fancy_engine):
    """Test that nax() works correctly within atomic() context."""
    assert fancy_engine.x(None, q_count).scalar_one() == 0

    with fancy_engine.atomic():
        fancy_engine.nax(q_insert)
        fancy_engine.ax(q_insert)
        # Both should be visible in the same transaction
        assert fancy_engine.nax(q_count).scalar_one() == 2

    # Both should be committed
    assert fancy_engine.x(None, q_count).scalar_one() == 2


def test_ax_fails_in_non_atomic_without_explicit_transaction(fancy_engine):
    """Test that ax() raises error in non_atomic() without explicit transaction started."""
    from sqla_fancy_core.errors import AtomicContextError

    # ax() outside any context should raise AtomicContextError
    with pytest.raises(AtomicContextError):
        fancy_engine.ax(q_insert)

    # ax() in non_atomic without transaction raises NotInTransactionError
    # because the connection exists but is not in a transaction
    with pytest.raises(NotInTransactionError):
        with fancy_engine.non_atomic():
            fancy_engine.ax(q_insert)


def test_mixed_nesting_atomic_non_atomic_atomic(fancy_engine):
    """Test complex nesting: atomic -> non_atomic -> atomic."""
    assert fancy_engine.x(None, q_count).scalar_one() == 0

    with fancy_engine.atomic() as conn1:
        fancy_engine.ax(q_insert)
        assert fancy_engine.ax(q_count).scalar_one() == 1

        with fancy_engine.non_atomic() as conn2:
            assert conn1 is conn2
            fancy_engine.nax(q_insert)
            assert fancy_engine.nax(q_count).scalar_one() == 2

            with fancy_engine.atomic() as conn3:
                assert conn1 is conn3
                fancy_engine.ax(q_insert)
                assert fancy_engine.ax(q_count).scalar_one() == 3

            assert fancy_engine.nax(q_count).scalar_one() == 3

        assert fancy_engine.ax(q_count).scalar_one() == 3

    # All should be committed
    assert fancy_engine.x(None, q_count).scalar_one() == 3


def test_rollback_in_non_atomic_inside_atomic_affects_all(fancy_engine):
    """Test that rollback in nested non_atomic affects the outer atomic transaction."""
    assert fancy_engine.x(None, q_count).scalar_one() == 0

    with fancy_engine.atomic() as conn1:
        fancy_engine.ax(q_insert)
        assert fancy_engine.ax(q_count).scalar_one() == 1

        with fancy_engine.non_atomic() as conn2:
            assert conn1 is conn2
            fancy_engine.nax(q_insert)
            assert fancy_engine.nax(q_count).scalar_one() == 2
            # Rollback should affect the entire transaction
            conn2.rollback()
            # After rollback, connection is not in transaction
            assert conn2.in_transaction() is False

        # After rollback, ax should fail because not in transaction
        with pytest.raises(NotInTransactionError):
            fancy_engine.ax(q_insert)

    # Nothing should be committed due to rollback
    assert fancy_engine.x(None, q_count).scalar_one() == 0


def test_commit_in_non_atomic_inside_atomic_commits_early(fancy_engine):
    """Test that commit in nested non_atomic commits the outer atomic transaction."""
    assert fancy_engine.x(None, q_count).scalar_one() == 0

    with fancy_engine.atomic() as conn1:
        fancy_engine.ax(q_insert)
        assert fancy_engine.ax(q_count).scalar_one() == 1

        with fancy_engine.non_atomic() as conn2:
            assert conn1 is conn2
            fancy_engine.nax(q_insert)
            assert fancy_engine.nax(q_count).scalar_one() == 2
            # Explicit commit
            conn2.commit()
            # After commit, connection is not in transaction
            assert conn2.in_transaction() is False

        # After commit, ax should fail because not in transaction
        with pytest.raises(NotInTransactionError):
            fancy_engine.ax(q_insert)

    # The committed changes should persist
    assert fancy_engine.x(None, q_count).scalar_one() == 2
