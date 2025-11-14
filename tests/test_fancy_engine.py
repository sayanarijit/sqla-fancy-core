import pytest
import sqlalchemy as sa

from sqla_fancy_core import TableBuilder, fancy
from sqla_fancy_core.errors import NotInTransactionError

tb = TableBuilder()


class Counter:
    id = tb.auto_id()
    Table = tb("counter")


q_insert = sa.insert(Counter.Table)
q_count = sa.select(sa.func.count()).select_from(Counter.Table)


@pytest.fixture
def fancy_engine():
    fancy_engine = fancy(sa.create_engine("sqlite:///:memory:"))
    tb.metadata.create_all(fancy_engine.engine)
    yield fancy_engine
    tb.metadata.drop_all(fancy_engine.engine)
    fancy_engine.engine.dispose()


def test_tx_auto_commits_without_explicit_connection(fancy_engine):
    """Test that tx(None, ...) auto-commits when no connection is provided."""
    assert fancy_engine.x(None, q_count).scalar_one() == 0
    fancy_engine.tx(None, q_insert)
    assert fancy_engine.x(None, q_count).scalar_one() == 1
    assert fancy_engine.tx(None, q_count).scalar_one() == 1


def test_no_commit_default_rollback(fancy_engine):
    assert fancy_engine.x(None, q_count).scalar_one() == 0
    fancy_engine.x(None, q_insert)
    assert fancy_engine.x(None, q_count).scalar_one() == 0

    with fancy_engine.engine.connect() as conn:
        assert fancy_engine.x(conn, q_count).scalar_one() == 0
        fancy_engine.x(conn, q_insert)
        assert fancy_engine.x(conn, q_count).scalar_one() == 1
    assert fancy_engine.x(None, q_count).scalar_one() == 0


def test_explicit_commit(fancy_engine):
    with fancy_engine.engine.connect() as conn:
        assert fancy_engine.x(conn, q_count).scalar_one() == 0
        fancy_engine.x(conn, q_insert)
        assert fancy_engine.x(conn, q_count).scalar_one() == 1
        conn.commit()
        assert fancy_engine.x(conn, q_count).scalar_one() == 1
    assert fancy_engine.x(None, q_count).scalar_one() == 1


def test_explicit_rollback(fancy_engine):
    with fancy_engine.engine.connect() as conn:
        assert fancy_engine.x(conn, q_count).scalar_one() == 0
        fancy_engine.x(conn, q_insert)
        assert fancy_engine.x(conn, q_count).scalar_one() == 1
        conn.rollback()
        assert fancy_engine.x(conn, q_count).scalar_one() == 0
    assert fancy_engine.x(None, q_count).scalar_one() == 0


def test_transaction_context_manager_commit(fancy_engine):
    assert fancy_engine.x(None, q_count).scalar_one() == 0
    with fancy_engine.engine.begin() as txn:
        fancy_engine.tx(txn, q_insert)
        assert fancy_engine.tx(txn, q_count).scalar_one() == 1
        assert fancy_engine.tx(txn, q_count).scalar_one() == 1
    assert fancy_engine.x(None, q_count).scalar_one() == 1
    assert fancy_engine.tx(None, q_count).scalar_one() == 1


def test_transaction_context_manager_rollback(fancy_engine):
    assert fancy_engine.x(None, q_count).scalar_one() == 0
    try:
        with fancy_engine.engine.begin() as txn:
            assert fancy_engine.tx(txn, q_count).scalar_one() == 0
            fancy_engine.tx(txn, q_insert)
            assert fancy_engine.tx(txn, q_count).scalar_one() == 1
            raise Exception("Trigger rollback")
    except Exception:
        pass
    assert fancy_engine.x(None, q_count).scalar_one() == 0
    assert fancy_engine.tx(None, q_count).scalar_one() == 0


def test_x_tx_order_in_begin_1(fancy_engine):
    """Test that x() and tx() can be used interchangeably and maintain transaction state."""
    assert fancy_engine.x(None, q_count).scalar_one() == 0

    with fancy_engine.engine.begin() as conn:
        fancy_engine.tx(conn, q_insert)
        fancy_engine.x(conn, q_insert)
        assert fancy_engine.x(conn, q_count).scalar_one() == 2

    assert fancy_engine.x(None, q_count).scalar_one() == 2


def test_x_tx_order_in_begin_2(fancy_engine):
    """Test that tx() and x() can be used interchangeably and maintain transaction state."""
    assert fancy_engine.x(None, q_count).scalar_one() == 0

    with fancy_engine.engine.begin() as conn:
        fancy_engine.x(conn, q_insert)
        fancy_engine.tx(conn, q_insert)
        assert fancy_engine.x(conn, q_count).scalar_one() == 2

    assert fancy_engine.x(None, q_count).scalar_one() == 2


def test_tx_raises_error_on_non_transactional_connection(fancy_engine):
    """Test that tx() raises NotInTransactionError when connection is not in a transaction."""
    with fancy_engine.engine.connect() as conn:
        # Connection exists but is not in a transaction
        assert conn.in_transaction() is False
        with pytest.raises(NotInTransactionError):
            fancy_engine.tx(conn, q_insert)


def test_x_with_none_creates_new_connection_each_time(fancy_engine):
    """Test that x(None, ...) creates a new connection for each call."""
    assert fancy_engine.x(None, q_count).scalar_one() == 0
    fancy_engine.x(None, q_insert)
    # Without commit, nothing should persist
    assert fancy_engine.x(None, q_count).scalar_one() == 0


def test_multiple_tx_calls_with_none(fancy_engine):
    """Test that tx(None, ...) auto-commits each time."""
    assert fancy_engine.x(None, q_count).scalar_one() == 0
    fancy_engine.tx(None, q_insert)
    assert fancy_engine.x(None, q_count).scalar_one() == 1
    fancy_engine.tx(None, q_insert)
    assert fancy_engine.x(None, q_count).scalar_one() == 2
    fancy_engine.tx(None, q_insert)
    assert fancy_engine.x(None, q_count).scalar_one() == 3


def test_x_and_tx_with_explicit_connection_see_same_state(fancy_engine):
    """Test that x() and tx() with the same connection see the same uncommitted state."""
    with fancy_engine.engine.begin() as conn:
        fancy_engine.tx(conn, q_insert)
        # x() with same connection should see uncommitted insert
        assert fancy_engine.x(conn, q_count).scalar_one() == 1
        fancy_engine.x(conn, q_insert)
        # tx() with same connection should see both inserts
        assert fancy_engine.tx(conn, q_count).scalar_one() == 2

    # Both should be committed
    assert fancy_engine.x(None, q_count).scalar_one() == 2
