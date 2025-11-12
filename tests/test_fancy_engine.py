import pytest
import sqlalchemy as sa

from sqla_fancy_core import TableBuilder, fancy

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


def test_insert(fancy_engine):
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
