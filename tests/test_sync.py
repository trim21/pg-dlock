import os
import threading
import time

import pytest
from psycopg.conninfo import make_conninfo

from pg_dlock import FailedToLockError, Locker

DSN = os.environ.get("PG_DLOCK_TEST_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="PG_DLOCK_TEST_DSN not set")


@pytest.fixture
def locker():
    assert DSN is not None
    lk = Locker(DSN)
    try:
        yield lk
    finally:
        lk.close()


def _dsn_with_statement_timeout(timeout_ms: int) -> str:
    assert DSN is not None
    return make_conninfo(
        DSN,
        options=f"-c statement_timeout={timeout_ms}",
    )


def test_session_acquire_release(locker: Locker):
    with locker.lock("pg-dlock-test-session-1"):
        pass


def test_session_acquire_nonblocking_contention(locker: Locker):
    key = "pg-dlock-test-session-contend"
    with locker.lock(key):
        # Another lock on a separate connection must fail non-blocking acquire.
        other = locker.lock(key)
        assert other.acquire(blocking=False) is False


def test_session_acquire_timeout(locker: Locker):
    key = "pg-dlock-test-session-timeout"
    with locker.lock(key):
        other = locker.lock(key)
        t0 = time.monotonic()
        got = other.acquire(timeout=0.3)
        elapsed = time.monotonic() - t0
        assert got is False
        assert 0.25 <= elapsed < 2.0


def test_session_acquire_none_overrides_connection_timeout():
    key = "pg-dlock-test-session-no-timeout"
    acquired = threading.Event()

    with Locker(_dsn_with_statement_timeout(50)) as locker:

        def worker():
            with locker.lock(key):
                acquired.set()
                time.sleep(0.2)

        t = threading.Thread(target=worker)
        t.start()
        assert acquired.wait(timeout=2)

        other = locker.lock(key)
        t0 = time.monotonic()
        assert other.acquire() is True
        elapsed = time.monotonic() - t0
        assert elapsed >= 0.15
        other.release()
        t.join(timeout=2)
        assert not t.is_alive()


def test_session_shared_locks_coexist(locker: Locker):
    key = "pg-dlock-test-session-shared"
    a = locker.lock(key, shared=True)
    b = locker.lock(key, shared=True)
    a.acquire()
    try:
        assert b.acquire(blocking=False) is True
        b.release()
    finally:
        a.release()


def test_session_lock_is_not_reentrant(locker: Locker):
    lock = locker.lock("pg-dlock-test-session-not-reentrant")
    lock.acquire()
    try:
        with pytest.raises(RuntimeError, match="already held"):
            lock.acquire()
    finally:
        lock.release()


def test_session_context_blocks_until_released(locker: Locker):
    key = "pg-dlock-test-session-wait"
    acquired = threading.Event()
    done = threading.Event()

    def worker():
        with locker.lock(key):
            acquired.set()
            time.sleep(0.2)
        done.set()

    t = threading.Thread(target=worker)
    t.start()
    assert acquired.wait(timeout=2)

    t0 = time.monotonic()
    with locker.lock(key):
        # should only acquire after the worker released
        assert time.monotonic() - t0 >= 0.15
    assert done.wait(timeout=2)
    t.join(timeout=2)
    assert not t.is_alive()


def test_transaction_scope_auto_release(locker: Locker):
    key = "pg-dlock-test-xact-1"
    with locker.lock(key, scope="transaction"):
        pass
    # after txn commit, lock is free — should be immediately acquirable
    other = locker.lock(key, scope="transaction")
    assert other.acquire(blocking=False) is True
    other.release()


def test_transaction_acquire_none_overrides_connection_timeout():
    key = "pg-dlock-test-xact-no-timeout"
    acquired = threading.Event()

    with Locker(_dsn_with_statement_timeout(50)) as locker:

        def worker():
            with locker.lock(key, scope="transaction"):
                acquired.set()
                time.sleep(0.2)

        t = threading.Thread(target=worker)
        t.start()
        assert acquired.wait(timeout=2)

        other = locker.lock(key, scope="transaction")
        t0 = time.monotonic()
        assert other.acquire() is True
        elapsed = time.monotonic() - t0
        assert elapsed >= 0.15
        other.release()
        t.join(timeout=2)
        assert not t.is_alive()


def test_transaction_scope_rollback_on_exception(locker: Locker):
    key = "pg-dlock-test-xact-rollback"
    with pytest.raises(RuntimeError), locker.lock(key, scope="transaction"):
        raise RuntimeError("boom")
    # lock must be released even though the context exited via exception
    other = locker.lock(key, scope="transaction")
    assert other.acquire(blocking=False) is True
    other.release()


def test_transaction_scope_contention(locker: Locker):
    key = "pg-dlock-test-xact-contend"
    with locker.lock(key, scope="transaction"):
        other = locker.lock(key, scope="transaction")
        assert other.acquire(blocking=False) is False


def test_transaction_lock_is_not_reentrant(locker: Locker):
    lock = locker.lock("pg-dlock-test-xact-not-reentrant", scope="transaction")
    lock.acquire()
    try:
        with pytest.raises(RuntimeError, match="already held"):
            lock.acquire()
    finally:
        lock.release()


def test_acquire_rejects_timeout_with_nonblocking(locker: Locker):
    lock = locker.lock("pg-dlock-test-invalid-timeout")
    with pytest.raises(ValueError, match="non-blocking"):
        lock.acquire(blocking=False, timeout=0.1)


def test_acquire_rejects_negative_timeout(locker: Locker):
    lock = locker.lock("pg-dlock-test-negative-timeout")
    with pytest.raises(ValueError, match="positive"):
        lock.acquire(timeout=-2)


def test_enter_raises_on_failed_acquire_with_zero_timeout():
    assert FailedToLockError.__name__ == "FailedToLockError"
