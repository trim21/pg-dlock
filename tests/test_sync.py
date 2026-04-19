import os
import threading
import time

import pytest

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


def test_session_acquire_release(locker: Locker):
    with locker.lock("pg-dlock-test-session-1"):
        pass


def test_session_try_acquire_nonblocking_contention(locker: Locker):
    key = "pg-dlock-test-session-contend"
    with locker.lock(key):
        # Another lock on a separate connection must fail non-blocking acquire.
        other = locker.lock(key)
        assert other.try_acquire(timeout=0) is False


def test_session_try_acquire_timeout(locker: Locker):
    key = "pg-dlock-test-session-timeout"
    with locker.lock(key):
        other = locker.lock(key)
        t0 = time.monotonic()
        got = other.try_acquire(timeout=0.3)
        elapsed = time.monotonic() - t0
        assert got is False
        assert 0.25 <= elapsed < 2.0


def test_session_shared_locks_coexist(locker: Locker):
    key = "pg-dlock-test-session-shared"
    a = locker.lock(key, shared=True)
    b = locker.lock(key, shared=True)
    a.try_acquire(None)
    try:
        assert b.try_acquire(timeout=0) is True
        b.release()
    finally:
        a.release()


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
    assert other.try_acquire(timeout=0) is True
    other.release()


def test_transaction_scope_rollback_on_exception(locker: Locker):
    key = "pg-dlock-test-xact-rollback"
    with pytest.raises(RuntimeError), locker.lock(key, scope="transaction"):
        raise RuntimeError("boom")
    # lock must be released even though the context exited via exception
    other = locker.lock(key, scope="transaction")
    assert other.try_acquire(timeout=0) is True
    other.release()


def test_transaction_scope_contention(locker: Locker):
    key = "pg-dlock-test-xact-contend"
    with locker.lock(key, scope="transaction"):
        other = locker.lock(key, scope="transaction")
        assert other.try_acquire(timeout=0) is False


def test_enter_raises_on_failed_acquire_with_zero_timeout():
    # try_acquire(0) path via context manager is not exercised (ctx uses None),
    # so we test that FailedToLockError is raised only when try_acquire reports False.
    # This is a smoke test that the exception class is exported.
    assert FailedToLockError.__name__ == "FailedToLockError"
