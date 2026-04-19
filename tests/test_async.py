import asyncio
import os

import pytest

from pg_dlock import AsyncLocker

DSN = os.environ.get("PG_DLOCK_TEST_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="PG_DLOCK_TEST_DSN not set")


@pytest.mark.asyncio
async def test_async_session_acquire_release():
    assert DSN is not None
    async with AsyncLocker(DSN) as locker:
        lock = await locker.lock("pg-dlock-async-1")
        async with lock:
            pass


@pytest.mark.asyncio
async def test_async_session_nonblocking_contention():
    assert DSN is not None
    async with AsyncLocker(DSN) as locker:
        key = "pg-dlock-async-contend"
        a = await locker.lock(key)
        b = await locker.lock(key)
        async with a:
            assert await b.try_acquire(timeout=0) is False


@pytest.mark.asyncio
async def test_async_transaction_auto_release():
    assert DSN is not None
    async with AsyncLocker(DSN) as locker:
        key = "pg-dlock-async-xact"
        lock = await locker.lock(key, scope="transaction")
        async with lock:
            pass
        other = await locker.lock(key, scope="transaction")
        assert await other.try_acquire(timeout=0) is True
        await other.release()


@pytest.mark.asyncio
async def test_async_transaction_rollback_on_exception():
    assert DSN is not None
    async with AsyncLocker(DSN) as locker:
        key = "pg-dlock-async-xact-rollback"
        lock = await locker.lock(key, scope="transaction")
        with pytest.raises(RuntimeError):
            async with lock:
                raise RuntimeError("boom")
        other = await locker.lock(key, scope="transaction")
        assert await other.try_acquire(timeout=0) is True
        await other.release()


# Ensure top-level name works without running an event loop
def test_async_locker_importable():
    assert AsyncLocker is not None


_ = asyncio  # keep import
