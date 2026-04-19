import asyncio
import os

import pytest
from psycopg.conninfo import make_conninfo

from pg_dlock import AsyncLocker

DSN = os.environ.get("PG_DLOCK_TEST_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="PG_DLOCK_TEST_DSN not set")


def _dsn_with_statement_timeout(timeout_ms: int) -> str:
    assert DSN is not None
    return make_conninfo(
        DSN,
        options=f"-c statement_timeout={timeout_ms}",
    )


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
async def test_async_session_none_timeout_overrides_connection_timeout():
    async with AsyncLocker(_dsn_with_statement_timeout(50)) as locker:
        key = "pg-dlock-async-no-timeout"
        acquired = asyncio.Event()

        async def worker() -> None:
            lock = await locker.lock(key)
            async with lock:
                acquired.set()
                await asyncio.sleep(0.2)

        task = asyncio.create_task(worker())
        await asyncio.wait_for(acquired.wait(), timeout=2)

        other = await locker.lock(key)
        t0 = asyncio.get_running_loop().time()
        assert await other.try_acquire(timeout=None) is True
        elapsed = asyncio.get_running_loop().time() - t0
        assert elapsed >= 0.15
        await other.release()
        await asyncio.wait_for(task, timeout=2)


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
async def test_async_transaction_none_timeout_overrides_connection_timeout():
    async with AsyncLocker(_dsn_with_statement_timeout(50)) as locker:
        key = "pg-dlock-async-xact-no-timeout"
        acquired = asyncio.Event()

        async def worker() -> None:
            lock = await locker.lock(key, scope="transaction")
            async with lock:
                acquired.set()
                await asyncio.sleep(0.2)

        task = asyncio.create_task(worker())
        await asyncio.wait_for(acquired.wait(), timeout=2)

        other = await locker.lock(key, scope="transaction")
        t0 = asyncio.get_running_loop().time()
        assert await other.try_acquire(timeout=None) is True
        elapsed = asyncio.get_running_loop().time() - t0
        assert elapsed >= 0.15
        await other.release()
        await asyncio.wait_for(task, timeout=2)


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
