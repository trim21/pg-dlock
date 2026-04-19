from __future__ import annotations

from typing import TYPE_CHECKING, Literal, LiteralString, Protocol, Self, cast

if TYPE_CHECKING:
    from types import TracebackType

    from psycopg.rows import TupleRow

import psycopg
from psycopg.errors import QueryCanceled
from psycopg_pool import AsyncConnectionPool

from pg_dlock._keys import KeyType, normalize_key
from pg_dlock.errors import FailedToLockError, ReleaseError, UnreachableError

Scope = Literal["session", "transaction"]


class AsyncLock(Protocol):
    async def __aenter__(self) -> Self: ...
    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None: ...
    async def try_acquire(self, timeout: float | None = None) -> bool: ...  # noqa: ASYNC109
    async def release(self) -> None: ...


def _session_try_lock_query(shared: bool) -> LiteralString:
    if shared:
        return "SELECT pg_catalog.pg_try_advisory_lock_shared(%s)"
    return "SELECT pg_catalog.pg_try_advisory_lock(%s)"


def _session_lock_query(shared: bool) -> LiteralString:
    if shared:
        return "SELECT pg_catalog.pg_advisory_lock_shared(%s)"
    return "SELECT pg_catalog.pg_advisory_lock(%s)"


def _session_unlock_query(shared: bool) -> LiteralString:
    if shared:
        return "SELECT pg_catalog.pg_advisory_unlock_shared(%s)"
    return "SELECT pg_catalog.pg_advisory_unlock(%s)"


def _transaction_try_lock_query(shared: bool) -> LiteralString:
    if shared:
        return "SELECT pg_catalog.pg_try_advisory_xact_lock_shared(%s)"
    return "SELECT pg_catalog.pg_try_advisory_xact_lock(%s)"


def _transaction_lock_query(shared: bool) -> LiteralString:
    if shared:
        return "SELECT pg_catalog.pg_advisory_xact_lock_shared(%s)"
    return "SELECT pg_catalog.pg_advisory_xact_lock(%s)"


async def _one(cursor: psycopg.AsyncCursor[TupleRow]) -> object:
    row = await cursor.fetchone()
    if row is None:
        raise UnreachableError
    return cast("object", row[0])


async def _set_statement_timeout(
    cursor: psycopg.AsyncCursor[TupleRow],
    value: str,
    *,
    local: bool,
) -> None:
    await cursor.execute(
        "SELECT pg_catalog.set_config('statement_timeout', %s, %s)",
        (value, local),
    )
    await cursor.fetchone()


class AsyncLocker:
    def __init__(
        self,
        conninfo: str,
        *,
        pool_min_size: int = 0,
        pool_max_size: int = 10,
    ):
        self._conninfo = conninfo
        self._pool_min_size = pool_min_size
        self._pool_max_size = pool_max_size
        self._pool: AsyncConnectionPool[psycopg.AsyncConnection[TupleRow]] | None = None

    async def _get_pool(self) -> AsyncConnectionPool[psycopg.AsyncConnection[TupleRow]]:
        if self._pool is None:
            self._pool = AsyncConnectionPool(
                conninfo=self._conninfo,
                min_size=self._pool_min_size,
                max_size=self._pool_max_size,
                open=False,
            )
            await self._pool.open()
        return self._pool

    async def lock(
        self,
        key: KeyType,
        *,
        scope: Scope = "session",
        shared: bool = False,
    ) -> AsyncLock:
        lock_id = normalize_key(key)
        if scope == "session":
            return _AsyncSessionLock(
                conninfo=self._conninfo,
                key=key,
                lock_id=lock_id,
                shared=shared,
            )
        if scope == "transaction":
            pool = await self._get_pool()
            return _AsyncTransactionLock(
                pool=pool,
                key=key,
                lock_id=lock_id,
                shared=shared,
            )
        raise ValueError(f"unknown scope {scope!r}")

    async def close(self) -> None:
        if self._pool is not None:
            await self._pool.close()
            self._pool = None

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.close()


class _AsyncSessionLock:
    def __init__(
        self,
        *,
        conninfo: str,
        key: KeyType,
        lock_id: int,
        shared: bool,
    ):
        self._conninfo = conninfo
        self.key = key
        self._lock_id = lock_id
        self._shared = shared
        self._conn: psycopg.AsyncConnection[TupleRow] | None = None
        self._held = False

    async def _conn_or_open(self) -> psycopg.AsyncConnection[TupleRow]:
        if self._conn is None:
            self._conn = await psycopg.AsyncConnection.connect(self._conninfo, autocommit=True)
        return self._conn

    async def try_acquire(self, timeout: float | None = None) -> bool:  # noqa: ASYNC109
        if self._held:
            return True
        conn = await self._conn_or_open()

        if timeout == 0:
            async with conn.cursor() as cur:
                await cur.execute(
                    _session_try_lock_query(self._shared),
                    (self._lock_id,),
                )
                acquired = bool(await _one(cur))
            self._held = acquired
            return acquired

        prev_timeout: str | None = None
        if timeout is not None:
            async with conn.cursor() as cur:
                await cur.execute("SHOW statement_timeout")
                row = await cur.fetchone()
                prev_timeout = str(cast("object", row[0])) if row else "0"
                await _set_statement_timeout(cur, str(int(timeout * 1000)), local=False)
        try:
            async with conn.cursor() as cur:
                await cur.execute(
                    _session_lock_query(self._shared),
                    (self._lock_id,),
                )
                await cur.fetchone()
        except QueryCanceled:
            return False
        finally:
            if prev_timeout is not None:
                async with conn.cursor() as cur:
                    await _set_statement_timeout(cur, prev_timeout, local=False)

        self._held = True
        return True

    async def release(self) -> None:
        if not self._held:
            raise ReleaseError(f"lock {self.key!r} is not held")
        if self._conn is None:
            raise UnreachableError
        async with self._conn.cursor() as cur:
            await cur.execute(
                _session_unlock_query(self._shared),
                (self._lock_id,),
            )
            result = await _one(cur)
        self._held = False
        if result is False:
            raise ReleaseError(f"failed to release lock {self.key!r}")

    async def _close_conn(self) -> None:
        if self._conn is not None:
            try:
                await self._conn.close()
            finally:
                self._conn = None

    async def __aenter__(self) -> Self:
        acquired = await self.try_acquire(None)
        if not acquired:
            await self._close_conn()
            raise FailedToLockError(f"failed to acquire lock {self.key!r}")
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        try:
            if self._held:
                await self.release()
        finally:
            await self._close_conn()


class _AsyncTransactionLock:
    def __init__(
        self,
        *,
        pool: AsyncConnectionPool[psycopg.AsyncConnection[TupleRow]],
        key: KeyType,
        lock_id: int,
        shared: bool,
    ):
        self._pool = pool
        self.key = key
        self._lock_id = lock_id
        self._shared = shared
        self._conn: psycopg.AsyncConnection[TupleRow] | None = None
        self._held = False

    async def try_acquire(self, timeout: float | None = None) -> bool:  # noqa: ASYNC109
        if self._held:
            return True
        conn = await self._pool.getconn()
        try:
            if conn.autocommit:
                await conn.set_autocommit(False)
            async with conn.cursor() as cur:
                await cur.execute("BEGIN")
                if timeout is not None and timeout > 0:
                    await _set_statement_timeout(cur, str(int(timeout * 1000)), local=True)

                if timeout == 0:
                    await cur.execute(
                        _transaction_try_lock_query(self._shared),
                        (self._lock_id,),
                    )
                    acquired = bool(await _one(cur))
                    if not acquired:
                        await cur.execute("ROLLBACK")
                        await self._pool.putconn(conn)
                        return False
                else:
                    try:
                        await cur.execute(
                            _transaction_lock_query(self._shared),
                            (self._lock_id,),
                        )
                        await cur.fetchone()
                    except QueryCanceled:
                        await cur.execute("ROLLBACK")
                        await self._pool.putconn(conn)
                        return False
        except BaseException:
            try:
                await conn.rollback()
            finally:
                await self._pool.putconn(conn)
            raise

        self._conn = conn
        self._held = True
        return True

    async def release(self) -> None:
        if not self._held or self._conn is None:
            raise ReleaseError(f"lock {self.key!r} is not held")
        conn = self._conn
        try:
            await conn.commit()
        finally:
            self._conn = None
            self._held = False
            await self._pool.putconn(conn)

    async def __aenter__(self) -> Self:
        acquired = await self.try_acquire(None)
        if not acquired:
            raise FailedToLockError(f"failed to acquire lock {self.key!r}")
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        if not self._held or self._conn is None:
            return
        conn = self._conn
        self._conn = None
        self._held = False
        try:
            if exc_type is None:
                await conn.commit()
            else:
                await conn.rollback()
        finally:
            await self._pool.putconn(conn)
