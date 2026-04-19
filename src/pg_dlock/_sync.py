from __future__ import annotations

from typing import TYPE_CHECKING, Literal, LiteralString, Protocol, Self, cast

if TYPE_CHECKING:
    from types import TracebackType

    from psycopg.rows import TupleRow

import psycopg
from psycopg.errors import QueryCanceled
from psycopg_pool import ConnectionPool

from pg_dlock._keys import KeyType, normalize_key
from pg_dlock.errors import FailedToLockError, ReleaseError, UnreachableError

Scope = Literal["session", "transaction"]


class Lock(Protocol):
    def __enter__(self) -> Self: ...
    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None: ...
    def try_acquire(self, timeout: float | None = None) -> bool: ...
    def release(self) -> None: ...


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


def _one(cursor: psycopg.Cursor[TupleRow]) -> object:
    row = cursor.fetchone()
    if row is None:
        raise UnreachableError
    return cast("object", row[0])


def _set_statement_timeout(
    cursor: psycopg.Cursor[TupleRow],
    value: str,
    *,
    local: bool,
) -> None:
    cursor.execute(
        "SELECT pg_catalog.set_config('statement_timeout', %s, %s)",
        (value, local),
    )
    cursor.fetchone()


def _statement_timeout_value(timeout: float | None) -> str:
    if timeout is None:
        return "0"
    return str(int(timeout * 1000))


class Locker:
    """Entry point for acquiring PostgreSQL advisory locks.

    Session-scoped locks open their own dedicated connection. Transaction-
    scoped locks use an internal connection pool that is created lazily.
    """

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
        self._pool: ConnectionPool[psycopg.Connection[TupleRow]] | None = None

    def _get_pool(self) -> ConnectionPool[psycopg.Connection[TupleRow]]:
        if self._pool is None:
            self._pool = ConnectionPool(
                conninfo=self._conninfo,
                min_size=self._pool_min_size,
                max_size=self._pool_max_size,
                open=True,
            )
        return self._pool

    def lock(
        self,
        key: KeyType,
        *,
        scope: Scope = "session",
        shared: bool = False,
    ) -> Lock:
        lock_id = normalize_key(key)
        if scope == "session":
            return _SessionLock(
                conninfo=self._conninfo,
                key=key,
                lock_id=lock_id,
                shared=shared,
            )
        if scope == "transaction":
            return _TransactionLock(
                pool=self._get_pool(),
                key=key,
                lock_id=lock_id,
                shared=shared,
            )
        raise ValueError(f"unknown scope {scope!r}")

    def close(self) -> None:
        if self._pool is not None:
            self._pool.close()
            self._pool = None

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()


class _SessionLock:
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
        self._conn: psycopg.Connection[TupleRow] | None = None
        self._held = False

    def _conn_or_open(self) -> psycopg.Connection[TupleRow]:
        if self._conn is None:
            self._conn = psycopg.Connection.connect(self._conninfo, autocommit=True)
        return self._conn

    def try_acquire(self, timeout: float | None = None) -> bool:
        if self._held:
            return True
        conn = self._conn_or_open()

        if timeout == 0:
            with conn.cursor() as cur:
                cur.execute(
                    _session_try_lock_query(self._shared),
                    (self._lock_id,),
                )
                acquired = bool(_one(cur))
            self._held = acquired
            return acquired

        with conn.cursor() as cur:
            _set_statement_timeout(cur, _statement_timeout_value(timeout), local=False)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    _session_lock_query(self._shared),
                    (self._lock_id,),
                )
                cur.fetchone()
        except QueryCanceled:
            return False

        self._held = True
        return True

    def release(self) -> None:
        if not self._held:
            raise ReleaseError(f"lock {self.key!r} is not held")
        if self._conn is None:
            raise UnreachableError
        with self._conn.cursor() as cur:
            cur.execute(
                _session_unlock_query(self._shared),
                (self._lock_id,),
            )
            result = _one(cur)
        self._held = False
        if result is False:
            raise ReleaseError(f"failed to release lock {self.key!r}")

    def _close_conn(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            finally:
                self._conn = None

    def __enter__(self) -> Self:
        acquired = self.try_acquire(None)
        if not acquired:
            self._close_conn()
            raise FailedToLockError(f"failed to acquire lock {self.key!r}")
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        try:
            if self._held:
                self.release()
        finally:
            self._close_conn()


class _TransactionLock:
    def __init__(
        self,
        *,
        pool: ConnectionPool[psycopg.Connection[TupleRow]],
        key: KeyType,
        lock_id: int,
        shared: bool,
    ):
        self._pool = pool
        self.key = key
        self._lock_id = lock_id
        self._shared = shared
        self._conn: psycopg.Connection[TupleRow] | None = None
        self._held = False

    def try_acquire(self, timeout: float | None = None) -> bool:
        if self._held:
            return True
        conn = self._pool.getconn()
        try:
            if conn.autocommit:
                conn.autocommit = False
            with conn.cursor() as cur:
                cur.execute("BEGIN")
                if timeout != 0:
                    _set_statement_timeout(cur, _statement_timeout_value(timeout), local=True)

                if timeout == 0:
                    cur.execute(
                        _transaction_try_lock_query(self._shared),
                        (self._lock_id,),
                    )
                    acquired = bool(_one(cur))
                    if not acquired:
                        cur.execute("ROLLBACK")
                        self._pool.putconn(conn)
                        return False
                else:
                    try:
                        cur.execute(
                            _transaction_lock_query(self._shared),
                            (self._lock_id,),
                        )
                        cur.fetchone()
                    except QueryCanceled:
                        cur.execute("ROLLBACK")
                        self._pool.putconn(conn)
                        return False
        except BaseException:
            try:
                conn.rollback()
            finally:
                self._pool.putconn(conn)
            raise

        self._conn = conn
        self._held = True
        return True

    def release(self) -> None:
        if not self._held or self._conn is None:
            raise ReleaseError(f"lock {self.key!r} is not held")
        conn = self._conn
        try:
            conn.commit()
        finally:
            self._conn = None
            self._held = False
            self._pool.putconn(conn)

    def __enter__(self) -> Self:
        acquired = self.try_acquire(None)
        if not acquired:
            raise FailedToLockError(f"failed to acquire lock {self.key!r}")
        return self

    def __exit__(
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
                conn.commit()
            else:
                conn.rollback()
        finally:
            self._pool.putconn(conn)
