# pg-dlock

PostgreSQL advisory-lock based distributed lock, sync + async.

```python
from pg_dlock import Locker

with Locker("postgres://...") as locker:
    with locker.lock("my-key"):
        ...  # critical section
```

Async:

```python
from pg_dlock import AsyncLocker

async with AsyncLocker("postgres://...") as locker:
    async with locker.lock("my-key"):
        ...
```

## API

- `Locker(conninfo, *, pool_min_size=0, pool_max_size=10)`
- `locker.lock(key, *, scope="session"|"transaction", shared=False) -> Lock`
- `lock.acquire(blocking=True, timeout=-1) -> bool`
  - `blocking=True, timeout=-1`: block forever
  - `blocking=False`: non-blocking
  - `blocking=True, timeout>=0` (seconds): wait up to N seconds (uses `statement_timeout`)
- `lock.release()`
- Context manager support (`with lock:` / `async with lock:`).

### Scopes

- `session`: each lock opens its own dedicated connection (autocommit). Released via `pg_advisory_unlock`.
- `transaction`: connection is checked out from an internal pool and a transaction is begun; lock is released by the commit/rollback on exit.

### Keys

- `str` / `bytes`: hashed with `xxhash.xxh3_64` into a signed int64.
- `int`: used directly (must fit in signed int64).
- `tuple[int, int]`: each element must fit in signed int32; packed into a single int64.
