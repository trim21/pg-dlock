# AGENTS.md

Guidance for AI coding agents working on the `pg-dlock` package.

## Project overview

`pg-dlock` is a standalone Python package that exposes a distributed lock API
backed by PostgreSQL advisory locks. It provides both sync and async variants
and is published to PyPI as `pg-dlock`.

- Import name: `pg_dlock`
- Python: `>=3.12`
- Build backend: `pdm-backend`
- Runtime deps: `psycopg[binary,pool]`, `xxhash`

## Layout

```
src/pg_dlock/
  __init__.py     # public re-exports
  _keys.py        # KeyType + normalize_key(key) -> int64
  _sync.py        # Locker, Lock (sync)
  _async.py       # AsyncLocker, AsyncLock (async)
  errors.py       # FailedToLockError, ReleaseError, UnreachableError
  py.typed
tests/
  test_keys.py    # offline, always runs
  test_sync.py    # integration, gated by PG_DLOCK_TEST_DSN
  test_async.py   # integration, gated by PG_DLOCK_TEST_DSN
```

## Conventions

- Strict `pyright` (see `[tool.pyright]` in `pyproject.toml`). Everything must
  type-check in strict mode.
- `ruff check` must be clean. Selected rule groups include `PT`, `ASYNC`, `TC`,
  `TRY`, `PL`, `SIM`, etc.
- Use `from __future__ import annotations` + `if TYPE_CHECKING:` for
  runtime-only type imports (`TracebackType`, etc.).
- `__enter__` / `__aenter__` return `Self`.
- Public API surface is only what `__init__.py` re-exports. Everything else is
  underscore-prefixed.

### Key normalization (`_keys.py`)

`KeyType = str | bytes | int | tuple[int, int]`.

- `str` / `bytes`: hashed with `xxhash.xxh3_64_intdigest`, shifted into signed
  int64 range.
- `int`: validated to fit in signed int64; used as-is.
- `tuple[int, int]`: **both elements** must fit in signed int32; packed into a
  single int64 as `(hi << 32) | (lo & 0xFFFFFFFF)`, then reinterpreted as
  signed.

Do not change the encoding — lock identities must be stable across versions.

### Scopes

`Locker.lock(key, *, scope="session" | "transaction", shared=False)`:

- `session` (default): dedicated autocommit connection; uses
  `pg_advisory_lock[_shared]` / `pg_try_advisory_lock[_shared]` /
  `pg_advisory_unlock[_shared]`.
- `transaction`: connection checked out from a pool; wraps `BEGIN` / `COMMIT` /
  `ROLLBACK` around `pg_advisory_xact_lock[_shared]`. The lock is released
  automatically when the transaction ends.

`try_acquire(timeout)`:
- `None` → block forever (`pg_advisory_lock`).
- `0` → non-blocking (`pg_try_advisory_lock`).
- `> 0` → `SET LOCAL statement_timeout` and catch `QueryCanceled`.

### SQL

Use the default cursor with `%s` placeholders. Do **not** use `RawCursor` / `$1`
placeholders; they don't play well with pyright + psycopg's generic typing.

## Tooling

```sh
uv sync --locked --group dev
ruff check src tests
pyright
```

For integration tests, spin up Postgres and set:

```sh
export PG_DLOCK_TEST_DSN=postgresql://postgres:postgres@localhost:5432/pg_dlock_test
```

## Lockfile

`uv.lock` **is committed** and CI uses `uv sync --locked`. After changing
dependencies:

```sh
uv lock
```

Regenerate the lockfile in the package directory itself, not from the
monorepo's workspace root — `pg-dlock` is shipped as a standalone repo and its
`uv.lock` must be self-contained.

## CI

GitHub Actions under `.github/workflows/`:

- `build.yaml` — `uv build` + `uvx twine check --strict`.
- `lint.yaml` — `pyright` + pre-commit via `trim21/actions/prek`.
- `test.yaml` — matrix of Python 3.12/3.13/3.14 × Postgres 15/16/17 using a
  service container; `PG_DLOCK_TEST_DSN` is set so integration tests run.
- `release.yaml` — on tag `v*`: `uv build` + `uv publish`  then generate a GitHub release from
  conventional-commit history.

## Don't

- Don't add extra overloads, or clever key-compression
  schemes — the current encoding is intentional and stable.
- Don't introduce new runtime dependencies lightly; this library aims to stay
  minimal.
- Don't edit generated files: `uv.lock` is machine-managed (regenerate via
  `uv lock`), `.pdm-build/` is ignored.
