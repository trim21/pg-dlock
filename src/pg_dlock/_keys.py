import xxhash

_INT64_MIN = -9223372036854775808
_INT64_MAX = 9223372036854775807
_INT32_MIN = -2147483648
_INT32_MAX = 2147483647
_UINT32_MASK = 0xFFFFFFFF


def _to_bytes(v: str | bytes) -> bytes:
    if isinstance(v, str):
        return v.encode("utf-8")
    return v


KeyType = str | bytes | int | tuple[int, int]


def normalize_key(key: KeyType) -> int:
    """Normalize a user-supplied lock key into a signed int64 advisory-lock id.

    - ``str`` / ``bytes``: hashed with ``xxhash.xxh3_64`` then shifted from
      the unsigned 64-bit range into the signed 64-bit range.
    - ``int``: used as-is; must fit in signed int64.
    - ``tuple[int, int]``: each element must fit in signed int32; packed into
      a single int64 as ``(hi << 32) | (lo & 0xFFFFFFFF)`` and then
      reinterpreted as signed int64.
    """
    if isinstance(key, str | bytes):
        return xxhash.xxh3_64_intdigest(_to_bytes(key)) + _INT64_MIN

    if isinstance(key, tuple):
        hi, lo = key
        if not (_INT32_MIN <= hi <= _INT32_MAX):
            raise ValueError(f"lock id[0] {hi!r} out of range for int32")
        if not (_INT32_MIN <= lo <= _INT32_MAX):
            raise ValueError(f"lock id[1] {lo!r} out of range for int32")
        packed = ((hi & _UINT32_MASK) << 32) | (lo & _UINT32_MASK)
        # reinterpret the unsigned 64-bit value as signed int64
        if packed >= (1 << 63):
            packed -= 1 << 64
        return packed

    if isinstance(key, int):  # pyright: ignore[reportUnnecessaryIsInstance]
        if not (_INT64_MIN <= key <= _INT64_MAX):
            raise ValueError(f"lock id {key!r} out of range for int64")
        return key

    raise TypeError(f"unsupported key type: {type(key).__name__}")
