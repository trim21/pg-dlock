import pytest

from pg_dlock._keys import normalize_key


def test_int_key_roundtrip():
    assert normalize_key(0) == 0
    assert normalize_key(42) == 42
    assert normalize_key(-1) == -1
    assert normalize_key(9223372036854775807) == 9223372036854775807
    assert normalize_key(-9223372036854775808) == -9223372036854775808


def test_str_and_bytes_keys_produce_int64():
    a = normalize_key("hello")
    b = normalize_key(b"hello")
    assert a == b
    assert -9223372036854775808 <= a <= 9223372036854775807


def test_different_str_keys_differ():
    assert normalize_key("a") != normalize_key("b")


def test_int_key_out_of_range():
    with pytest.raises(ValueError, match="out of range"):
        normalize_key(1 << 63)
    with pytest.raises(ValueError, match="out of range"):
        normalize_key(-(1 << 63) - 1)


def test_unsupported_key_type():
    with pytest.raises(TypeError):
        normalize_key(1.5)  # type: ignore[arg-type]


def test_tuple_key_packs_into_int64():
    # (0, 0) -> 0
    assert normalize_key((0, 0)) == 0
    # (0, 1) -> 1
    assert normalize_key((0, 1)) == 1
    # (1, 0) -> 1 << 32
    assert normalize_key((1, 0)) == 1 << 32
    # high bit set via negative hi -> negative signed int64
    assert normalize_key((-1, -1)) == -1
    # distinct tuples produce distinct ids
    assert normalize_key((1, 2)) != normalize_key((2, 1))
    # stays within int64 range
    v = normalize_key((2147483647, -2147483648))
    assert -9223372036854775808 <= v <= 9223372036854775807


def test_tuple_key_out_of_range():
    with pytest.raises(ValueError, match="out of range"):
        normalize_key((1 << 31, 0))
    with pytest.raises(ValueError, match="out of range"):
        normalize_key((0, -(1 << 31) - 1))
