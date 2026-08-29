"""Caps count UTF-8 bytes of the compact JSON encoding; error metadata shrinks to fit."""

import json
import math

import pytest

from fronta import codec


def test_encoding_is_compact_and_keeps_non_ascii():
    assert codec.encode({"a": [1, "é"], "b": None}) == '{"a":[1,"é"],"b":null}'


@pytest.mark.parametrize("value", [{"k": 1}, [1, 2], "s", 1, 1.5, True, None])
def test_every_json_value_kind_round_trips(value):
    assert json.loads(codec.encode(value)) == value


@pytest.mark.parametrize("value", [math.nan, math.inf, -math.inf])
def test_non_finite_numbers_are_rejected(value):
    with pytest.raises(ValueError, match="Out of range float"):
        codec.encode(value)


def test_nul_characters_are_unstorable_and_sanitizable():
    with pytest.raises(codec.Unstorable, match="NUL"):
        codec.encode({"s": "a\x00b"})
    assert codec.sanitize("a\x00b") == "a\ufffdb"


def test_error_metadata_never_carries_nul():
    error = codec.error_metadata(RuntimeError("bad\x00byte"), 65536)
    assert "\x00" not in error["message"]
    codec.encode(error)


def test_non_json_objects_are_rejected():
    with pytest.raises(TypeError):
        codec.encode({"when": object()})  # type: ignore[dict-item]


def test_cap_counts_utf8_bytes_exactly_at_the_boundary():
    # "é" is 2 bytes in UTF-8: the quotes add 2, so the encoding is 4 bytes.
    assert codec.utf8_len(codec.encode("é")) == 4
    assert codec.encode_capped("é", 4, "x") == '"é"'
    with pytest.raises(codec.OverCap, match="x is 4 bytes, cap is 3 bytes"):
        codec.encode_capped("é", 3, "x")


def test_error_metadata_has_type_message_traceback():
    try:
        msg = "boom"
        raise RuntimeError(msg)
    except RuntimeError as exc:
        error = codec.error_metadata(exc, 65536)
    assert error["type"] == "RuntimeError"
    assert error["message"] == "boom"
    assert "Traceback" in error["traceback"]
    assert "raise RuntimeError" in error["traceback"]
    assert "truncated" not in error


def test_truncation_fits_the_cap_keeps_valid_utf8_and_flags_it():
    error = {"type": "E", "message": "é" * 5000, "traceback": "tail-" + "x" * 5000 + "-END"}
    small = codec.truncate(error, 512, keep_tail=("traceback",))
    assert codec.utf8_len(codec.encode(small)) <= 512
    assert small["truncated"] is True
    assert small["type"] == "E"
    assert small["traceback"].endswith("-END")  # tail kept
    assert small["message"].startswith("é")  # head kept, no broken code points
    small["message"].encode("utf-8")


def test_truncation_is_a_no_op_under_the_cap():
    error = {"type": "E", "message": "short"}
    assert codec.truncate(error, 1024) == error


def test_truncation_terminates_at_tiny_caps():
    error = {"type": "RuntimeError", "message": "m" * 100, "traceback": "t" * 100}
    small = codec.truncate(error, 40, keep_tail=("traceback",))
    assert codec.utf8_len(codec.encode(small)) <= 40
    assert small["truncated"] is True
    minimal = codec.truncate(error, 1)
    assert minimal == {"type": "RuntimeError", "truncated": True}


def test_literal_backslash_u0000_text_is_valid_and_storable():
    text = "\\u0000 is six characters"
    assert "\x00" not in text
    assert json.loads(codec.encode({"s": text})) == {"s": text}


def test_non_string_mapping_keys_are_rejected_and_tuples_become_arrays():
    with pytest.raises(TypeError, match="keys must be strings"):
        codec.encode({1: "x"})  # type: ignore[dict-item]
    with pytest.raises(TypeError, match="keys must be strings"):
        codec.encode({"a": [{None: 1}]})  # type: ignore[list-item]
    assert codec.encode({"t": (1, 2)}) == '{"t":[1,2]}'  # type: ignore[dict-item]
