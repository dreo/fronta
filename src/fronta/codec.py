"""JSON encoding with byte caps and error-metadata shaping.

Caps count UTF-8 bytes of the compact JSON encoding (`separators=(",", ":")`, non-ASCII kept as is).
"""

from __future__ import annotations

import codecs
import json
import traceback
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from fronta.model import JSON

_JSON_KW: dict[str, Any] = {"separators": (",", ":"), "ensure_ascii": False, "allow_nan": False}

MAX_DEPTH = 200
"""Nesting levels accepted in a stored value; deeper structures are rejected before any recursion
limit can turn a deterministic bad value into a retryable failure."""

_REPLACEMENT = "�"


def _replace_lone_surrogates(err: UnicodeError) -> tuple[bytes, int]:
    """Codec error handler: one U+FFFD per code point that has no UTF-8 encoding.

    The replacement is returned as bytes: the UTF-8 encoder accepts only ASCII text replacements
    from a handler and would reject U+FFFD itself.
    """
    if isinstance(err, UnicodeEncodeError):
        return _REPLACEMENT.encode("utf-8") * (err.end - err.start), err.end
    raise err  # pragma: no cover  # only registered for encoding


codecs.register_error("fronta_replace", _replace_lone_surrogates)


class Unstorable(ValueError):
    """The value cannot live in a `jsonb` column.

    NUL characters, lone surrogates (no UTF-8 encoding), NaN/infinity, circular references and
    nesting deeper than `MAX_DEPTH`.
    """


class OverCap(ValueError):
    """The encoding exceeds its byte cap."""


def encode(value: JSON) -> str:
    """Compact JSON.

    Raises `Unstorable` for NaN/inf, NUL characters and lone surrogates (which `jsonb` and UTF-8
    reject), circular references and excessive depth, and `TypeError` for anything that is not
    JSON: unknown objects and non-string mapping keys (`json.dumps` would silently turn
    `{1: ...}` into `{"1": ...}`).
    """
    _check(value, 0, set())
    try:
        return json.dumps(value, **_JSON_KW)
    except ValueError as exc:
        raise Unstorable(str(exc)) from exc


def _check_text(text: str) -> None:
    if "\x00" in text:
        msg = "NUL characters cannot be stored in jsonb"
        raise Unstorable(msg)
    try:
        text.encode("utf-8")
    except UnicodeEncodeError as exc:
        msg = "lone surrogate code points cannot be stored (no UTF-8 encoding)"
        raise Unstorable(msg) from exc


def _check(value: object, depth: int, path: set[int]) -> None:
    """Validate storability without unbounded recursion: depth is capped, cycles are detected."""
    if isinstance(value, str):
        _check_text(value)
        return
    if not isinstance(value, dict | list | tuple):
        return
    if depth >= MAX_DEPTH:
        msg = f"value is nested deeper than {MAX_DEPTH} levels"
        raise Unstorable(msg)
    if id(value) in path:
        msg = "circular reference in value"
        raise Unstorable(msg)
    path.add(id(value))
    try:
        if isinstance(value, dict):
            for key, item in value.items():
                if not isinstance(key, str):
                    msg = f"mapping keys must be strings, got {type(key).__name__}"
                    raise TypeError(msg)
                _check_text(key)
                _check(item, depth + 1, path)
        else:
            for item in value:
                _check(item, depth + 1, path)
    finally:
        path.discard(id(value))


def sanitize(text: str) -> str:
    """Make text storable: NUL and lone surrogates become U+FFFD."""
    text = text.replace("\x00", _REPLACEMENT)
    try:
        text.encode("utf-8")
    except UnicodeEncodeError:
        text = text.encode("utf-8", "fronta_replace").decode("utf-8")
    return text


def utf8_len(text: str) -> int:
    return len(text.encode("utf-8"))


def encode_capped(value: JSON, cap: int, what: str) -> str:
    """Encode and enforce the cap. Raises `OverCap` with a message naming `what`."""
    text = encode(value)
    size = utf8_len(text)
    if size > cap:
        msg = f"{what} is {size} bytes, cap is {cap} bytes"
        raise OverCap(msg)
    return text


def error_metadata(exc: BaseException, cap: int) -> dict[str, Any]:
    """Structured error for a failed attempt: type, message, traceback — truncated to `cap`.

    Always returns a storable value: formatting failures of the exception itself (a broken
    `__str__`, unencodable text) degrade to a placeholder instead of raising.
    """
    name = type(exc).__qualname__
    try:
        message = str(exc)
    except Exception as failure:  # a broken __str__ must not lose the outcome
        message = f"<{name}: str() raised {type(failure).__qualname__}>"
    try:
        tb = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
    except Exception as failure:  # same: the traceback is optional metadata
        tb = f"<traceback unavailable: {type(failure).__qualname__}>"
    return truncate(
        {"type": name, "message": sanitize(message), "traceback": sanitize(tb)},
        cap,
        keep_tail=("traceback",),
    )


def truncate(error: dict[str, Any], cap: int, keep_tail: tuple[str, ...] = ()) -> dict[str, Any]:
    """Shrink the longest string fields until the encoding fits `cap`; marks `truncated: true`.

    Fields in `keep_tail` keep their end (the useful part of a traceback or a log stream); other
    fields keep their start. Slicing happens on code points, so the output stays valid UTF-8.
    Always terminates: a field that cannot shrink further is emptied, and when nothing is left
    to cut only the type survives.
    """
    data = dict(error)
    if utf8_len(encode(data)) <= cap:
        return data
    data["truncated"] = True
    while utf8_len(encode(data)) > cap:
        name, text = max(
            ((k, v) for k, v in data.items() if isinstance(v, str) and v),
            key=lambda kv: len(kv[1]),
            default=(None, ""),
        )
        if name is None:
            return {"type": str(error.get("type", ""))[:64], "truncated": True}
        keep = len(text) // 2
        if keep == 0:
            data[name] = ""
        else:
            data[name] = text[-keep:] if name in keep_tail else text[:keep]
    return data
