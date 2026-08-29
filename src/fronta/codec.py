"""JSON encoding with byte caps and error-metadata shaping.

Caps count UTF-8 bytes of the compact JSON encoding (`separators=(",", ":")`, non-ASCII kept as is).
"""

from __future__ import annotations

import json
import traceback
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from fronta.model import JSON

_JSON_KW: dict[str, Any] = {"separators": (",", ":"), "ensure_ascii": False, "allow_nan": False}


class Unstorable(ValueError):
    """The value cannot live in a `jsonb` column (NUL characters, NaN, infinity)."""


class OverCap(ValueError):
    """The encoding exceeds its byte cap."""


def encode(value: JSON) -> str:
    """Compact JSON.

    Raises `Unstorable` for NaN/inf and for NUL characters (which `jsonb` rejects), and
    `TypeError` for anything that is not JSON: unknown objects and non-string mapping keys
    (`json.dumps` would silently turn `{1: ...}` into `{"1": ...}`).
    """
    _check(value)
    try:
        return json.dumps(value, **_JSON_KW)
    except ValueError as exc:
        raise Unstorable(str(exc)) from exc


def _check(value: object) -> None:
    if isinstance(value, str):
        if "\x00" in value:
            msg = "NUL characters cannot be stored in jsonb"
            raise Unstorable(msg)
    elif isinstance(value, dict):
        for key, item in value.items():
            if not isinstance(key, str):
                msg = f"mapping keys must be strings, got {type(key).__name__}"
                raise TypeError(msg)
            _check(key)
            _check(item)
    elif isinstance(value, list | tuple):
        for item in value:
            _check(item)


def sanitize(text: str) -> str:
    """Make text storable: NUL is the one code point `jsonb` rejects."""
    return text.replace("\x00", "\ufffd")


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
    """Structured error for a failed attempt: type, message, traceback — truncated to `cap`."""
    tb = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
    return truncate(
        {"type": type(exc).__qualname__, "message": sanitize(str(exc)), "traceback": sanitize(tb)},
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
