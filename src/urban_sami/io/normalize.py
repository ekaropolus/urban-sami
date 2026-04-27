from __future__ import annotations

import unicodedata


def norm_key(value: str) -> str:
    text = str(value or "").strip().lower()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return text.replace("_", "").replace("-", "").replace(" ", "")


def pick(row: dict, aliases: tuple[str, ...], default: str = "") -> str:
    normalized = {norm_key(key): value for key, value in (row or {}).items()}
    for alias in aliases:
        value = normalized.get(norm_key(alias))
        if value is not None:
            text = str(value).strip()
            if text:
                return text
    return default

