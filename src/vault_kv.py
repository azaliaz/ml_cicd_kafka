"""Чтение данных из HashiCorp Vault (KV v2), путь вида secret/data/<logical>."""
from __future__ import annotations

import json
import urllib.error
import urllib.request
from typing import Any


def read_kv_v2_secret(*, addr: str, token: str, secret_path: str) -> dict[str, Any]:
    path = secret_path.lstrip("/")
    url = f"{addr.rstrip('/')}/v1/{path}"
    req = urllib.request.Request(url, headers={"X-Vault-Token": token})
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace") if e.fp else ""
        raise RuntimeError(f"Vault HTTP {e.code}: {body}") from e
    except urllib.error.URLError as e:
        raise RuntimeError(f"Vault unreachable at {addr}: {e}") from e

    if "errors" in payload and payload["errors"]:
        raise RuntimeError(f"Vault error: {payload['errors']}")
    data = (payload.get("data") or {}).get("data")
    if not isinstance(data, dict):
        raise RuntimeError("Vault response missing data.data object")
    return data
