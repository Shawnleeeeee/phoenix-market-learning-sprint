#!/usr/bin/env python3
from __future__ import annotations

import json
import sys
from urllib import parse, request


def main() -> int:
    try:
        payload = json.loads(sys.stdin.read())
    except Exception as exc:  # noqa: BLE001
        print(json.dumps({"ok": False, "error": f"invalid_input:{exc}"}))
        return 1
    token = str(payload.get("token") or "").strip()
    chat_id = str(payload.get("chat_id") or "").strip()
    text = str(payload.get("text") or "")
    if not token or not chat_id:
        print(json.dumps({"ok": False, "error": "missing_token_or_chat_id"}))
        return 1
    body = parse.urlencode(
        {
            "chat_id": chat_id,
            "text": text,
            "disable_web_page_preview": "true",
        }
    ).encode("utf-8")
    req = request.Request(
        f"https://api.telegram.org/bot{token}/sendMessage",
        data=body,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        method="POST",
    )
    try:
        with request.urlopen(req, timeout=30) as resp:
            response = resp.read().decode("utf-8", errors="replace")
    except Exception as exc:  # noqa: BLE001
        print(json.dumps({"ok": False, "error": str(exc)}))
        return 1
    print(response)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
