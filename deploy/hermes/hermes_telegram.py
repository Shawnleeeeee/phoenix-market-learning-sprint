from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path
from typing import Any
from urllib import parse, request


def _strip_matching_quotes(value: str) -> str:
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        return value[1:-1]
    return value


def load_hermes_env(home: Path) -> dict[str, str]:
    env_path = home / ".env"
    values: dict[str, str] = {}
    if not env_path.exists():
        return values
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].lstrip()
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = _strip_matching_quotes(value.strip())
    return values


def load_hermes_config_values(home: Path) -> dict[str, str]:
    config_path = home / "config.yaml"
    values: dict[str, str] = {}
    if not config_path.exists():
        return values
    for raw_line in config_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or ":" not in line:
            continue
        key, value = line.split(":", 1)
        key = key.strip()
        if key not in {
            "TELEGRAM_ALLOWED_USERS",
            "TELEGRAM_HOME_CHANNEL",
            "TELEGRAM_PROXY",
            "HTTP_PROXY",
            "HTTPS_PROXY",
            "ALL_PROXY",
            "TELEGRAM_BOT_TOKEN",
            "HERMES_TELEGRAM_RELAY_TARGET",
            "HERMES_TELEGRAM_RELAY_KEY_PATH",
        }:
            continue
        values[key] = value.strip().strip("'").strip('"')
    return values


def telegram_settings(home: Path) -> dict[str, str | None]:
    env = load_hermes_env(home)
    config = load_hermes_config_values(home)
    token = (
        os.environ.get("TELEGRAM_BOT_TOKEN")
        or env.get("TELEGRAM_BOT_TOKEN")
        or config.get("TELEGRAM_BOT_TOKEN")
        or ""
    ).strip()
    chat_id = (
        os.environ.get("TELEGRAM_HOME_CHANNEL")
        or os.environ.get("TELEGRAM_ALLOWED_USERS")
        or env.get("TELEGRAM_HOME_CHANNEL")
        or env.get("TELEGRAM_ALLOWED_USERS")
        or config.get("TELEGRAM_HOME_CHANNEL")
        or config.get("TELEGRAM_ALLOWED_USERS")
        or ""
    ).strip()
    if "," in chat_id:
        chat_id = chat_id.split(",", 1)[0].strip()
    proxy = (
        os.environ.get("TELEGRAM_PROXY")
        or os.environ.get("HTTPS_PROXY")
        or os.environ.get("HTTP_PROXY")
        or env.get("TELEGRAM_PROXY")
        or env.get("HTTPS_PROXY")
        or env.get("HTTP_PROXY")
        or env.get("ALL_PROXY")
        or config.get("TELEGRAM_PROXY")
        or config.get("HTTPS_PROXY")
        or config.get("HTTP_PROXY")
        or config.get("ALL_PROXY")
    )
    relay_target = (
        os.environ.get("HERMES_TELEGRAM_RELAY_TARGET")
        or env.get("HERMES_TELEGRAM_RELAY_TARGET")
        or config.get("HERMES_TELEGRAM_RELAY_TARGET")
        or ""
    ).strip()
    relay_key_path = (
        os.environ.get("HERMES_TELEGRAM_RELAY_KEY_PATH")
        or env.get("HERMES_TELEGRAM_RELAY_KEY_PATH")
        or config.get("HERMES_TELEGRAM_RELAY_KEY_PATH")
        or ""
    ).strip()
    return {
        "token": token or None,
        "chat_id": chat_id or None,
        "proxy": proxy or None,
        "relay_target": relay_target or None,
        "relay_key_path": relay_key_path or None,
    }


def _send_local(text: str, settings: dict[str, str | None]) -> str | None:
    token = settings.get("token")
    chat_id = settings.get("chat_id")
    if not token or not chat_id:
        return "telegram_not_configured"
    payload = parse.urlencode(
        {
            "chat_id": chat_id,
            "text": text,
            "disable_web_page_preview": "true",
        }
    ).encode("utf-8")
    req = request.Request(
        f"https://api.telegram.org/bot{token}/sendMessage",
        data=payload,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        method="POST",
    )
    opener = request.build_opener()
    proxy = settings.get("proxy")
    if proxy:
        opener.add_handler(request.ProxyHandler({"http": proxy, "https": proxy}))
    try:
        with opener.open(req, timeout=30) as resp:
            body = resp.read().decode("utf-8", errors="replace")
    except Exception as exc:  # noqa: BLE001
        return str(exc)
    try:
        decoded = json.loads(body)
    except json.JSONDecodeError:
        return f"telegram_invalid_json:{body[:120]}"
    if not isinstance(decoded, dict) or not decoded.get("ok"):
        return f"telegram_api_error:{decoded}"
    return None


def _send_via_relay(text: str, settings: dict[str, str | None]) -> str | None:
    relay_target = settings.get("relay_target")
    relay_key_path = settings.get("relay_key_path")
    token = settings.get("token")
    chat_id = settings.get("chat_id")
    if not relay_target or not token or not chat_id:
        return "telegram_relay_not_configured"
    ssh_cmd = ["ssh", "-o", "StrictHostKeyChecking=no"]
    if relay_key_path:
        ssh_cmd.extend(["-i", relay_key_path])
    relay_script = (
        "python3 - <<'PY'\n"
        "import json, sys, urllib.parse, urllib.request\n"
        "payload = json.loads(sys.stdin.read())\n"
        "data = urllib.parse.urlencode({"
        "'chat_id': payload['chat_id'], 'text': payload['text'], 'disable_web_page_preview': 'true'"
        "}).encode('utf-8')\n"
        "req = urllib.request.Request("
        "f\"https://api.telegram.org/bot{payload['token']}/sendMessage\", "
        "data=data, headers={'Content-Type':'application/x-www-form-urlencoded'}, method='POST')\n"
        "with urllib.request.urlopen(req, timeout=30) as resp:\n"
        "    body = resp.read().decode('utf-8', 'replace')\n"
        "print(body)\n"
        "PY"
    )
    try:
        completed = subprocess.run(
            [*ssh_cmd, relay_target, relay_script],
            input=json.dumps({"token": token, "chat_id": chat_id, "text": text}),
            text=True,
            capture_output=True,
            check=True,
        )
    except subprocess.CalledProcessError as exc:
        details = (exc.stderr or exc.stdout or str(exc)).strip()
        return f"telegram_relay_failed:{details[:240]}"
    try:
        decoded = json.loads((completed.stdout or "").strip())
    except json.JSONDecodeError:
        return f"telegram_relay_invalid_json:{(completed.stdout or '')[:120]}"
    if not isinstance(decoded, dict) or not decoded.get("ok"):
        return f"telegram_relay_api_error:{decoded}"
    return None


def send_telegram_message(text: str, home: Path) -> str | None:
    settings = telegram_settings(home)
    local_error = _send_local(text, settings)
    if local_error is None:
        return None
    relay_error = _send_via_relay(text, settings)
    if relay_error is None:
        return None
    return f"local={local_error}; relay={relay_error}"
