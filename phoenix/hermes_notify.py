from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any
from urllib import parse, request

import aiohttp

DEFAULT_HERMES_HOME = Path.home() / ".hermes"


def hermes_home() -> Path:
    return Path(str(os.environ.get("HERMES_HOME") or DEFAULT_HERMES_HOME))


def _strip_matching_quotes(value: str) -> str:
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        return value[1:-1]
    return value


def load_hermes_env(home: Path | None = None) -> dict[str, str]:
    env_path = (home or hermes_home()) / ".env"
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
        key = key.strip()
        if not key:
            continue
        values[key] = _strip_matching_quotes(value.strip())
    return values


def load_hermes_config_values(home: Path | None = None) -> dict[str, str]:
    config_file = (home or hermes_home()) / "config.yaml"
    values: dict[str, str] = {}
    if not config_file.exists():
        return values
    for raw_line in config_file.read_text(encoding="utf-8").splitlines():
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
            "TELEGRAM_BOT_TOKEN",
        }:
            continue
        values[key] = value.strip().strip("'").strip('"')
    return values


def telegram_settings(home: Path | None = None) -> dict[str, str | None]:
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
    )
    return {
        "token": token or None,
        "chat_id": chat_id or None,
        "proxy": proxy or None,
    }


async def send_telegram_message_async(
    session: aiohttp.ClientSession,
    *,
    text: str,
    notification_settings: dict[str, str | None] | None = None,
    home: Path | None = None,
) -> bool:
    settings = notification_settings or telegram_settings(home)
    token = settings.get("token")
    chat_id = settings.get("chat_id")
    if not token or not chat_id:
        return False
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    async with session.post(
        url,
        data={"chat_id": chat_id, "text": text, "disable_web_page_preview": "true"},
        proxy=settings.get("proxy"),
    ) as response:
        payload = await response.json(content_type=None)
        if response.status >= 400 or not payload.get("ok"):
            raise RuntimeError(f"Telegram send failed: {payload}")
    return True


def send_telegram_message(text: str, home: Path | None = None) -> str | None:
    settings = telegram_settings(home)
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


def humanize_side(value: Any) -> str:
    side = str(value or "").upper()
    if side == "BUY":
        return "做多"
    if side == "SELL":
        return "做空"
    return str(value or "未知")


def humanize_sentiment(value: Any) -> str:
    sentiment = str(value or "").strip().lower()
    mapping = {
        "positive": "正面",
        "negative": "负面",
        "neutral": "中性",
    }
    return mapping.get(sentiment, str(value or "未知"))


def humanize_bias(value: Any) -> str:
    bias = str(value or "").upper()
    mapping = {
        "LONG": "做多",
        "SHORT": "做空",
        "NONE": "观望",
    }
    return mapping.get(bias, str(value or "未知"))
