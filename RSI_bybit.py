#!/usr/bin/env python3
from __future__ import annotations
import asyncio
import copy
import contextlib
import json
import logging
import os
import random
import sys
import shutil
import weakref
from math import ceil
import time
import math
from decimal import Decimal, InvalidOperation
from dataclasses import dataclass
from datetime import datetime
from io import BytesIO
from typing import Awaitable, Callable, Dict, FrozenSet, List, Optional, Tuple, Set, cast
from zoneinfo import ZoneInfo

import aiohttp
from dotenv import load_dotenv
from PIL import Image, ImageDraw, ImageFont

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.error import BadRequest, Forbidden, NetworkError, TimedOut, RetryAfter
from telegram.request import HTTPXRequest
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

# =========================
# Load .env
# =========================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

# =========================
# CONFIG
# =========================
BYBIT_BASE_URL = os.getenv("BYBIT_BASE_URL", "https://api.bybit.com").rstrip("/")
BINANCE_FUTURES_BASE_URL = "https://fapi.binance.com"
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")

RSI_PERIOD = 6
STOCH_RSI_RSI_LEN = int(os.getenv("STOCH_RSI_RSI_LEN", "14"))
STOCH_RSI_STOCH_LEN = int(os.getenv("STOCH_RSI_STOCH_LEN", "14"))
STOCH_RSI_SMOOTH_K = int(os.getenv("STOCH_RSI_SMOOTH_K", "3"))
STOCH_RSI_SMOOTH_D = int(os.getenv("STOCH_RSI_SMOOTH_D", "3"))
STOCH_RSI_USE_CLOSED_CANDLE = os.getenv("STOCH_RSI_USE_CLOSED_CANDLE", "true").strip().lower() in {
    "1",
    "true",
    "yes",
    "y",
    "on",
}
LEVERAGE = float(os.getenv("LEVERAGE", "20"))

# Unified timeframe registry (label -> interval in minutes)
TIMEFRAME_REGISTRY: Dict[str, str] = {
    "5m": "5",
    "15m": "15",
    "30m": "30",
    "1h": "60",
    "4h": "240",
}
TIMEFRAME_BY_INTERVAL = {interval: label for label, interval in TIMEFRAME_REGISTRY.items()}

# RSI timeframes for SUM (now includes 5m)
TIMEFRAMES: List[Tuple[str, str]] = [(label, interval) for label, interval in TIMEFRAME_REGISTRY.items()]

# Alert options (fixed to 5m/15m/30m/1h/4h)
ALERT_TFS: List[Tuple[str, str]] = [
    (label, interval)
    for label, interval in TIMEFRAME_REGISTRY.items()
    if label in {"5m", "15m", "30m", "1h", "4h"}
]
ALERT_INTERVALS = {iv for _, iv in ALERT_TFS}
ALERT_SLOW_TFS = {"60", "240"}
ALERT_SLOW_CHECK_SEC = int(os.getenv("ALERT_SLOW_CHECK_SEC", str(15 * 60)))

STOCH_RSI_TFS: List[Tuple[str, str]] = list(TIMEFRAMES)

PAIR_RSI_LOW_THRESHOLD = float(os.getenv("PAIR_RSI_LOW_THRESHOLD", "15"))
PAIR_RSI_HIGH_THRESHOLD = float(os.getenv("PAIR_RSI_HIGH_THRESHOLD", "90"))
MONITOR_LONG_ALL_MAX = float(os.getenv("MONITOR_LONG_ALL_MAX", "50"))
MONITOR_LONG_FAST_MAX = float(os.getenv("MONITOR_LONG_FAST_MAX", "35"))
MONITOR_SHORT_ALL_MIN = float(os.getenv("MONITOR_SHORT_ALL_MIN", "70"))
MONITOR_SHORT_FAST_MIN = float(os.getenv("MONITOR_SHORT_FAST_MIN", "80"))

ALERT_CHECK_SEC = int(os.getenv("ALERT_CHECK_SEC", "300"))  # check every 5 minutes by default
ALERT_EPS = float(os.getenv("ALERT_EPS", "0.10"))  # "equals threshold" tolerance
ALERT_LONG_THRESHOLD = float(os.getenv("ALERT_LONG_THRESHOLD", "40"))
ALERT_SHORT_THRESHOLD = float(os.getenv("ALERT_SHORT_THRESHOLD", "60"))
PRICE_ALERT_CHECK_SEC = int(os.getenv("PRICE_ALERT_CHECK_SEC", "180"))
ALERT_ERROR_THROTTLE_MIN = int(os.getenv("ALERT_ERROR_THROTTLE_MIN", "30"))
ALERT_ERROR_NOTIFY_TTL_HOURS = float(os.getenv("ALERT_ERROR_NOTIFY_TTL_HOURS", "24"))
ALERT_COOLDOWN_CACHE_TTL_SEC = int(os.getenv("ALERT_COOLDOWN_CACHE_TTL_SEC", str(24 * 3600)))
MAX_ALERTS_PER_CHAT = int(os.getenv("MAX_ALERTS_PER_CHAT", "50"))
MAX_ALERTS_GLOBAL = int(os.getenv("MAX_ALERTS_GLOBAL", "1000"))
ALERT_TTL_HOURS = float(os.getenv("ALERT_TTL_HOURS", "24"))
ALERT_COOLDOWN_SEC = int(os.getenv("ALERT_COOLDOWN_SEC", "0"))
ALERT_MAX_BACKOFF_SEC = int(os.getenv("ALERT_MAX_BACKOFF_SEC", "3600"))
HEAVY_ACTION_COOLDOWN_SEC = int(os.getenv("HEAVY_ACTION_COOLDOWN_SEC", "5"))
SHUTDOWN_TASK_TIMEOUT = int(os.getenv("SHUTDOWN_TASK_TIMEOUT", "10"))
CLEANUP_INTERVAL_SEC = int(os.getenv("CLEANUP_INTERVAL_SEC", "600"))
ALERT_MAX_CONCURRENCY = int(os.getenv("ALERT_MAX_CONCURRENCY", "10"))
BINANCE_SYMBOLS_CACHE_TTL = int(os.getenv("BINANCE_SYMBOLS_CACHE_TTL", "3600"))

MAX_SYMBOLS = int(os.getenv("MAX_SYMBOLS", "200"))
MAX_CONCURRENCY = int(os.getenv("MAX_CONCURRENCY", "20"))  # Increased from 10 for better performance
KLINE_LIMIT = int(os.getenv("KLINE_LIMIT", "120"))

TICKERS_CACHE_TTL = int(os.getenv("TICKERS_CACHE_TTL", "60"))
PERP_SYMBOLS_CACHE_TTL = int(os.getenv("PERP_SYMBOLS_CACHE_TTL", "3600"))
INNOVATION_SYMBOLS_CACHE_TTL = int(os.getenv("INNOVATION_SYMBOLS_CACHE_TTL", str(6 * 3600)))

LOCAL_TZ = ZoneInfo(os.getenv("LOCAL_TZ", "Asia/Ho_Chi_Minh"))

FONT_SIZE = int(os.getenv("FONT_SIZE", "20"))
IMAGE_PADDING = int(os.getenv("IMAGE_PADDING", "26"))
LINE_SPACING = int(os.getenv("LINE_SPACING", "6"))

STATE_FILE = os.getenv("STATE_FILE", "bot_state.json")
if not os.path.isabs(STATE_FILE):
    STATE_FILE = os.path.join(BASE_DIR, STATE_FILE)

PERP_SYMBOLS_FILE = os.getenv("PERP_SYMBOLS_FILE", "perp_symbols.json")
if not os.path.isabs(PERP_SYMBOLS_FILE):
    PERP_SYMBOLS_FILE = os.path.join(BASE_DIR, PERP_SYMBOLS_FILE)

# Subscription interval limits
MIN_INTERVAL_MINUTES = 1
MAX_INTERVAL_MINUTES = 1440

# Error rate threshold for notifications (percentage)
ERROR_RATE_THRESHOLD = 0.3  # 30%

# Global rate limiter settings
RATE_LIMIT_DELAY = 1.0  # Initial delay in seconds
RATE_LIMIT_MAX_DELAY = 60.0  # Maximum delay
RATE_LIMIT_QUEUE_TIMEOUT = 300  # Maximum time to wait in queue (5 minutes)
RATE_LIMIT_MIN_SPACING = float(os.getenv("RATE_LIMIT_MIN_SPACING", "0.1"))
API_GET_REQUEST_TIMEOUT = int(os.getenv("API_GET_REQUEST_TIMEOUT", "20"))

# Timeout for monitor scanning (seconds)
DEFAULT_MONITOR_TIMEOUT = max(
    180,
    int(
        ceil(
            MAX_SYMBOLS
            / max(1, MAX_CONCURRENCY)
            * len(TIMEFRAMES)
            * max(RATE_LIMIT_DELAY, RATE_LIMIT_MIN_SPACING)
        )
    ),
)
MONITOR_TIMEOUT = int(os.getenv("MONITOR_TIMEOUT", str(DEFAULT_MONITOR_TIMEOUT)))
MAX_MONITOR_TIMEOUT = int(os.getenv("MAX_MONITOR_TIMEOUT", str(60 * 60)))

# =========================
# CONFIG VALIDATION
# =========================
def validate_config() -> None:
    errors = []

    if not TELEGRAM_BOT_TOKEN:
        errors.append("TELEGRAM_BOT_TOKEN is required")
    if not BYBIT_BASE_URL:
        errors.append("BYBIT_BASE_URL must not be empty")
    if LEVERAGE <= 0:
        errors.append("LEVERAGE must be > 0")
    if ALERT_CHECK_SEC <= 0:
        errors.append("ALERT_CHECK_SEC must be > 0")
    if ALERT_SLOW_CHECK_SEC <= 0:
        errors.append("ALERT_SLOW_CHECK_SEC must be > 0")
    if PRICE_ALERT_CHECK_SEC <= 0:
        errors.append("PRICE_ALERT_CHECK_SEC must be > 0")
    if ALERT_ERROR_THROTTLE_MIN <= 0:
        errors.append("ALERT_ERROR_THROTTLE_MIN must be > 0")
    if ALERT_ERROR_NOTIFY_TTL_HOURS <= 0:
        errors.append("ALERT_ERROR_NOTIFY_TTL_HOURS must be > 0")
    if SHUTDOWN_TASK_TIMEOUT <= 0:
        errors.append("SHUTDOWN_TASK_TIMEOUT must be > 0")
    if MAX_ALERTS_PER_CHAT <= 0:
        errors.append("MAX_ALERTS_PER_CHAT must be > 0")
    if MAX_ALERTS_GLOBAL <= 0:
        errors.append("MAX_ALERTS_GLOBAL must be > 0")
    if ALERT_TTL_HOURS <= 0:
        errors.append("ALERT_TTL_HOURS must be > 0")
    if ALERT_COOLDOWN_SEC < 0:
        errors.append("ALERT_COOLDOWN_SEC must be >= 0")
    if ALERT_MAX_BACKOFF_SEC <= 0:
        errors.append("ALERT_MAX_BACKOFF_SEC must be > 0")
    if ALERT_MAX_CONCURRENCY <= 0:
        errors.append("ALERT_MAX_CONCURRENCY must be > 0")
    if HEAVY_ACTION_COOLDOWN_SEC < 0:
        errors.append("HEAVY_ACTION_COOLDOWN_SEC must be >= 0")
    if BINANCE_SYMBOLS_CACHE_TTL <= 0:
        errors.append("BINANCE_SYMBOLS_CACHE_TTL must be > 0")
    if MAX_SYMBOLS <= 0:
        errors.append("MAX_SYMBOLS must be > 0")
    if MAX_CONCURRENCY <= 0:
        errors.append("MAX_CONCURRENCY must be > 0")
    if KLINE_LIMIT <= 0:
        errors.append("KLINE_LIMIT must be > 0")
    if MAX_MONITOR_TIMEOUT <= 0:
        errors.append("MAX_MONITOR_TIMEOUT must be > 0")
    if RSI_PERIOD <= 0:
        errors.append("RSI_PERIOD must be > 0")
    if STOCH_RSI_RSI_LEN <= 0:
        errors.append("STOCH_RSI_RSI_LEN must be > 0")
    if STOCH_RSI_STOCH_LEN <= 0:
        errors.append("STOCH_RSI_STOCH_LEN must be > 0")
    if STOCH_RSI_SMOOTH_K <= 0:
        errors.append("STOCH_RSI_SMOOTH_K must be > 0")
    if STOCH_RSI_SMOOTH_D <= 0:
        errors.append("STOCH_RSI_SMOOTH_D must be > 0")
    if not (0 <= PAIR_RSI_LOW_THRESHOLD < PAIR_RSI_HIGH_THRESHOLD <= 100):
        errors.append("PAIR_RSI thresholds must be 0 <= low < high <= 100")
    if not (0 <= MONITOR_LONG_ALL_MAX <= 100):
        errors.append("MONITOR_LONG_ALL_MAX must be within 0..100")
    if not (0 <= MONITOR_LONG_FAST_MAX <= 100):
        errors.append("MONITOR_LONG_FAST_MAX must be within 0..100")
    if not (0 <= MONITOR_SHORT_ALL_MIN <= 100):
        errors.append("MONITOR_SHORT_ALL_MIN must be within 0..100")
    if not (0 <= MONITOR_SHORT_FAST_MIN <= 100):
        errors.append("MONITOR_SHORT_FAST_MIN must be within 0..100")
    if RATE_LIMIT_DELAY <= 0:
        errors.append("RATE_LIMIT_DELAY must be > 0")
    if RATE_LIMIT_MAX_DELAY < RATE_LIMIT_DELAY:
        errors.append("RATE_LIMIT_MAX_DELAY must be >= RATE_LIMIT_DELAY")
    if RATE_LIMIT_QUEUE_TIMEOUT <= 0:
        errors.append("RATE_LIMIT_QUEUE_TIMEOUT must be > 0")
    if RATE_LIMIT_MIN_SPACING <= 0:
        errors.append("RATE_LIMIT_MIN_SPACING must be > 0")
    if API_GET_REQUEST_TIMEOUT <= 0:
        errors.append("API_GET_REQUEST_TIMEOUT must be > 0")
    if CLEANUP_INTERVAL_SEC <= 0:
        errors.append("CLEANUP_INTERVAL_SEC must be > 0")
    if MONITOR_TIMEOUT <= 0:
        errors.append("MONITOR_TIMEOUT must be > 0")
    if INNOVATION_SYMBOLS_CACHE_TTL <= 0:
        errors.append("INNOVATION_SYMBOLS_CACHE_TTL must be > 0")

    if errors:
        msg = "Config validation failed:\n- " + "\n- ".join(errors)
        raise RuntimeError(msg)

# =========================
# LOGGING
# =========================
_LOG_RECORD_ATTRS = {
    "args",
    "asctime",
    "created",
    "exc_info",
    "exc_text",
    "filename",
    "funcName",
    "levelname",
    "levelno",
    "lineno",
    "module",
    "msecs",
    "message",
    "msg",
    "name",
    "pathname",
    "process",
    "processName",
    "relativeCreated",
    "stack_info",
    "thread",
    "threadName",
}


class JSONFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "time": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        extra = {k: v for k, v in record.__dict__.items() if k not in _LOG_RECORD_ATTRS}
        if extra:
            payload["extra"] = extra
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False)


_handler = logging.StreamHandler()
_handler.setFormatter(JSONFormatter())
logging.basicConfig(level=logging.INFO, handlers=[_handler])
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("telegram.request").setLevel(logging.WARNING)

# =========================
# STATE / STORAGE
# =========================
# {
#   "subs": { "<chat_id>": { "interval_min": 15, "enabled": true } },
#   "alerts": {
#       "<chat_id>": {
#           "BTCUSDT|15": {
#               "symbol":"BTCUSDT",
#               "tf":"15",
#               "tf_label":"15m",
#               "side":"L",
#               "last_above": true/false/null,
#               "created_at": 1717000000.0
#           }
#       }
#   }
# }
def load_state() -> dict:
    try:
        if os.path.exists(STATE_FILE):
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                state = json.load(f)
            if validate_state(state):
                return state
            bad_name = f"{STATE_FILE}.bad-{int(time.time())}"
            os.replace(STATE_FILE, bad_name)
            logging.warning("Invalid state file moved to %s", bad_name)
    except Exception as e:
        logging.warning("Failed to load state: %s", e)
    return {"subs": {}, "alerts": {}, "pair_rsi_alerts": {}, "price_alerts": {}}


def save_state_json(state_json: str) -> bool:
    tmp = None
    try:
        encoded = state_json.encode("utf-8")
        base_dir = os.path.dirname(STATE_FILE) or "."
        try:
            free_bytes = shutil.disk_usage(base_dir).free
            if free_bytes < len(encoded) + 1024:
                logging.error("Failed to save state: insufficient disk space")
                return False
        except Exception as e:
            logging.warning("Failed to check free disk space: %s", e)
        tmp = STATE_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(state_json)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, STATE_FILE)
        logging.debug("State saved to %s (%d bytes)", STATE_FILE, len(encoded))
        return True
    except OSError as e:
        if e.errno == 28:
            logging.error("Failed to save state: no space left on device")
            with contextlib.suppress(Exception):
                if os.path.exists(tmp):
                    os.remove(tmp)
            return False
        logging.warning("Failed to save state: %s", e)
    except Exception as e:
        logging.warning("Failed to save state: %s", e)
    return False


async def save_state_json_with_retry(state_json: str, attempts: int = 3) -> bool:
    delay = 0.2
    for _ in range(attempts):
        if save_state_json(state_json):
            return True
        await asyncio.sleep(delay)
        delay *= 2
    return False


def _json_default(value: object) -> object:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, set):
        return sorted(value)
    raise TypeError(f"Not JSON serializable: {type(value).__name__}")


def save_state(state: dict) -> None:
    try:
        state_json = json.dumps(state, ensure_ascii=False, indent=2, default=_json_default)
    except Exception as e:
        logging.warning("Failed to serialize state: %s", e)
        return
    save_state_json(state_json)


def load_perp_symbols() -> Set[str]:
    if not os.path.exists(PERP_SYMBOLS_FILE):
        return set()
    try:
        with open(PERP_SYMBOLS_FILE, "r", encoding="utf-8") as f:
            payload = json.load(f)
        if not isinstance(payload, list) or not all(isinstance(item, str) for item in payload):
            logging.warning("Invalid perp symbols cache format, ignoring")
            return set()
        return set(payload)
    except Exception as e:
        logging.warning("Failed to load perp symbols cache: %s", e)
        return set()


def save_perp_symbols(symbols: Set[str]) -> None:
    try:
        payload = json.dumps(sorted(symbols), ensure_ascii=False, indent=2)
    except Exception as e:
        logging.warning("Failed to serialize perp symbols cache: %s", e)
        return
    tmp = PERP_SYMBOLS_FILE + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(payload)
        os.replace(tmp, PERP_SYMBOLS_FILE)
    except Exception as e:
        logging.warning("Failed to save perp symbols cache: %s", e)
        with contextlib.suppress(Exception):
            if os.path.exists(tmp):
                os.remove(tmp)


async def persist_state(app: Application, state: dict) -> None:
    save_event: asyncio.Event = app.bot_data["state_save_event"]
    save_done: asyncio.Event = app.bot_data["state_save_done"]
    seq_lock: asyncio.Lock = app.bot_data["state_save_seq_lock"]
    async with seq_lock:
        app.bot_data["state_save_seq"] += 1
        app.bot_data["state_save_last_requested_seq"] = app.bot_data["state_save_seq"]
        save_done.clear()
        save_event.set()


async def state_save_worker(app: Application) -> None:
    try:
        save_event: asyncio.Event = app.bot_data["state_save_event"]
        save_done: asyncio.Event = app.bot_data["state_save_done"]
        seq_lock: asyncio.Lock = app.bot_data["state_save_seq_lock"]
        while True:
            await save_event.wait()
            save_event.clear()
            async with seq_lock:
                seq_before = app.bot_data["state_save_seq"]
            state_lock: asyncio.Lock = app.bot_data["state_lock"]
            async with state_lock:
                snapshot = copy.deepcopy(
                    app.bot_data.get(
                        "state",
                        {"subs": {}, "alerts": {}, "pair_rsi_alerts": {}, "price_alerts": {}},
                    )
                )
            try:
                state_json = json.dumps(snapshot, ensure_ascii=False, indent=2, default=_json_default)
            except Exception as e:
                logging.warning("Failed to serialize state: %s", e)
                continue
            write_lock: asyncio.Lock = app.bot_data["state_write_lock"]
            async with write_lock:
                ok = await save_state_json_with_retry(state_json, attempts=3)
            if not ok:
                await asyncio.sleep(2)
                save_event.set()
                continue
            async with seq_lock:
                seq_after = app.bot_data["state_save_seq"]
            if seq_after == seq_before:
                save_done.set()
    except asyncio.CancelledError:
        save_done: asyncio.Event = app.bot_data["state_save_done"]
        save_done.set()
        raise


async def flush_state(app: Application) -> None:
    await persist_state(app, app.bot_data.get("state") or {})
    save_done: asyncio.Event = app.bot_data["state_save_done"]
    with contextlib.suppress(asyncio.TimeoutError, asyncio.CancelledError):
        await asyncio.wait_for(save_done.wait(), timeout=SHUTDOWN_TASK_TIMEOUT)


def log_state_change(state: dict, op: str, **fields: object) -> None:
    subs_total = len(state.get("subs", {}) or {})
    alerts_total = sum(len(v) for v in (state.get("alerts", {}) or {}).values())
    pair_total = sum(len(v) for v in (state.get("pair_rsi_alerts", {}) or {}).values())
    price_total = sum(len(v) for v in (state.get("price_alerts", {}) or {}).values())
    details = " ".join(f"{key}={value}" for key, value in fields.items())
    logging.debug(
        "state_op=%s %s subs_total=%d alerts_total=%d pair_rsi_total=%d price_alerts_total=%d",
        op,
        details,
        subs_total,
        alerts_total,
        pair_total,
        price_total,
    )


def validate_state(state: object) -> bool:
    if not isinstance(state, dict):
        return False
    subs = state.get("subs", {})
    alerts = state.get("alerts", {})
    pair_alerts = state.get("pair_rsi_alerts", {})
    price_alerts = state.get("price_alerts", {})
    if not isinstance(subs, dict) or not isinstance(alerts, dict) or not isinstance(pair_alerts, dict):
        return False
    if not isinstance(price_alerts, dict):
        return False
    for chat_id, sub in subs.items():
        if not isinstance(chat_id, str) or not isinstance(sub, dict):
            return False
        interval = sub.get("interval_min")
        if not isinstance(interval, int) or not validate_interval(interval):
            return False
    for chat_id, amap in alerts.items():
        if not isinstance(chat_id, str) or not isinstance(amap, dict):
            return False
        for _, alert in amap.items():
            if not isinstance(alert, dict):
                return False
            if not isinstance(alert.get("symbol"), str):
                return False
            if not isinstance(alert.get("tf"), str):
                return False
            if alert.get("side") not in {"L", "S"}:
                return False
            last_above = alert.get("last_above")
            if last_above is not None and not isinstance(last_above, bool):
                return False
    for chat_id, amap in pair_alerts.items():
        if not isinstance(chat_id, str) or not isinstance(amap, dict):
            return False
        for _, alert in amap.items():
            if not isinstance(alert, dict):
                return False
            if not isinstance(alert.get("symbol"), str):
                return False
            if not isinstance(alert.get("tfs"), list):
                return False
            if alert.get("mode") not in {"LOW", "HIGH"}:
                return False
    for chat_id, amap in price_alerts.items():
        if not isinstance(chat_id, str) or not isinstance(amap, dict):
            return False
        for _, alert in amap.items():
            if not isinstance(alert, dict):
                return False
            if not isinstance(alert.get("symbol"), str):
                return False
            price = alert.get("price")
            if not isinstance(price, (int, float)):
                return False
            if alert.get("direction") not in {"UP", "DOWN"}:
                return False
    return True


def get_sub_job_name(chat_id: int) -> str:
    return f"rsi_sub:{chat_id}"


def get_alert_job_name(chat_id: int, symbol: str, tf: str) -> str:
    return f"rsi_alert:{chat_id}:{symbol}:{tf}"


def alert_check_interval_sec(tf: str) -> int:
    if tf in ALERT_SLOW_TFS:
        return ALERT_SLOW_CHECK_SEC
    return ALERT_CHECK_SEC


def get_pair_rsi_alert_job_name(chat_id: int) -> str:
    return f"pair_rsi_alert:{chat_id}"


def get_price_alert_job_name(chat_id: int, key: str) -> str:
    return f"price_alert:{chat_id}:{key}"


# =========================
# INPUT VALIDATION
# =========================
def validate_chat_id(chat_id: int) -> bool:
    """Validate chat_id to prevent injection attacks."""
    return isinstance(chat_id, int) and abs(chat_id) < 10**15


async def get_valid_symbols_with_fallback(app: Application) -> Optional[Set[str]]:
    """Return valid symbols set, refreshing cache if missing."""
    perp_cache = app.bot_data.get("perp_symbols_cache")
    if perp_cache and perp_cache.value:
        base_symbols = set(perp_cache.value)
        session: aiohttp.ClientSession = app.bot_data["http_session"]
        rate_limiter: RateLimiter = app.bot_data["rate_limiter"]
        innovation_symbols = await get_innovation_zone_symbols(app, session, rate_limiter)
        return base_symbols.union(innovation_symbols)

    try:
        symbols = await get_cached_perp_symbols(app)
    except Exception as e:
        logging.warning("Failed to refresh perp symbols cache: %s", e)
        return None

    session: aiohttp.ClientSession = app.bot_data["http_session"]
    rate_limiter: RateLimiter = app.bot_data["rate_limiter"]
    innovation_symbols = await get_innovation_zone_symbols(app, session, rate_limiter)
    return set(symbols).union(innovation_symbols)


def validate_symbol(symbol: str, valid_symbols: Optional[Set[str]] = None) -> bool:
    """Validate symbol to prevent injection attacks.
    
    Args:
        symbol: Symbol to validate
        valid_symbols: Optional whitelist of valid symbols from Bybit API
    """
    if not isinstance(symbol, str) or len(symbol) > 30 or len(symbol) < 3:
        return False
    
    # Bybit format: only uppercase alphanumeric, must end with USDT
    if not symbol.isupper() or not symbol.endswith("USDT"):
        return False
    
    # Only alphanumeric characters allowed (no dashes or underscores in Bybit perpetuals)
    if not symbol.isalnum():
        return False
    
    # If whitelist provided, check against it
    if valid_symbols is not None:
        return symbol in valid_symbols
    
    return True


def validate_interval(interval_min: int) -> bool:
    """Validate subscription interval."""
    return MIN_INTERVAL_MINUTES <= interval_min <= MAX_INTERVAL_MINUTES


def normalize_symbol(raw: str) -> Optional[str]:
    """Normalize user-entered symbol into Bybit linear USDT format."""
    if not isinstance(raw, str):
        return None
    raw_cleaned = raw.strip().upper()
    if not raw_cleaned or len(raw_cleaned) > 30:
        return None
    if not raw_cleaned.isalnum():
        return None
    if not raw_cleaned.endswith("USDT"):
        return None
    return raw_cleaned


def parse_price_input(raw: str) -> Optional[Decimal]:
    if not isinstance(raw, str):
        return None
    cleaned = raw.strip().replace(",", ".")
    if not cleaned:
        return None
    try:
        value = Decimal(cleaned)
    except (InvalidOperation, ValueError):
        return None
    if not value.is_finite() or value <= 0:
        return None
    return value


def normalize_price_text(value: Decimal) -> str:
    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def get_required_bot_data(
    app: Application,
    key: str,
    default_factory: Optional[Callable[[], object]] = None,
) -> object:
    value = app.bot_data.get(key)
    if value is not None:
        return value
    if default_factory is not None:
        value = default_factory()
        app.bot_data[key] = value
        return value
    raise RuntimeError(f"Missing required bot_data key: {key}")


def log_event(level: int, message: str, **fields: object) -> None:
    logging.log(level, message, extra=fields)


async def tg_call_with_retry(
    call: Callable[..., Awaitable[object]],
    *args: object,
    **kwargs: object,
) -> object:
    attempts = 4
    backoff = 0.8
    for attempt in range(1, attempts + 1):
        try:
            return await call(*args, **kwargs)
        except RetryAfter as e:
            jitter = random.uniform(0.2, 0.6)
            await asyncio.sleep(e.retry_after + jitter)
        except (TimedOut, NetworkError):
            if attempt >= attempts:
                raise
            jitter = random.uniform(0.2, 0.6)
            await asyncio.sleep(backoff + jitter)
            backoff = min(backoff * 2, 10.0)
    return None


async def check_action_cooldown(
    app: Application,
    chat_id: int,
    action_key: str,
    cooldown_sec: int,
) -> float:
    if cooldown_sec <= 0:
        return 0.0
    cooldowns = app.bot_data.setdefault("heavy_action_cooldowns", {})
    lock: asyncio.Lock = app.bot_data.setdefault("heavy_action_cooldowns_lock", asyncio.Lock())
    now = time.time()
    remaining = 0.0
    async with lock:
        last_ts = cooldowns.get((chat_id, action_key), 0.0)
        remaining = cooldown_sec - (now - last_ts)
        if remaining <= 0:
            cooldowns[(chat_id, action_key)] = now
            return 0.0
    return remaining


def needs_price_confirmation(current_price: float, target_price: Decimal) -> bool:
    if current_price <= 0:
        return False
    target_value = float(target_price)
    if current_price < 1:
        ratio = target_value / current_price
        return ratio >= 10 or ratio <= 0.1
    return False


# =========================
# BYBIT HTTP
# =========================
@dataclass
class CacheItem:
    ts: float
    value: object


class BybitAPIError(Exception):
    pass


class RateLimitError(BybitAPIError):
    pass


class BinanceAPIError(Exception):
    pass


BINANCE_SYMBOLS_CACHE: Optional[CacheItem] = None
BINANCE_SYMBOLS_CACHE_LOCK: Optional[asyncio.Lock] = None


class RateLimiter:
    """Global rate limiter with dynamic backoff and queue timeout."""
    
    def __init__(self):
        self._lock = asyncio.Lock()
        self._delay = RATE_LIMIT_DELAY
        self._last_request = 0.0
        self._rate_limited = False
        self._success_streak = 0
    
    async def acquire(self):
        """Acquire permission to make a request with timeout."""
        try:
            async with asyncio.timeout(RATE_LIMIT_QUEUE_TIMEOUT):
                async with self._lock:
                    now = time.time()
                    base_spacing = RATE_LIMIT_MIN_SPACING
                    target_delay = self._delay if self._rate_limited else base_spacing
                    wait_time = target_delay - (now - self._last_request)
                    if wait_time > 0:
                        jitter = random.uniform(0.7, 1.3) if self._rate_limited else 1.0
                        await asyncio.sleep(wait_time * jitter)
                    self._last_request = time.time()
        except asyncio.TimeoutError:
            raise BybitAPIError(f"Rate limiter queue timeout after {RATE_LIMIT_QUEUE_TIMEOUT}s")
    
    async def report_rate_limit(self):
        """Report that a rate limit was hit."""
        async with self._lock:
            self._rate_limited = True
            self._delay = min(self._delay * 2, RATE_LIMIT_MAX_DELAY)
            self._success_streak = 0
            logging.warning("Rate limit hit, increasing delay to %.2fs", self._delay)
    
    async def report_success(self):
        """Report a successful request."""
        async with self._lock:
            if self._rate_limited:
                self._success_streak += 1
                if self._success_streak >= 3:
                    self._delay = max(self._delay * 0.5, RATE_LIMIT_DELAY)
                else:
                    # Gradually decrease delay on success
                    self._delay = max(self._delay * 0.8, RATE_LIMIT_DELAY)
                if self._delay <= RATE_LIMIT_DELAY:
                    self._rate_limited = False
                    self._success_streak = 0
                    logging.info("Rate limiter reset")

    async def get_current_delay(self) -> float:
        """Return the current delay used by the limiter."""
        async with self._lock:
            return self._delay


async def api_get_json(
    session: aiohttp.ClientSession,
    url: str,
    params: Dict[str, str],
    rate_limiter: RateLimiter,
    retries: int = 5,
    raise_on_rate_limit: bool = False,
) -> dict:
    backoff = 0.8
    last_err: Optional[Exception] = None
    attempt = 0

    while attempt < retries:
        try:
            await rate_limiter.acquire()

            async with session.get(
                url,
                params=params,
                timeout=aiohttp.ClientTimeout(total=API_GET_REQUEST_TIMEOUT),
            ) as resp:
                if resp.status == 429:
                    await rate_limiter.report_rate_limit()
                    raise RateLimitError("Bybit rate limit: HTTP 429 (Too many requests)")
                data = await resp.json(content_type=None)

                if not isinstance(data, dict):
                    raise BybitAPIError(
                        f"Unexpected response payload type: {type(data).__name__}"
                    )

                # Common rate limit code
                if data.get("retCode") == 10006:
                    await rate_limiter.report_rate_limit()
                    raise RateLimitError("Bybit rate limit: retCode=10006 (Too many visits!)")

                if resp.status >= 400:
                    raise BybitAPIError(f"HTTP {resp.status}: {data}")

                if data.get("retCode") not in (0, None):
                    raise BybitAPIError(
                        f"Bybit retCode={data.get('retCode')} retMsg={data.get('retMsg')}"
                    )

                # Report success to gradually reduce delays
                await rate_limiter.report_success()
                return data

        except RateLimitError as e:
            last_err = e
            if raise_on_rate_limit:
                raise
            attempt += 1
            if attempt >= retries:
                break
            await asyncio.sleep(backoff)
            backoff *= 1.7
        except (asyncio.TimeoutError, aiohttp.ClientError, BybitAPIError, OSError, ConnectionError) as e:
            last_err = e
            attempt += 1
            if attempt >= retries:
                break
            await asyncio.sleep(backoff)
            backoff *= 1.7

    raise BybitAPIError(f"Request failed after retries: {last_err}")


# =========================
# BYBIT DATA: Perpetual symbols only
# =========================
async def get_all_usdt_linear_perp_symbols(
    session: aiohttp.ClientSession,
    rate_limiter: RateLimiter,
    innovation_symbols: Optional[Set[str]] = None,
) -> List[str]:
    """
    Only LinearPerpetual (USDT-settled), status Trading, quoteCoin USDT.
    """
    url = f"{BYBIT_BASE_URL}/v5/market/instruments-info"
    cursor = ""
    out: List[str] = []

    while True:
        params: Dict[str, str] = {"category": "linear", "limit": "1000"}
        if cursor:
            params["cursor"] = cursor

        payload = await api_get_json(session, url, params, rate_limiter)
        result = payload.get("result") or {}
        items = result.get("list") or []

        for it in items:
            try:
                symbol = it.get("symbol", "")
                contract_type = it.get("contractType", "")
                status = it.get("status", "")
                settle = it.get("settleCoin", "")
                quote = it.get("quoteCoin", "")

                if (
                    contract_type == "LinearPerpetual"
                    and status == "Trading"
                    and settle == "USDT"
                    and quote == "USDT"
                ):
                    out.append(symbol)
            except Exception:
                continue

        cursor = result.get("nextPageCursor") or ""
        if not cursor:
            break

    symbols = sorted(set(out))
    if innovation_symbols:
        total = len(symbols)
        symbols = [s for s in symbols if s not in innovation_symbols]
        excluded = total - len(symbols)
        logging.info(
            "Filtered Innovation Zone symbols: total=%d excluded=%d remaining=%d",
            total,
            excluded,
            len(symbols),
        )
    return symbols


async def get_linear_tickers(session: aiohttp.ClientSession, rate_limiter: RateLimiter, symbol: Optional[str] = None) -> List[dict]:
    url = f"{BYBIT_BASE_URL}/v5/market/tickers"
    params = {"category": "linear"}
    if symbol:
        params["symbol"] = symbol
    payload = await api_get_json(session, url, params, rate_limiter)
    return (payload.get("result") or {}).get("list") or []


async def pick_top_symbols_by_turnover(
    session: aiohttp.ClientSession,
    rate_limiter: RateLimiter,
    perp_symbols_set: set,
    limit: int,
    innovation_symbols: Optional[Set[str]] = None,
) -> List[str]:
    tickers = await get_linear_tickers(session, rate_limiter)
    rows: List[Tuple[str, float]] = []

    for it in tickers:
        sym = it.get("symbol", "")
        if sym not in perp_symbols_set:
            continue
        if innovation_symbols and sym in innovation_symbols:
            continue
        try:
            turnover = float(it.get("turnover24h") or 0.0)
        except Exception:
            turnover = 0.0
        rows.append((sym, turnover))

    rows.sort(key=lambda x: x[1], reverse=True)
    return [s for s, _ in rows[:limit]]


# =========================
# BYBIT DATA: Kline + RSI
# =========================
BYBIT_TO_BINANCE_INTERVAL = {
    "1": "1m",
    "5": "5m",
    "15": "15m",
    "30": "30m",
    "60": "1h",
    "120": "2h",
    "240": "4h",
}


async def binance_get_klines(
    session: aiohttp.ClientSession,
    symbol: str,
    bybit_interval: str,
    limit: int,
    *,
    timeout_sec: int = API_GET_REQUEST_TIMEOUT,
    max_retries: int = 3,
) -> List[list]:
    binance_interval = BYBIT_TO_BINANCE_INTERVAL.get(bybit_interval)
    if not binance_interval:
        raise BinanceAPIError(f"Unsupported Binance interval mapping: {bybit_interval}")
    url = f"{BINANCE_FUTURES_BASE_URL}/fapi/v1/klines"
    params = {"symbol": symbol, "interval": binance_interval, "limit": str(limit)}
    data = None
    backoff = 1.0
    for attempt in range(1, max_retries + 1):
        try:
            async with session.get(
                url,
                params=params,
                timeout=aiohttp.ClientTimeout(total=timeout_sec),
            ) as resp:
                if resp.status in (418, 429):
                    retry_after = float(resp.headers.get("Retry-After") or 0)
                    sleep_for = max(backoff, retry_after, 1.0)
                    if attempt >= max_retries:
                        raise RateLimitError(
                            f"Binance rate limit (HTTP {resp.status}), retry_after={retry_after}"
                        )
                    await asyncio.sleep(sleep_for)
                    backoff = min(backoff * 2, 60.0)
                    continue
                if resp.status != 200:
                    text = await resp.text()
                    raise BinanceAPIError(
                        f"HTTP {resp.status} while fetching Binance klines: {text[:200]}"
                    )
                data = await resp.json(content_type=None)
                break
        except (asyncio.TimeoutError, aiohttp.ClientError, OSError):
            if attempt >= max_retries:
                raise
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30.0)
    if not isinstance(data, list):
        raise BinanceAPIError(f"Unexpected Binance payload type: {type(data).__name__}")
    if not data:
        return []
    for item in data:
        if not isinstance(item, (list, tuple)) or len(item) < 6:
            raise BinanceAPIError("Malformed Binance kline payload")
    return data


async def binance_get_closes(
    session: aiohttp.ClientSession,
    symbol: str,
    bybit_interval: str,
    limit: int,
) -> List[float]:
    rows = await binance_get_klines(session, symbol, bybit_interval, limit)
    if not rows:
        raise BinanceAPIError(
            f"No Binance kline rows for {symbol} interval={bybit_interval} limit={limit}"
        )
    closes: List[float] = []
    for c in rows:
        try:
            value = float(c[4])
            if not math.isfinite(value):
                continue
            closes.append(value)
        except Exception:
            continue
    if not closes:
        raise BinanceAPIError(
            f"No valid Binance closes for {symbol} interval={bybit_interval} limit={limit}"
        )
    return closes


async def get_binance_futures_symbols(session: aiohttp.ClientSession) -> FrozenSet[str]:
    global BINANCE_SYMBOLS_CACHE
    global BINANCE_SYMBOLS_CACHE_LOCK

    if BINANCE_SYMBOLS_CACHE_LOCK is None:
        BINANCE_SYMBOLS_CACHE_LOCK = asyncio.Lock()

    async with BINANCE_SYMBOLS_CACHE_LOCK:
        now = time.time()
        if BINANCE_SYMBOLS_CACHE and (now - BINANCE_SYMBOLS_CACHE.ts) <= BINANCE_SYMBOLS_CACHE_TTL:
            return BINANCE_SYMBOLS_CACHE.value  # type: ignore[return-value]

        url = f"{BINANCE_FUTURES_BASE_URL}/fapi/v1/exchangeInfo"
        try:
            data = None
            backoff = 1.0
            for attempt in range(1, 4):
                try:
                    async with session.get(
                        url,
                        timeout=aiohttp.ClientTimeout(total=API_GET_REQUEST_TIMEOUT),
                    ) as resp:
                        if resp.status != 200:
                            text = await resp.text()
                            raise BinanceAPIError(
                                f"HTTP {resp.status} while fetching Binance futures symbols: {text[:200]}"
                            )
                        data = await resp.json(content_type=None)
                        break
                except (asyncio.TimeoutError, aiohttp.ClientError, OSError):
                    if attempt >= 3:
                        raise
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, 30.0)

            symbols: Set[str] = set()
            for item in (data or {}).get("symbols", []):
                if not isinstance(item, dict):
                    continue
                if item.get("contractType") != "PERPETUAL":
                    continue
                if item.get("status") not in {"TRADING", "PRE_TRADING"}:
                    continue
                sym = item.get("symbol")
                if isinstance(sym, str):
                    symbols.add(sym)

            if not symbols:
                raise BinanceAPIError("No Binance futures symbols found in exchangeInfo")
        except Exception as exc:
            if BINANCE_SYMBOLS_CACHE:
                logging.warning(
                    "Failed to refresh Binance futures symbols cache, using stale data: %s",
                    exc,
                )
                return BINANCE_SYMBOLS_CACHE.value  # type: ignore[return-value]
            raise

        frozen_symbols = frozenset(symbols)
        BINANCE_SYMBOLS_CACHE = CacheItem(ts=now, value=frozen_symbols)
        return frozen_symbols


async def get_kline_rows(
    session: aiohttp.ClientSession,
    rate_limiter: RateLimiter,
    symbol: str,
    interval: str,
    limit: int,
) -> List[List[str]]:
    url = f"{BYBIT_BASE_URL}/v5/market/kline"
    params = {
        "category": "linear",
        "symbol": symbol,
        "interval": interval,
        "limit": str(limit),
    }
    payload = await api_get_json(
        session,
        url,
        params,
        rate_limiter,
        raise_on_rate_limit=True,
    )
    rows = ((payload.get("result") or {}).get("list")) or []
    # API returns newest first
    return rows


async def get_kline_closes(
    session: aiohttp.ClientSession,
    rate_limiter: RateLimiter,
    symbol: str,
    interval: str,
    limit: int,
) -> List[float]:
    try:
        rows = await get_kline_rows(session, rate_limiter, symbol, interval, limit)
        if not rows:
            raise BybitAPIError(f"No kline rows for {symbol} interval={interval} limit={limit}")
    except RateLimitError as e:
        tf_label = TIMEFRAME_BY_INTERVAL.get(interval, interval)
        logging.warning(
            "Bybit rate limit for RSI klines (%s, %s). Falling back to Binance.",
            symbol,
            tf_label,
        )
        try:
            binance_symbols = await get_binance_futures_symbols(session)
        except Exception as symbols_err:
            logging.warning(
                "Failed to load Binance futures symbols for fallback (%s): %s",
                symbol,
                symbols_err,
            )
            raise e
        if symbol not in binance_symbols:
            logging.warning(
                "Binance futures symbol not found for %s; skipping fallback.",
                symbol,
            )
            raise e
        try:
            return await binance_get_closes(session, symbol, interval, limit)
        except Exception as binance_err:
            logging.warning(
                "Binance fallback failed for %s (%s): %s",
                symbol,
                tf_label,
                binance_err,
            )
            raise e
    closes_with_ts: List[Tuple[float, float]] = []
    for c in rows:
        # [startTime, open, high, low, close, volume, turnover]
        if not isinstance(c, list) or len(c) < 5:
            continue
        try:
            ts = float(c[0])
            value = float(c[4])
            if not math.isfinite(ts) or not math.isfinite(value):
                continue
            closes_with_ts.append((ts, value))
        except Exception:
            continue
    closes_with_ts.sort(key=lambda item: item[0])
    if STOCH_RSI_USE_CLOSED_CANDLE and closes_with_ts:
        try:
            interval_minutes = int(interval)
        except ValueError:
            interval_minutes = 0
        if interval_minutes > 0:
            candle_ms = interval_minutes * 60 * 1000
            now_ms = time.time() * 1000
            last_ts = closes_with_ts[-1][0]
            if now_ms < last_ts + candle_ms:
                closes_with_ts.pop()
    closes = [value for _, value in closes_with_ts]
    return closes


def rsi_wilder(closes: List[float], period: int) -> Optional[float]:
    if not closes:
        return None
    if len(closes) < period + 1:
        return None

    gains = []
    losses = []
    for i in range(1, len(closes)):
        diff = closes[i] - closes[i - 1]
        gains.append(diff if diff > 0 else 0.0)
        losses.append(-diff if diff < 0 else 0.0)

    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period

    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period

    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def rsi_wilder_series(closes: List[float], period: int) -> List[float]:
    if not closes or len(closes) < period + 1:
        return []

    gains = []
    losses = []
    for i in range(1, len(closes)):
        diff = closes[i] - closes[i - 1]
        gains.append(diff if diff > 0 else 0.0)
        losses.append(-diff if diff < 0 else 0.0)

    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period

    rsi_values: List[float] = []
    if avg_loss == 0:
        rsi_values.append(100.0)
    else:
        rs = avg_gain / avg_loss
        rsi_values.append(100.0 - (100.0 / (1.0 + rs)))

    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
        if avg_loss == 0:
            rsi_values.append(100.0)
        else:
            rs = avg_gain / avg_loss
            rsi_values.append(100.0 - (100.0 / (1.0 + rs)))

    return rsi_values


def sma_series(values: List[float], length: int) -> List[float]:
    if length <= 0:
        return []
    if length == 1:
        return list(values)
    if len(values) < length:
        return []
    window_sum = sum(values[:length])
    out = [window_sum / length]
    for i in range(length, len(values)):
        window_sum += values[i] - values[i - length]
        out.append(window_sum / length)
    return out


def stoch_rsi_kd_from_closes(
    closes: List[float],
    rsi_period: int,
    stoch_period: int,
    smooth_k: int,
    smooth_d: int,
) -> Optional[Tuple[float, float]]:
    rsi_values = rsi_wilder_series(closes, rsi_period)
    if len(rsi_values) < stoch_period:
        return None

    raw_values: List[float] = []
    for i in range(stoch_period - 1, len(rsi_values)):
        window = rsi_values[i - stoch_period + 1 : i + 1]
        low = min(window)
        high = max(window)
        if high == low:
            raw_values.append(0.0)
        else:
            raw_values.append((rsi_values[i] - low) / (high - low) * 100.0)

    k_values = sma_series(raw_values, smooth_k)
    if not k_values:
        return None
    d_values = sma_series(k_values, smooth_d)
    if not d_values:
        return None
    return k_values[-1], d_values[-1]


def is_long_candidate(row: MonitorRow) -> bool:
    return row.sum_rsi6 <= 100


def is_short_candidate(row: MonitorRow) -> bool:
    return row.sum_rsi6 >= 400


@dataclass
class MonitorRow:
    symbol: str
    sum_rsi6: float
    sum_stoch: Optional[float]
    rsi6: Dict[str, float]
    stoch_k: Dict[str, float]
    stoch_d: Dict[str, float]
    macd: Dict[str, float]
    dif: Dict[str, float]
    dea: Dict[str, float]


async def compute_symbol_indicators(
    session: aiohttp.ClientSession,
    rate_limiter: RateLimiter,
    sem: asyncio.Semaphore,
    symbol: str,
) -> Optional[MonitorRow]:
    rsis: Dict[str, float] = {}
    stoch_k: Dict[str, float] = {}
    stoch_d: Dict[str, float] = {}
    macd_vals: Dict[str, float] = {}
    dif_vals: Dict[str, float] = {}
    dea_vals: Dict[str, float] = {}

    async def one_tf(tf_label: str, interval: str):
        async with sem:
            closes = await get_kline_closes(session, rate_limiter, symbol, interval, KLINE_LIMIT)
        val = rsi_wilder(closes, RSI_PERIOD)
        if val is None:
            raise BybitAPIError(f"Not enough data for RSI{RSI_PERIOD} {symbol} {tf_label}")
        rsis[tf_label] = val
        stoch_value = stoch_rsi_kd_from_closes(
            closes,
            STOCH_RSI_RSI_LEN,
            STOCH_RSI_STOCH_LEN,
            STOCH_RSI_SMOOTH_K,
            STOCH_RSI_SMOOTH_D,
        )
        if stoch_value is not None:
            stoch_k[tf_label], stoch_d[tf_label] = stoch_value
        macd_value = macd_from_closes(closes)
        if macd_value is not None:
            macd, dif, dea = macd_value
            macd_vals[tf_label] = macd
            dif_vals[tf_label] = dif
            dea_vals[tf_label] = dea

    try:
        await asyncio.gather(*(one_tf(lbl, iv) for (lbl, iv) in TIMEFRAMES))
    except Exception as e:
        logging.warning("Failed to compute indicators for %s: %s", symbol, e)
        return None

    s = sum(rsis[lbl] for (lbl, _) in TIMEFRAMES)
    sum_stoch = sum(stoch_k.values()) if stoch_k else None
    return MonitorRow(
        symbol=symbol,
        sum_rsi6=s,
        sum_stoch=sum_stoch,
        rsi6=rsis,
        stoch_k=stoch_k,
        stoch_d=stoch_d,
        macd=macd_vals,
        dif=dif_vals,
        dea=dea_vals,
    )


# =========================
# EXTRA: symbol click calc (price + Stoch RSI)
# =========================
async def get_last_price(session: aiohttp.ClientSession, rate_limiter: RateLimiter, symbol: str) -> float:
    items = await get_linear_tickers(session, rate_limiter, symbol=symbol)
    if not items:
        raise BybitAPIError(f"Ticker empty for {symbol}")
    it = items[0]
    def as_finite_float(value: object) -> Optional[float]:
        try:
            parsed = float(value)
        except (TypeError, ValueError):
            return None
        return parsed if math.isfinite(parsed) else None

    price = (
        as_finite_float(it.get("lastPrice"))
        or as_finite_float(it.get("markPrice"))
        or as_finite_float(it.get("indexPrice"))
    )
    if price is None:
        bid = as_finite_float(it.get("bid1Price"))
        ask = as_finite_float(it.get("ask1Price"))
        if bid is not None and ask is not None and bid > 0 and ask > 0:
            price = (bid + ask) / 2
    if price is None:
        raise BybitAPIError(f"No valid price for {symbol}: {it}")
    return price


async def get_stoch_rsi_values(
    session: aiohttp.ClientSession,
    rate_limiter: RateLimiter,
    symbol: str,
    sem: Optional[asyncio.Semaphore] = None,
) -> Dict[str, Tuple[float, float]]:
    async def one_tf(tf_label: str, interval: str) -> Tuple[str, Tuple[float, float]]:
        if sem is None:
            closes = await get_kline_closes(session, rate_limiter, symbol, interval, KLINE_LIMIT)
        else:
            async with sem:
                closes = await get_kline_closes(session, rate_limiter, symbol, interval, KLINE_LIMIT)
        value = stoch_rsi_kd_from_closes(
            closes,
            STOCH_RSI_RSI_LEN,
            STOCH_RSI_STOCH_LEN,
            STOCH_RSI_SMOOTH_K,
            STOCH_RSI_SMOOTH_D,
        )
        if value is None:
            raise BybitAPIError(
                "Not enough data for Stoch RSI "
                f"({STOCH_RSI_RSI_LEN}/{STOCH_RSI_STOCH_LEN}/"
                f"{STOCH_RSI_SMOOTH_K}/{STOCH_RSI_SMOOTH_D}) "
                f"{symbol} {tf_label}"
            )
        return tf_label, value

    results = await asyncio.gather(*(one_tf(lbl, iv) for (lbl, iv) in STOCH_RSI_TFS))
    return {label: value for label, value in results}


async def get_rsi_values(
    session: aiohttp.ClientSession,
    rate_limiter: RateLimiter,
    sem: asyncio.Semaphore,
    symbol: str,
) -> Dict[str, float]:
    async def one_tf(tf_label: str, interval: str) -> Tuple[str, float]:
        async with sem:
            closes = await get_kline_closes(session, rate_limiter, symbol, interval, KLINE_LIMIT)
        value = rsi_wilder(closes, RSI_PERIOD)
        if value is None:
            raise BybitAPIError(f"Not enough data for RSI{RSI_PERIOD} {symbol} {tf_label}")
        return tf_label, value

    results = await asyncio.gather(*(one_tf(lbl, iv) for (lbl, iv) in TIMEFRAMES))
    return {label: value for label, value in results}


async def get_macd_values(
    session: aiohttp.ClientSession,
    rate_limiter: RateLimiter,
    sem: asyncio.Semaphore,
    symbol: str,
) -> Dict[str, Tuple[float, float, float]]:
    async def one_tf(tf_label: str, interval: str) -> Tuple[str, Tuple[float, float, float]]:
        async with sem:
            closes = await get_kline_closes(session, rate_limiter, symbol, interval, KLINE_LIMIT)
        value = macd_from_closes(closes)
        if value is None:
            raise BybitAPIError(f"Not enough data for MACD {symbol} {tf_label}")
        return tf_label, value

    results = await asyncio.gather(*(one_tf(lbl, iv) for (lbl, iv) in TIMEFRAMES))
    return {label: value for label, value in results}


def format_rsi_line(values: Dict[str, float]) -> str:
    parts = []
    for label, _ in TIMEFRAMES:
        value = values.get(label)
        if value is None:
            parts.append(f"{label} <code>n/a</code>")
        else:
            parts.append(f"{label} <code>{value:.2f}</code>")
    return f"RSI{RSI_PERIOD}: " + " | ".join(parts)


def format_rsi_line_for_labels(values: Dict[str, float], labels: List[str]) -> str:
    parts = []
    for label in labels:
        value = values.get(label)
        if value is None:
            parts.append(f"{label} <code>n/a</code>")
        else:
            parts.append(f"{label} <code>{value:.2f}</code>")
    return f"RSI{RSI_PERIOD}: " + " | ".join(parts)


def format_stoch_rsi_line(values: Dict[str, Tuple[float, float]]) -> str:
    k_parts = []
    d_parts = []
    for label, _ in STOCH_RSI_TFS:
        value = values.get(label)
        if not value or len(value) != 2:
            k_parts.append(f"{label} <code>n/a</code>")
            d_parts.append(f"{label} <code>n/a</code>")
            continue
        k_val, d_val = value
        if not (math.isfinite(k_val) and math.isfinite(d_val)):
            k_parts.append(f"{label} <code>n/a</code>")
            d_parts.append(f"{label} <code>n/a</code>")
            continue
        k_parts.append(f"{label} <code>{k_val:.2f}</code>")
        d_parts.append(f"{label} <code>{d_val:.2f}</code>")
    return "Stoch RSI(K): " + " | ".join(k_parts) + "\nStoch RSI(D): " + " | ".join(d_parts)


def format_macd_lines(values: Dict[str, Tuple[float, float, float]]) -> Tuple[str, str, str]:
    macd_parts = []
    dif_parts = []
    dea_parts = []
    for label, _ in TIMEFRAMES:
        triple = values.get(label)
        if not triple:
            macd_parts.append(f"{label} <code>n/a</code>")
            dif_parts.append(f"{label} <code>n/a</code>")
            dea_parts.append(f"{label} <code>n/a</code>")
        else:
            macd, dif, dea = triple
            macd_parts.append(f"{label} <code>{macd:.4f}</code>")
            dif_parts.append(f"{label} <code>{dif:.4f}</code>")
            dea_parts.append(f"{label} <code>{dea:.4f}</code>")
    return (
        "MACD: " + " | ".join(macd_parts),
        "DIF: " + " | ".join(dif_parts),
        "DEA: " + " | ".join(dea_parts),
    )


def ema_series(values: List[float], period: int) -> List[Optional[float]]:
    if not values or len(values) < period:
        return []
    multiplier = 2 / (period + 1)
    ema_values: List[Optional[float]] = [None] * (period - 1)
    sma = sum(values[:period]) / period
    ema = sma
    ema_values.append(ema)
    for value in values[period:]:
        ema = (value - ema) * multiplier + ema
        ema_values.append(ema)
    return ema_values


def macd_from_closes(
    closes: List[float],
    fast_period: int = 12,
    slow_period: int = 26,
    signal_period: int = 9,
) -> Optional[Tuple[float, float, float]]:
    if len(closes) < slow_period + signal_period:
        return None
    ema_fast = ema_series(closes, fast_period)
    ema_slow = ema_series(closes, slow_period)
    if not ema_fast or not ema_slow:
        return None
    dif_values: List[float] = []
    for fast, slow in zip(ema_fast, ema_slow):
        if fast is None or slow is None:
            continue
        dif_values.append(fast - slow)
    if len(dif_values) < signal_period:
        return None
    dea_series = ema_series(dif_values, signal_period)
    if not dea_series:
        return None
    dea = dea_series[-1]
    dif = dif_values[-1]
    if dea is None:
        return None
    macd = (dif - dea) * 2
    return macd, dif, dea


# =========================
# RENDER PNG
# =========================
def load_monospace_font(size: int) -> ImageFont.FreeTypeFont:
    candidates = [
        "/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf",
        "/usr/share/fonts/truetype/liberation/LiberationMono-Regular.ttf",
    ]
    for path in candidates:
        if os.path.exists(path):
            return ImageFont.truetype(path, size=size)
    return ImageFont.load_default()


def measure_text(draw: ImageDraw.ImageDraw, text: str, font: ImageFont.FreeTypeFont) -> Tuple[int, int]:
    bbox = draw.textbbox((0, 0), text, font=font)
    return bbox[2] - bbox[0], bbox[3] - bbox[1]


def build_table_layout_generic(
    draw: ImageDraw.ImageDraw,
    font: ImageFont.FreeTypeFont,
    header_cells: List[str],
    data_rows: List[List[str]],
    column_gap: int,
) -> Tuple[List[str], List[List[str]], List[int], int]:
    col_count = len(header_cells)
    col_widths = [0] * col_count
    for col_idx, cell in enumerate(header_cells):
        width, _ = measure_text(draw, cell, font)
        col_widths[col_idx] = max(col_widths[col_idx], width)
    for row in data_rows:
        for col_idx, cell in enumerate(row):
            width, _ = measure_text(draw, cell, font)
            col_widths[col_idx] = max(col_widths[col_idx], width)

    total_width = sum(col_widths) + column_gap * (col_count - 1 if col_count > 0 else 0)
    return header_cells, data_rows, col_widths, total_width


def render_png(
    long_rows: List[MonitorRow],
    short_rows: List[MonitorRow],
    symbols_scanned: int,
) -> BytesIO:
    ts = datetime.now(LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S %Z")

    header_lines = [
        "Bybit USDT Perpetuals — RSI(6) + Stoch RSI(14/14/3/3) + MACD monitor",
        f"Timeframes: {', '.join(lbl for (lbl, _) in TIMEFRAMES)}",
        f"Generated: {ts}",
        f"Symbols scanned: {symbols_scanned} (top by turnover24h)",
        "",
    ]

    font = load_monospace_font(FONT_SIZE)

    dummy = Image.new("RGB", (10, 10), (255, 255, 255))
    d = ImageDraw.Draw(dummy)

    _, line_h = measure_text(d, "Hg", font)
    row_step = line_h + LINE_SPACING
    column_gap = 12

    blocks: List[Tuple[str, object]] = []
    for line in header_lines:
        if line:
            blocks.append(("text", line))
        else:
            blocks.append(("blank", None))

    def add_table_block(
        title: str,
        header_cells: List[str],
        data_rows: List[List[str]],
    ) -> None:
        if data_rows:
            blocks.append(("table", (title, header_cells, data_rows)))
        else:
            blocks.append(("text", title))
            blocks.append(("text", "No candidates found"))

    tf_labels = [lbl for (lbl, _) in TIMEFRAMES]

    def format_optional(value: Optional[float], fmt: str) -> str:
        if value is None or not math.isfinite(value):
            return "n/a"
        return fmt.format(value)

    def build_rsi_rows(rows: List[MonitorRow]) -> List[List[str]]:
        data = []
        for row in rows:
            row_cells = [row.symbol, f"{row.sum_rsi6:.1f}"]
            for lbl in tf_labels:
                row_cells.append(format_optional(row.rsi6.get(lbl), "{:.1f}"))
            data.append(row_cells)
        return data

    def build_stoch_rows(rows: List[MonitorRow]) -> List[List[str]]:
        data = []
        for row in rows:
            sum_stoch = (
                format_optional(row.sum_stoch, "{:.1f}") if row.sum_stoch is not None else "n/a"
            )
            row_cells = [row.symbol, sum_stoch]
            for lbl in tf_labels:
                k_val = row.stoch_k.get(lbl)
                d_val = row.stoch_d.get(lbl)
                if (
                    k_val is None
                    or d_val is None
                    or not math.isfinite(k_val)
                    or not math.isfinite(d_val)
                ):
                    row_cells.append("n/a")
                else:
                    row_cells.append(f"{k_val:.2f}/{d_val:.2f}")
            data.append(row_cells)
        return data

    def build_simple_rows(
        rows: List[MonitorRow],
        accessor: Callable[[MonitorRow], Dict[str, float]],
    ) -> List[List[str]]:
        data = []
        for row in rows:
            row_cells = [row.symbol]
            values = accessor(row)
            for lbl in tf_labels:
                row_cells.append(format_optional(values.get(lbl), "{:.6f}"))
            data.append(row_cells)
        return data

    add_table_block(
        "TOP-10 LONG (min SUM RSI6)",
        ["SYMBOL", "SUM RSI6"] + tf_labels,
        build_rsi_rows(long_rows),
    )
    blocks.append(("blank", None))
    add_table_block(
        "TOP-10 LONG (Stoch RSI 14/14/3/3 K/D)",
        ["SYMBOL", "SUM Stoch"] + tf_labels,
        build_stoch_rows(long_rows),
    )
    blocks.append(("blank", None))
    add_table_block(
        "TOP-10 LONG (MACD)",
        ["SYMBOL"] + tf_labels,
        build_simple_rows(long_rows, lambda row: row.macd),
    )
    blocks.append(("blank", None))
    add_table_block(
        "TOP-10 LONG (DIF)",
        ["SYMBOL"] + tf_labels,
        build_simple_rows(long_rows, lambda row: row.dif),
    )
    blocks.append(("blank", None))
    add_table_block(
        "TOP-10 LONG (DEA)",
        ["SYMBOL"] + tf_labels,
        build_simple_rows(long_rows, lambda row: row.dea),
    )
    blocks.append(("blank", None))
    blocks.append(("blank", None))
    add_table_block(
        "TOP-10 SHORT (max SUM RSI6)",
        ["SYMBOL", "SUM RSI6"] + tf_labels,
        build_rsi_rows(short_rows),
    )
    blocks.append(("blank", None))
    add_table_block(
        "TOP-10 SHORT (Stoch RSI 14/14/3/3 K/D)",
        ["SYMBOL", "SUM Stoch"] + tf_labels,
        build_stoch_rows(short_rows),
    )
    blocks.append(("blank", None))
    add_table_block(
        "TOP-10 SHORT (MACD)",
        ["SYMBOL"] + tf_labels,
        build_simple_rows(short_rows, lambda row: row.macd),
    )
    blocks.append(("blank", None))
    add_table_block(
        "TOP-10 SHORT (DIF)",
        ["SYMBOL"] + tf_labels,
        build_simple_rows(short_rows, lambda row: row.dif),
    )
    blocks.append(("blank", None))
    add_table_block(
        "TOP-10 SHORT (DEA)",
        ["SYMBOL"] + tf_labels,
        build_simple_rows(short_rows, lambda row: row.dea),
    )

    max_w = 0
    total_h = IMAGE_PADDING * 2
    table_layouts: Dict[int, Tuple[List[str], List[List[str]], List[int], int]] = {}
    for idx, (kind, payload) in enumerate(blocks):
        if kind == "text":
            w, _ = measure_text(d, str(payload), font)
            max_w = max(max_w, w)
            total_h += row_step
        elif kind == "blank":
            total_h += row_step
        elif kind == "table":
            title, header_cells, data_rows = payload  # type: ignore[misc]
            header_cells, data_rows, col_widths, table_w = build_table_layout_generic(
                d,
                font,
                header_cells,
                data_rows,
                column_gap,
            )
            table_layouts[idx] = (header_cells, data_rows, col_widths, table_w)
            title_w, _ = measure_text(d, title, font)
            max_w = max(max_w, title_w, table_w)
            total_h += row_step * (3 + len(data_rows))

    total_w = IMAGE_PADDING * 2 + max_w

    img = Image.new("RGB", (total_w, total_h), (255, 255, 255))
    d = ImageDraw.Draw(img)

    y = IMAGE_PADDING
    for idx, (kind, payload) in enumerate(blocks):
        if kind == "text":
            d.text((IMAGE_PADDING, y), str(payload), fill=(0, 0, 0), font=font)
            y += row_step
        elif kind == "blank":
            y += row_step
        elif kind == "table":
            title, _, _ = payload  # type: ignore[misc]
            header_cells, data_rows, col_widths, table_w = table_layouts[idx]
            d.text((IMAGE_PADDING, y), title, fill=(0, 0, 0), font=font)
            y += row_step
            x = IMAGE_PADDING
            col_positions = [x]
            for width in col_widths[:-1]:
                x += width + column_gap
                col_positions.append(x)
            for col_idx, cell in enumerate(header_cells):
                d.text((col_positions[col_idx], y), cell, fill=(0, 0, 0), font=font)
            y += row_step
            line_y = y + line_h // 2
            d.line((IMAGE_PADDING, line_y, IMAGE_PADDING + table_w, line_y), fill=(0, 0, 0))
            y += row_step
            for row in data_rows:
                for col_idx, cell in enumerate(row):
                    cell_w, _ = measure_text(d, cell, font)
                    if col_idx == 0:
                        cell_x = col_positions[col_idx]
                    else:
                        cell_x = col_positions[col_idx] + col_widths[col_idx] - cell_w
                    d.text((cell_x, y), cell, fill=(0, 0, 0), font=font)
                y += row_step

    bio = BytesIO()
    bio.name = "rsi_tables.png"
    img.save(bio, format="PNG", optimize=True)
    bio.seek(0)
    return bio


# =========================
# TELEGRAM UI (BUTTONS)
# =========================
def main_menu_kb(has_sub: bool) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton("➕ Создать подписку", callback_data="SUB_CREATE")],
        [InlineKeyboardButton("📋 Моя подписка", callback_data="SUB_VIEW")],
        [InlineKeyboardButton("⚡️ Прислать сейчас", callback_data="RUN_NOW")],
        [InlineKeyboardButton("ℹ️ Информация по паре", callback_data="PAIR_INFO")],
    ]
    if has_sub:
        rows.append([InlineKeyboardButton("🗑 Удалить подписку", callback_data="SUB_DELETE")])
        rows.append([InlineKeyboardButton("✏️ Изменить интервал", callback_data="SUB_CREATE")])
    return InlineKeyboardMarkup(rows)


def interval_picker_kb() -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton("5 мин", callback_data="SETINT:5"),
            InlineKeyboardButton("15 мин", callback_data="SETINT:15"),
            InlineKeyboardButton("30 мин", callback_data="SETINT:30"),
        ],
        [
            InlineKeyboardButton("60 мин", callback_data="SETINT:60"),
            InlineKeyboardButton("240 мин", callback_data="SETINT:240"),
        ],
        [InlineKeyboardButton("⬅️ Назад", callback_data="MENU")],
    ]
    return InlineKeyboardMarkup(rows)


def alerts_kb(symbol: str, side: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("Alert 5m", callback_data=f"ALERT|5|{side}|{symbol}"),
                InlineKeyboardButton("Alert 15m", callback_data=f"ALERT|15|{side}|{symbol}"),
                InlineKeyboardButton("Alert 30m", callback_data=f"ALERT|30|{side}|{symbol}"),
            ],
            [
                InlineKeyboardButton("Alert 1h", callback_data=f"ALERT|60|{side}|{symbol}"),
                InlineKeyboardButton("Alert 4h", callback_data=f"ALERT|240|{side}|{symbol}"),
            ],
        ]
    )


def pair_info_actions_kb(symbol: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("🔔 Отслеживать пару по RSI6", callback_data=f"PAIRTRACK|{symbol}")],
            [InlineKeyboardButton("💰 Создать alert по цене", callback_data=f"PRICE_ALERT|{symbol}")],
            [InlineKeyboardButton("⬅️ Меню", callback_data="MENU")],
        ]
    )


def price_alert_confirm_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("✅ Да, верно", callback_data="PRICE_ALERT_CONFIRM|YES"),
                InlineKeyboardButton("✏️ Изменить", callback_data="PRICE_ALERT_CONFIRM|NO"),
            ],
            [InlineKeyboardButton("⬅️ Меню", callback_data="MENU")],
        ]
    )


def pair_track_tf_kb(symbol: str, selected: Set[str]) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    labels = [label for label, _ in TIMEFRAMES]
    for i in range(0, len(labels), 2):
        row: List[InlineKeyboardButton] = []
        for label in labels[i:i + 2]:
            mark = "✅ " if label in selected else ""
            row.append(
                InlineKeyboardButton(f"{mark}{label}", callback_data=f"PAIRTRACK_TF|{symbol}|{label}")
            )
        rows.append(row)
    rows.append([InlineKeyboardButton("✅ Готово", callback_data=f"PAIRTRACK_TF_CONFIRM|{symbol}")])
    rows.append([InlineKeyboardButton("⬅️ Меню", callback_data="MENU")])
    return InlineKeyboardMarkup(rows)


def pair_track_range_kb(symbol: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton(f"RSI6 <= {PAIR_RSI_LOW_THRESHOLD:.0f}", callback_data=f"PAIRTRACK_RANGE|{symbol}|LOW")],
            [InlineKeyboardButton(f"RSI6 >= {PAIR_RSI_HIGH_THRESHOLD:.0f}", callback_data=f"PAIRTRACK_RANGE|{symbol}|HIGH")],
            [InlineKeyboardButton("⬅️ Назад", callback_data=f"PAIRTRACK_TF_BACK|{symbol}")],
        ]
    )


def pairs_keyboard(long_syms: List[str], short_syms: List[str]) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []

    def chunk_buttons(symbols: List[str], side: str) -> List[List[InlineKeyboardButton]]:
        out: List[List[InlineKeyboardButton]] = []
        for i in range(0, len(symbols), 2):
            row = []
            s1 = symbols[i]
            row.append(InlineKeyboardButton(s1, callback_data=f"PAIR|{side}|{s1}"))
            if i + 1 < len(symbols):
                s2 = symbols[i + 1]
                row.append(InlineKeyboardButton(s2, callback_data=f"PAIR|{side}|{s2}"))
            out.append(row)
        return out

    # Long buttons first (same order as table), then Short
    if long_syms:
        rows.append([InlineKeyboardButton("— LONG —", callback_data="SECTION|LONG")])
        rows += chunk_buttons(long_syms, "L")
    if short_syms:
        rows.append([InlineKeyboardButton("— SHORT —", callback_data="SECTION|SHORT")])
        rows += chunk_buttons(short_syms, "S")

    return InlineKeyboardMarkup(rows)


# =========================
# BOT STATE HELPERS
# =========================
async def get_sub(app: Application, chat_id: int) -> Optional[dict]:
    """Get subscription for chat_id with thread-safe state access."""
    if not validate_chat_id(chat_id):
        return None
    state_lock: asyncio.Lock = app.bot_data["state_lock"]
    async with state_lock:
        state = app.bot_data.get("state") or {
            "subs": {},
            "alerts": {},
            "pair_rsi_alerts": {},
            "price_alerts": {},
        }
        subs = state.get("subs", {})
        return subs.get(str(chat_id))


async def set_sub(app: Application, chat_id: int, interval_min: int, enabled: bool = True) -> None:
    if not validate_chat_id(chat_id) or not validate_interval(interval_min):
        logging.warning("Invalid chat_id or interval: %s, %s", chat_id, interval_min)
        return
    
    state_lock: asyncio.Lock = app.bot_data["state_lock"]
    async with state_lock:
        state = app.bot_data.get("state") or {
            "subs": {},
            "alerts": {},
            "pair_rsi_alerts": {},
            "price_alerts": {},
        }
        state.setdefault("subs", {})
        state["subs"][str(chat_id)] = {"interval_min": int(interval_min), "enabled": bool(enabled)}
        app.bot_data["state"] = state
        log_state_change(state, "set_sub", chat_id=chat_id, interval_min=interval_min, enabled=enabled)
        await persist_state(app, state)


async def delete_sub(app: Application, chat_id: int) -> None:
    if not validate_chat_id(chat_id):
        return
    
    state_lock: asyncio.Lock = app.bot_data["state_lock"]
    async with state_lock:
        state = app.bot_data.get("state") or {
            "subs": {},
            "alerts": {},
            "pair_rsi_alerts": {},
            "price_alerts": {},
        }
        subs = state.setdefault("subs", {})
        subs.pop(str(chat_id), None)
        log_state_change(state, "delete_sub", chat_id=chat_id)
        await persist_state(app, state)


def normalize_tf_labels(selected_labels: Set[str]) -> List[Tuple[str, str]]:
    ordered = []
    for label, interval in TIMEFRAMES:
        if label in selected_labels:
            ordered.append((label, interval))
    return ordered


async def get_pair_rsi_alerts_for_chat(app: Application, chat_id: int) -> Dict[str, dict]:
    if not validate_chat_id(chat_id):
        return {}
    state_lock: asyncio.Lock = app.bot_data["state_lock"]
    async with state_lock:
        state = app.bot_data.get("state") or {"subs": {}, "alerts": {}, "pair_rsi_alerts": {}, "price_alerts": {}}
        alerts = state.get("pair_rsi_alerts", {})
        return copy.deepcopy(alerts.get(str(chat_id), {}))


async def set_pair_rsi_alert_state(
    app: Application,
    chat_id: int,
    symbol: str,
    tf_intervals: List[str],
    mode: str,
    valid_symbols: Optional[Set[str]] = None,
) -> Tuple[bool, Optional[str]]:
    if not validate_chat_id(chat_id):
        return False, "Некорректный chat_id."

    if mode not in {"LOW", "HIGH"}:
        return False, "Некорректный режим."

    if valid_symbols is None or not valid_symbols:
        valid_symbols = await get_valid_symbols_with_fallback(app)
    if not valid_symbols:
        return False, "Whitelist временно недоступен. Попробуйте позже."

    if not validate_symbol(symbol, valid_symbols):
        return False, "Некорректный символ."

    allowed_intervals = {interval for _, interval in TIMEFRAMES}
    if not tf_intervals or any(tf not in allowed_intervals for tf in tf_intervals):
        return False, "Некорректные таймфреймы."

    ordered_intervals = [interval for _, interval in TIMEFRAMES if interval in tf_intervals]
    tf_labels = [TIMEFRAME_BY_INTERVAL[interval] for interval in ordered_intervals]
    key = f"{symbol}|{','.join(ordered_intervals)}|{mode}"

    state_lock: asyncio.Lock = app.bot_data["state_lock"]
    async with state_lock:
        state = app.bot_data.get("state") or {
            "subs": {},
            "alerts": {},
            "pair_rsi_alerts": {},
            "price_alerts": {},
        }
        alerts_all = state.setdefault("pair_rsi_alerts", {})
        alerts = alerts_all.setdefault(str(chat_id), {})
        is_new = key not in alerts
        if is_new:
            if len(alerts) >= MAX_ALERTS_PER_CHAT:
                return False, f"Лимит алертов на чат ({MAX_ALERTS_PER_CHAT}) достигнут."
            total_alerts = sum(len(a) for a in alerts_all.values())
            if total_alerts >= MAX_ALERTS_GLOBAL:
                return False, f"Глобальный лимит алертов ({MAX_ALERTS_GLOBAL}) достигнут."
        alerts[key] = {
            "symbol": symbol,
            "tfs": ordered_intervals,
            "tf_labels": tf_labels,
            "mode": mode,
            "created_at": time.time(),
        }
        app.bot_data["state"] = state
        log_state_change(
            state,
            "set_pair_rsi_alert_state",
            chat_id=chat_id,
            symbol=symbol,
            mode=mode,
            tfs=",".join(ordered_intervals),
        )
        await persist_state(app, state)
    return True, None


async def get_price_alerts_for_chat(app: Application, chat_id: int) -> Dict[str, dict]:
    if not validate_chat_id(chat_id):
        return {}
    state_lock: asyncio.Lock = app.bot_data["state_lock"]
    async with state_lock:
        state = app.bot_data.get("state") or {"subs": {}, "alerts": {}, "pair_rsi_alerts": {}, "price_alerts": {}}
        alerts = state.get("price_alerts", {})
        return copy.deepcopy(alerts.get(str(chat_id), {}))


async def set_price_alert_state(
    app: Application,
    chat_id: int,
    symbol: str,
    price_value: Decimal,
    direction: str,
    valid_symbols: Optional[Set[str]] = None,
) -> Tuple[bool, Optional[str], Optional[str]]:
    if not validate_chat_id(chat_id):
        return False, "Некорректный chat_id.", None

    if direction not in {"UP", "DOWN"}:
        return False, "Некорректное направление.", None

    if valid_symbols is None or not valid_symbols:
        valid_symbols = await get_valid_symbols_with_fallback(app)
    if not valid_symbols:
        return False, "Whitelist временно недоступен. Попробуйте позже.", None

    if not validate_symbol(symbol, valid_symbols):
        return False, "Некорректный символ.", None

    price_text = normalize_price_text(price_value)

    state_lock: asyncio.Lock = app.bot_data["state_lock"]
    async with state_lock:
        state = app.bot_data.get("state") or {"subs": {}, "alerts": {}, "pair_rsi_alerts": {}, "price_alerts": {}}
        alerts_all = state.setdefault("price_alerts", {})
        alerts = alerts_all.setdefault(str(chat_id), {})
        if len(alerts) >= MAX_ALERTS_PER_CHAT:
            return False, "Достигнут лимит алертов в этом чате.", None
        total_alerts = sum(len(a) for a in alerts_all.values())
        if total_alerts >= MAX_ALERTS_GLOBAL:
            return False, "Достигнут глобальный лимит алертов.", None
        key = f"{symbol}|{price_text}"
        alerts[key] = {
            "symbol": symbol,
            "price": float(price_value),
            "price_text": price_text,
            "direction": direction,
            "created_at": time.time(),
        }
        app.bot_data["state"] = state
        log_state_change(
            state,
            "set_price_alert_state",
            chat_id=chat_id,
            symbol=symbol,
            direction=direction,
            price=price_text,
        )
        await persist_state(app, state)
    return True, None, key


async def delete_price_alert_state(app: Application, chat_id: int, key: str) -> None:
    if not validate_chat_id(chat_id):
        return
    state_lock: asyncio.Lock = app.bot_data["state_lock"]
    async with state_lock:
        state = app.bot_data.get("state") or {"subs": {}, "alerts": {}, "pair_rsi_alerts": {}, "price_alerts": {}}
        alerts = state.setdefault("price_alerts", {}).setdefault(str(chat_id), {})
        if key in alerts:
            alerts.pop(key, None)
            app.bot_data["state"] = state
            log_state_change(state, "delete_price_alert_state", chat_id=chat_id, key=key)
            await persist_state(app, state)


async def delete_pair_rsi_alert_state(app: Application, chat_id: int, key: str) -> None:
    if not validate_chat_id(chat_id):
        return
    state_lock: asyncio.Lock = app.bot_data["state_lock"]
    async with state_lock:
        state = app.bot_data.get("state") or {
            "subs": {},
            "alerts": {},
            "pair_rsi_alerts": {},
            "price_alerts": {},
        }
        alerts = state.setdefault("pair_rsi_alerts", {}).setdefault(str(chat_id), {})
        if key in alerts:
            alerts.pop(key, None)
            app.bot_data["state"] = state
            log_state_change(state, "delete_pair_rsi_alert_state", chat_id=chat_id, key=key)
            await persist_state(app, state)


async def get_alerts_for_chat(app: Application, chat_id: int) -> Dict[str, dict]:
    """Get alerts for chat_id with thread-safe state access."""
    if not validate_chat_id(chat_id):
        return {}
    state_lock: asyncio.Lock = app.bot_data["state_lock"]
    async with state_lock:
        state = app.bot_data.get("state") or {
            "subs": {},
            "alerts": {},
            "pair_rsi_alerts": {},
            "price_alerts": {},
        }
        alerts = state.get("alerts", {})
        return copy.deepcopy(alerts.get(str(chat_id), {}))


async def get_alert_entry(app: Application, chat_id: int, key: str) -> Optional[dict]:
    if not validate_chat_id(chat_id):
        return None
    state_lock: asyncio.Lock = app.bot_data["state_lock"]
    async with state_lock:
        state = app.bot_data.get("state") or {
            "subs": {},
            "alerts": {},
            "pair_rsi_alerts": {},
            "price_alerts": {},
        }
        entry = state.get("alerts", {}).get(str(chat_id), {}).get(key)
        if isinstance(entry, dict):
            return copy.deepcopy(entry)
        return None


_ALERT_ENTRY_MISSING = object()


async def update_alert_entry(
    app: Application,
    chat_id: int,
    symbol: str,
    tf: str,
    *,
    created_at: object = _ALERT_ENTRY_MISSING,
    last_above: object = _ALERT_ENTRY_MISSING,
    delete: bool = False,
) -> bool:
    if not validate_chat_id(chat_id):
        return False
    state_lock: asyncio.Lock = app.bot_data["state_lock"]
    async with state_lock:
        state = app.bot_data.get("state") or {
            "subs": {},
            "alerts": {},
            "pair_rsi_alerts": {},
            "price_alerts": {},
        }
        alerts = state.setdefault("alerts", {}).setdefault(str(chat_id), {})
        key = f"{symbol}|{tf}"
        if key not in alerts:
            return False
        if delete:
            alerts.pop(key, None)
            app.bot_data["state"] = state
            log_state_change(state, "delete_alert_state", chat_id=chat_id, symbol=symbol, tf=tf)
            await persist_state(app, state)
            return True
        updated = False
        entry = alerts[key]
        if created_at is not _ALERT_ENTRY_MISSING:
            entry["created_at"] = float(created_at)
            updated = True
        if last_above is not _ALERT_ENTRY_MISSING:
            entry["last_above"] = bool(last_above)
            updated = True
        if updated:
            app.bot_data["state"] = state
            log_state_change(
                state,
                "update_alert_entry",
                chat_id=chat_id,
                symbol=symbol,
                tf=tf,
                last_above=entry.get("last_above"),
                created_at=entry.get("created_at"),
            )
            await persist_state(app, state)
        return updated


def alert_ttl_seconds() -> float:
    return ALERT_TTL_HOURS * 3600


def is_alert_expired(created_at: float, now: Optional[float] = None) -> bool:
    if now is None:
        now = time.time()
    try:
        created_at_ts = float(created_at)
    except (TypeError, ValueError):
        return True
    return (now - created_at_ts) >= alert_ttl_seconds()


def prune_ttl_dict(cache: Dict[object, float], ttl_sec: float, now: float) -> None:
    if ttl_sec <= 0:
        return
    for key, last_seen in list(cache.items()):
        if now - last_seen >= ttl_sec:
            del cache[key]


def _prune_expired_alerts_unsafe(state: dict, now: Optional[float] = None) -> bool:
    if now is None:
        now = time.time()
    changed = False
    alerts_all = state.get("alerts", {}) or {}
    for chat_id_str in list(alerts_all.keys()):
        amap = alerts_all.get(chat_id_str)
        if not isinstance(amap, dict):
            alerts_all.pop(chat_id_str, None)
            changed = True
            continue
        for key in list(amap.keys()):
            entry = amap.get(key)
            if not isinstance(entry, dict):
                amap.pop(key, None)
                changed = True
                continue
            created_at = entry.get("created_at")
            if created_at is None:
                entry["created_at"] = now
                changed = True
                continue
            if is_alert_expired(created_at, now):
                amap.pop(key, None)
                changed = True
        if not amap:
            alerts_all.pop(chat_id_str, None)
            changed = True
    return changed


async def set_alert_state(
    app: Application,
    chat_id: int,
    symbol: str,
    tf: str,
    tf_label: str,
    side: str,
    last_above: Optional[bool] = None,
    valid_symbols: Optional[Set[str]] = None,
) -> Tuple[bool, Optional[str]]:
    if not validate_chat_id(chat_id):
        logging.warning("Invalid chat_id for alert: %s", chat_id)
        return False, "Некорректный chat_id."

    if valid_symbols is None or not valid_symbols:
        valid_symbols = await get_valid_symbols_with_fallback(app)
    if not valid_symbols:
        logging.warning("Valid symbols whitelist unavailable for alert state")
        return False, "Whitelist временно недоступен. Попробуйте позже."

    if not validate_symbol(symbol, valid_symbols):
        logging.warning("Invalid symbol for alert: %s", symbol)
        return False, "Некорректный символ."

    state_lock: asyncio.Lock = app.bot_data["state_lock"]
    async with state_lock:
        state = app.bot_data.get("state") or {
            "subs": {},
            "alerts": {},
            "pair_rsi_alerts": {},
            "price_alerts": {},
        }
        now = time.time()
        changed = _prune_expired_alerts_unsafe(state, now=now)
        alerts_all = state.setdefault("alerts", {})
        alerts = alerts_all.setdefault(str(chat_id), {})
        key = f"{symbol}|{tf}"
        is_new = key not in alerts
        if is_new:
            if len(alerts) >= MAX_ALERTS_PER_CHAT:
                return False, f"Лимит алертов на чат ({MAX_ALERTS_PER_CHAT}) достигнут."
            total_alerts = sum(len(a) for a in alerts_all.values())
            if total_alerts >= MAX_ALERTS_GLOBAL:
                return False, f"Глобальный лимит алертов ({MAX_ALERTS_GLOBAL}) достигнут."
        created_at = alerts.get(key, {}).get("created_at", now)
        alerts[key] = {
            "symbol": symbol,
            "tf": tf,
            "tf_label": tf_label,
            "side": side,
            "last_above": last_above,  # can be None
            "created_at": created_at,
        }
        app.bot_data["state"] = state
        log_state_change(
            state,
            "set_alert_state",
            chat_id=chat_id,
            symbol=symbol,
            tf=tf,
            side=side,
        )
        await persist_state(app, state)
    return True, None


async def update_alert_last_above(app: Application, chat_id: int, symbol: str, tf: str, last_above: bool) -> None:
    if not validate_chat_id(chat_id):
        return

    if tf not in ALERT_INTERVALS:
        return

    valid_symbols = await get_valid_symbols_with_fallback(app)
    if not valid_symbols:
        return
    if not validate_symbol(symbol, valid_symbols):
        return
    
    state_lock: asyncio.Lock = app.bot_data["state_lock"]
    async with state_lock:
        state = app.bot_data.get("state") or {
            "subs": {},
            "alerts": {},
            "pair_rsi_alerts": {},
            "price_alerts": {},
        }
        alerts = state.setdefault("alerts", {}).setdefault(str(chat_id), {})
        key = f"{symbol}|{tf}"
        if key in alerts:
            alerts[key]["last_above"] = bool(last_above)
            app.bot_data["state"] = state
            log_state_change(
                state,
                "update_alert_last_above",
                chat_id=chat_id,
                symbol=symbol,
                tf=tf,
                last_above=last_above,
            )
            await persist_state(app, state)


async def update_alert_created_at(app: Application, chat_id: int, symbol: str, tf: str, created_at: float) -> None:
    if not validate_chat_id(chat_id):
        return

    if tf not in ALERT_INTERVALS:
        return

    valid_symbols = await get_valid_symbols_with_fallback(app)
    if not valid_symbols:
        return
    if not validate_symbol(symbol, valid_symbols):
        return

    state_lock: asyncio.Lock = app.bot_data["state_lock"]
    async with state_lock:
        state = app.bot_data.get("state") or {
            "subs": {},
            "alerts": {},
            "pair_rsi_alerts": {},
            "price_alerts": {},
        }
        alerts = state.setdefault("alerts", {}).setdefault(str(chat_id), {})
        key = f"{symbol}|{tf}"
        if key in alerts:
            alerts[key]["created_at"] = float(created_at)
            app.bot_data["state"] = state
            log_state_change(
                state,
                "update_alert_created_at",
                chat_id=chat_id,
                symbol=symbol,
                tf=tf,
                created_at=created_at,
            )
            await persist_state(app, state)


async def delete_alert_state(
    app: Application,
    chat_id: int,
    symbol: str,
    tf: str,
    validate_symbol_check: bool = True,
) -> None:
    if not validate_chat_id(chat_id):
        return

    if validate_symbol_check:
        valid_symbols = await get_valid_symbols_with_fallback(app)
        if not valid_symbols:
            return
        if not validate_symbol(symbol, valid_symbols):
            return

    state_lock: asyncio.Lock = app.bot_data["state_lock"]
    async with state_lock:
        state = app.bot_data.get("state") or {
            "subs": {},
            "alerts": {},
            "pair_rsi_alerts": {},
            "price_alerts": {},
        }
        alerts = state.setdefault("alerts", {}).setdefault(str(chat_id), {})
        key = f"{symbol}|{tf}"
        if key in alerts:
            alerts.pop(key, None)
            app.bot_data["state"] = state
            log_state_change(state, "delete_alert_state", chat_id=chat_id, symbol=symbol, tf=tf)
            await persist_state(app, state)


# =========================
# CACHES
# =========================
async def get_innovation_zone_symbols(
    app: Application,
    session: aiohttp.ClientSession,
    rate_limiter: RateLimiter,
) -> Set[str]:
    """
    Fetch Innovation Zone symbols (fee group 6). Fail-open on errors.

    Test hint: mock `/v5/market/fee-group-info` to return result.list[*].symbols
    including a known symbol (e.g. "FOOUSDTPERP") and verify it is filtered
    from auto-selected universes but still accepted for manual symbol input.
    """
    cache_lock: asyncio.Lock = app.bot_data["innovation_symbols_lock"]
    async with cache_lock:
        now = time.time()
        cached = app.bot_data.get("innovation_symbols_cache")
        if cached and (now - cached.ts) <= INNOVATION_SYMBOLS_CACHE_TTL:
            return cached.value  # type: ignore
        refresh_task = app.bot_data.get("innovation_symbols_refresh_task")
        if refresh_task and not refresh_task.done():
            task = refresh_task
        else:
            task = asyncio.create_task(
                _refresh_innovation_zone_symbols(app, session, rate_limiter)
            )
            app.bot_data["innovation_symbols_refresh_task"] = task

    try:
        return await task
    finally:
        if task.done():
            async with cache_lock:
                if app.bot_data.get("innovation_symbols_refresh_task") is task:
                    app.bot_data["innovation_symbols_refresh_task"] = None


async def _refresh_innovation_zone_symbols(
    app: Application,
    session: aiohttp.ClientSession,
    rate_limiter: RateLimiter,
) -> Set[str]:
    url = f"{BYBIT_BASE_URL}/v5/market/fee-group-info"
    params = {"productType": "contract", "groupId": "6"}
    symbols: Set[str] = set()
    try:
        payload = await api_get_json(session, url, params, rate_limiter)
        items = (payload.get("result") or {}).get("list") or []
        for item in items:
            for symbol in item.get("symbols") or []:
                if isinstance(symbol, str):
                    symbols.add(symbol)
    except Exception as e:
        logging.warning("Failed to fetch Innovation Zone symbols: %s", e)
        symbols = set()

    cache_lock: asyncio.Lock = app.bot_data["innovation_symbols_lock"]
    async with cache_lock:
        app.bot_data["innovation_symbols_cache"] = CacheItem(ts=time.time(), value=symbols)
    return symbols


async def get_cached_perp_symbols(app: Application) -> Set[str]:
    cache_lock: asyncio.Lock = app.bot_data["perp_symbols_lock"]
    async with cache_lock:
        now = time.time()
        cached = app.bot_data.get("perp_symbols_cache")
        if cached and (now - cached.ts) <= PERP_SYMBOLS_CACHE_TTL:
            return cached.value  # type: ignore

        session: aiohttp.ClientSession = app.bot_data["http_session"]
        rate_limiter: RateLimiter = app.bot_data["rate_limiter"]
        innovation_symbols = await get_innovation_zone_symbols(app, session, rate_limiter)
        symbols = set(
            await get_all_usdt_linear_perp_symbols(
                session,
                rate_limiter,
                innovation_symbols=innovation_symbols,
            )
        )
        app.bot_data["perp_symbols_cache"] = CacheItem(ts=now, value=symbols)
        save_perp_symbols(symbols)
        return symbols


async def get_cached_top_symbols(app: Application) -> List[str]:
    cache_lock: asyncio.Lock = app.bot_data["tickers_lock"]
    async with cache_lock:
        now = time.time()
        cached = app.bot_data.get("tickers_cache")
        if cached and (now - cached.ts) <= TICKERS_CACHE_TTL:
            return cached.value  # type: ignore

        session: aiohttp.ClientSession = app.bot_data["http_session"]
        rate_limiter: RateLimiter = app.bot_data["rate_limiter"]
        try:
            perp_symbols = await get_cached_perp_symbols(app)
            innovation_symbols = await _refresh_innovation_zone_symbols(app, session, rate_limiter)
            top = await pick_top_symbols_by_turnover(
                session,
                rate_limiter,
                perp_symbols,
                MAX_SYMBOLS,
                innovation_symbols=innovation_symbols,
            )
        except Exception as e:
            if cached:
                logging.warning("Failed to refresh tickers cache, using stale data: %s", e)
                return cached.value  # type: ignore
            raise
        app.bot_data["tickers_cache"] = CacheItem(ts=now, value=top)
        return top


# =========================
# SCHEDULING
# =========================
def schedule_subscription(app: Application, chat_id: int, interval_min: int) -> None:
    if not validate_chat_id(chat_id) or not validate_interval(interval_min):
        logging.warning("Invalid parameters for scheduling: chat_id=%s, interval=%s", chat_id, interval_min)
        return
    
    jobs = app.job_queue.get_jobs_by_name(get_sub_job_name(chat_id))
    for j in jobs:
        j.schedule_removal()
    app.job_queue.run_repeating(
        sub_job_callback,
        interval=interval_min * 60,
        first=2,
        chat_id=chat_id,
        name=get_sub_job_name(chat_id),
    )


def unschedule_subscription(app: Application, chat_id: int) -> None:
    jobs = app.job_queue.get_jobs_by_name(get_sub_job_name(chat_id))
    for j in jobs:
        j.schedule_removal()


async def schedule_alert(
    app: Application,
    chat_id: int,
    symbol: str,
    tf: str,
    tf_label: str,
    side: str,
    valid_symbols: Optional[Set[str]] = None,
) -> None:
    if not validate_chat_id(chat_id):
        logging.warning("Invalid chat_id for alert: %s", chat_id)
        return

    if valid_symbols is None or not valid_symbols:
        valid_symbols = await get_valid_symbols_with_fallback(app)
    if not valid_symbols:
        logging.warning("Valid symbols whitelist unavailable for scheduling alert")
        return

    if not validate_symbol(symbol, valid_symbols):
        logging.warning("Invalid symbol for alert: %s", symbol)
        return
    
    # dedupe by job name
    name = get_alert_job_name(chat_id, symbol, tf)
    jobs = app.job_queue.get_jobs_by_name(name)
    for j in jobs:
        j.schedule_removal()

    check_interval_sec = alert_check_interval_sec(tf)
    app.job_queue.run_repeating(
        alert_job_callback,
        interval=check_interval_sec,
        first=2,
        chat_id=chat_id,
        name=name,
        data={
            "symbol": symbol,
            "tf": tf,
            "tf_label": tf_label,
            "side": side,
            "backoff_sec": 0,
            "next_retry_ts": 0.0,
        },
    )


def unschedule_alert(app: Application, chat_id: int, symbol: str, tf: str) -> None:
    name = get_alert_job_name(chat_id, symbol, tf)
    jobs = app.job_queue.get_jobs_by_name(name)
    for j in jobs:
        j.schedule_removal()


def schedule_pair_rsi_alerts(app: Application, chat_id: int) -> None:
    if not validate_chat_id(chat_id):
        return
    jobs = app.job_queue.get_jobs_by_name(get_pair_rsi_alert_job_name(chat_id))
    for j in jobs:
        j.schedule_removal()
    app.job_queue.run_repeating(
        pair_rsi_alert_job_callback,
        interval=ALERT_CHECK_SEC,
        first=2,
        chat_id=chat_id,
        name=get_pair_rsi_alert_job_name(chat_id),
    )


def unschedule_pair_rsi_alerts(app: Application, chat_id: int) -> None:
    jobs = app.job_queue.get_jobs_by_name(get_pair_rsi_alert_job_name(chat_id))
    for j in jobs:
        j.schedule_removal()


def schedule_price_alert(app: Application, chat_id: int, key: str, symbol: str, target_price: float, direction: str) -> None:
    if not validate_chat_id(chat_id):
        return
    name = get_price_alert_job_name(chat_id, key)
    jobs = app.job_queue.get_jobs_by_name(name)
    for j in jobs:
        j.schedule_removal()
    app.job_queue.run_repeating(
        price_alert_job_callback,
        interval=PRICE_ALERT_CHECK_SEC,
        first=2,
        chat_id=chat_id,
        name=name,
        data={
            "key": key,
            "symbol": symbol,
            "target_price": float(target_price),
            "direction": direction,
        },
    )


def unschedule_price_alert(app: Application, chat_id: int, key: str) -> None:
    name = get_price_alert_job_name(chat_id, key)
    jobs = app.job_queue.get_jobs_by_name(name)
    for j in jobs:
        j.schedule_removal()


async def restore_alerts(app: Application) -> None:
    state_lock: asyncio.Lock = app.bot_data["state_lock"]
    async with state_lock:
        state = app.bot_data.get("state") or {
            "subs": {},
            "alerts": {},
            "pair_rsi_alerts": {},
            "price_alerts": {},
        }
        changed = _prune_expired_alerts_unsafe(state)
        alerts_all = copy.deepcopy(state.get("alerts", {}) or {})
        if changed:
            app.bot_data["state"] = state
            log_state_change(state, "restore_alerts_prune")
            await persist_state(app, state)

    valid_symbols = await get_valid_symbols_with_fallback(app)

    for chat_id_str, amap in alerts_all.items():
        try:
            chat_id = int(chat_id_str)
            if not validate_chat_id(chat_id):
                continue
        except Exception:
            continue
        if not isinstance(amap, dict):
            continue
        for _, a in amap.items():
            try:
                symbol = a.get("symbol")
                tf = a.get("tf")
                tf_label = a.get("tf_label") or f"{tf}m"
                side = a.get("side")
                if not symbol or not tf or not side:
                    continue
                if tf not in ALERT_INTERVALS or side not in {"L", "S"}:
                    continue
                if valid_symbols and not validate_symbol(symbol, valid_symbols):
                    unschedule_alert(app, chat_id, symbol, tf)
                    await delete_alert_state(
                        app,
                        chat_id,
                        symbol,
                        tf,
                        validate_symbol_check=False,
                    )
                    continue
                await schedule_alert(
                    app,
                    chat_id,
                    symbol,
                    tf,
                    tf_label,
                    side,
                    valid_symbols=valid_symbols,
                )
            except Exception as e:
                logging.warning("Failed restoring alert: %s", e)


async def restore_pair_rsi_alerts(app: Application) -> None:
    state_lock: asyncio.Lock = app.bot_data["state_lock"]
    async with state_lock:
        state = app.bot_data.get("state") or {
            "subs": {},
            "alerts": {},
            "pair_rsi_alerts": {},
            "price_alerts": {},
        }
        alerts_all = copy.deepcopy(state.get("pair_rsi_alerts", {}) or {})

    for chat_id_str, amap in alerts_all.items():
        try:
            chat_id = int(chat_id_str)
            if not validate_chat_id(chat_id):
                continue
        except Exception:
            continue
        if not isinstance(amap, dict) or not amap:
            continue
        schedule_pair_rsi_alerts(app, chat_id)


async def restore_price_alerts(app: Application) -> None:
    state_lock: asyncio.Lock = app.bot_data["state_lock"]
    async with state_lock:
        state = app.bot_data.get("state") or {
            "subs": {},
            "alerts": {},
            "pair_rsi_alerts": {},
            "price_alerts": {},
        }
        alerts_all = copy.deepcopy(state.get("price_alerts", {}) or {})

    for chat_id_str, amap in alerts_all.items():
        try:
            chat_id = int(chat_id_str)
            if not validate_chat_id(chat_id):
                continue
        except Exception:
            continue
        if not isinstance(amap, dict) or not amap:
            continue
        for key, entry in amap.items():
            symbol = entry.get("symbol")
            target = entry.get("price")
            direction = entry.get("direction")
            if not symbol or target is None or direction not in {"UP", "DOWN"}:
                continue
            schedule_price_alert(app, chat_id, key, symbol, float(target), direction)


# =========================
# CORE: sending scan result (photo + list message)
# =========================
def build_pairs_text(long_syms: List[str], short_syms: List[str]) -> str:
    # only lists, no RSI values
    lines = []
    
    if long_syms:
        lines.append("LONG:")
        for s in long_syms:
            lines.append(s)
    else:
        lines.append("LONG:")
        lines.append("No candidates found")
    
    lines.append("")
    
    if short_syms:
        lines.append("SHORT:")
        for s in short_syms:
            lines.append(s)
    else:
        lines.append("SHORT:")
        lines.append("No candidates found")
    
    lines.append("")
    
    if long_syms or short_syms:
        lines.append("Нажми на пару (кнопка) — получишь расчёт за последний час + кнопки Alerts.")
    
    return "\n".join(lines)


async def send_scan_result(
    app: Application,
    chat_id: int,
    long_rows: List[MonitorRow],
    short_rows: List[MonitorRow],
    symbols_scanned: int,
):
    long_syms = [r.symbol for r in long_rows]
    short_syms = [r.symbol for r in short_rows]

    png = render_png(long_rows, short_rows, symbols_scanned=symbols_scanned)
    await tg_call_with_retry(app.bot.send_photo, chat_id=chat_id, photo=png)

    text = build_pairs_text(long_syms, short_syms)
    kb = pairs_keyboard(long_syms, short_syms)
    await tg_call_with_retry(app.bot.send_message, chat_id=chat_id, text=text, reply_markup=kb)


async def run_monitor_once_internal(app: Application, chat_id: int, timeout_sec: Optional[int] = None):
    """Internal monitor function without timeout wrapper."""
    start_ts = time.monotonic()
    session = cast(
        aiohttp.ClientSession,
        get_required_bot_data(app, "http_session", aiohttp.ClientSession),
    )
    rate_limiter = cast(RateLimiter, get_required_bot_data(app, "rate_limiter", RateLimiter))
    symbols = await get_cached_top_symbols(app)
    sem = cast(
        asyncio.Semaphore,
        get_required_bot_data(app, "http_sem", lambda: asyncio.Semaphore(MAX_CONCURRENCY)),
    )
    logging.debug("monitor_start chat_id=%s symbols=%d", chat_id, len(symbols))

    results: List[MonitorRow] = []
    queue: asyncio.Queue[str] = asyncio.Queue()
    for symbol in symbols:
        queue.put_nowait(symbol)

    # Track task statistics
    total_tasks = len(symbols)
    success_count = 0
    error_count = 0

    async def worker() -> Tuple[List[MonitorRow], int, int]:
        local_results: List[MonitorRow] = []
        local_success = 0
        local_error = 0
        while True:
            try:
                symbol = queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            try:
                r = await compute_symbol_indicators(session, rate_limiter, sem, symbol)
                if r is not None:
                    local_results.append(r)
                    local_success += 1
                else:
                    local_error += 1
            except asyncio.CancelledError:
                logging.warning("Task cancelled during monitor scan")
                queue.put_nowait(symbol)
                raise
            except Exception as e:
                logging.warning("Failed to compute RSI for %s: %s", symbol, e)
                local_error += 1
            finally:
                queue.task_done()
        return local_results, local_success, local_error

    worker_count = min(MAX_CONCURRENCY, total_tasks) if total_tasks else 0
    tasks = [asyncio.create_task(worker()) for _ in range(worker_count)]

    timed_out = False
    try:
        if tasks:
            done, pending = await asyncio.wait(tasks, timeout=timeout_sec)
            if pending:
                timed_out = True
            for task in done:
                local_results, local_success, local_error = await task
                results.extend(local_results)
                success_count += local_success
                error_count += local_error
            for task in pending:
                task.cancel()
    finally:
        if tasks:
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    # Calculate error rate
    processed_count = success_count + error_count
    error_rate = error_count / processed_count if processed_count > 0 else 0
    duration = time.monotonic() - start_ts
    logging.debug(
        "monitor_done chat_id=%s symbols=%d success=%d errors=%d error_rate=%.3f duration=%.2fs",
        chat_id,
        total_tasks,
        success_count,
        error_count,
        error_rate,
        duration,
    )
    
    if timed_out:
        logging.warning(
            "monitor_partial chat_id=%s processed=%d total=%d timeout=%ss",
            chat_id,
            processed_count,
            total_tasks,
            timeout_sec,
        )
        await app.bot.send_message(
            chat_id=chat_id,
            text=(
                "⚠️ Мониторинг занял больше времени, отправляю частичный результат. "
                "Остальные пары будут обработаны в следующем цикле."
            ),
        )

    # Notify user if error rate is high
    if error_rate > ERROR_RATE_THRESHOLD:
        await app.bot.send_message(
            chat_id=chat_id,
            text=(
                f"⚠️ Внимание: {error_count}/{processed_count} символов не удалось обработать "
                f"({error_rate*100:.1f}% ошибок). Возможны проблемы с API или rate limiting."
            )
        )

    if not results:
        await app.bot.send_message(chat_id=chat_id, text="Не получилось собрать RSI (rate limit/временная ошибка).")
        return

    long_candidates = [r for r in results if is_long_candidate(r)]
    short_candidates = [r for r in results if is_short_candidate(r)]

    long_candidates.sort(key=lambda x: x.sum_rsi6)
    short_candidates.sort(key=lambda x: x.sum_rsi6, reverse=True)

    long_rows = long_candidates[:10]
    short_rows = short_candidates[:10]

    await send_scan_result(app, chat_id, long_rows, short_rows, symbols_scanned=processed_count)


async def run_monitor_once(app: Application, chat_id: int):
    """Run monitor with timeout protection."""
    locks = app.bot_data.setdefault("monitor_locks", {})
    lock = locks.setdefault(chat_id, asyncio.Lock())

    if lock.locked():
        await tg_call_with_retry(
            app.bot.send_message,
            chat_id=chat_id,
            text="⏳ Мониторинг уже выполняется, подождите…",
        )
        return

    monitor_tasks = cast(weakref.WeakSet, app.bot_data.setdefault("monitor_tasks", weakref.WeakSet()))
    current_task = asyncio.current_task()
    acquired = False
    try:
        if current_task:
            monitor_tasks.add(current_task)
        try:
            await asyncio.wait_for(lock.acquire(), timeout=0.05)
            acquired = True
        except asyncio.TimeoutError:
            await tg_call_with_retry(
                app.bot.send_message,
                chat_id=chat_id,
                text="⏳ Мониторинг уже выполняется, подождите…",
            )
            return
        rate_limiter = cast(RateLimiter, get_required_bot_data(app, "rate_limiter", RateLimiter))
        current_delay = await rate_limiter.get_current_delay()
        delay_factor = max(1.0, current_delay / RATE_LIMIT_DELAY)
        adaptive_timeout = int(MONITOR_TIMEOUT * delay_factor * 1.15)
        adaptive_timeout = min(adaptive_timeout, MAX_MONITOR_TIMEOUT)
        log_event(logging.INFO, "monitor_start", chat_id=chat_id, timeout_sec=adaptive_timeout)
        await run_monitor_once_internal(app, chat_id, timeout_sec=adaptive_timeout)
        log_event(logging.INFO, "monitor_done", chat_id=chat_id)
    finally:
        if acquired:
            lock.release()
        if current_task:
            monitor_tasks.discard(current_task)


# =========================
# JOB CALLBACKS
# =========================
async def sub_job_callback(context: ContextTypes.DEFAULT_TYPE):
    chat_id = context.job.chat_id
    try:
        await run_monitor_once(context.application, chat_id)
    except Exception as e:
        logging.exception("subscription job failed: %s", e)
        try:
            await context.application.bot.send_message(chat_id=chat_id, text=f"Ошибка мониторинга: {e}")
        except Exception:
            logging.error("Failed to send error message to chat %s", chat_id)


async def alert_job_callback(context: ContextTypes.DEFAULT_TYPE):
    job = context.job
    chat_id = job.chat_id
    data = job.data or {}
    symbol = data.get("symbol")
    tf = data.get("tf")
    tf_label = data.get("tf_label") or f"{tf}m"
    side = data.get("side")
    
    if not symbol or not tf or not side:
        return
    if tf not in ALERT_INTERVALS:
        logging.warning("Invalid tf for alert: %s", tf)
        return
    if side not in {"L", "S"}:
        return
    
    if not validate_chat_id(chat_id):
        logging.warning("Invalid chat_id for alert: %s", chat_id)
        return

    app = context.application
    alert_sem: asyncio.Semaphore = app.bot_data["alert_sem"]
    async with alert_sem:
        now = time.time()
        backoff_sec = int(data.get("backoff_sec") or 0)
        next_retry_ts = float(data.get("next_retry_ts") or 0.0)
        if backoff_sec > 0 and now < next_retry_ts:
            return
        notify_cache = app.bot_data.setdefault("alert_error_notify", {})
        ttl_sec = ALERT_ERROR_NOTIFY_TTL_HOURS * 3600
        last_prune = app.bot_data.get("alert_error_notify_last_prune", 0.0)
        if ttl_sec > 0 and now - last_prune >= 600:
            prune_ttl_dict(notify_cache, ttl_sec, now)
            app.bot_data["alert_error_notify_last_prune"] = now

        key = f"{symbol}|{tf}"
        entry = await get_alert_entry(app, chat_id, key)
        if not entry:
            unschedule_alert(app, chat_id, symbol, tf)
            return

        created_at = entry.get("created_at")
        if created_at is not None and is_alert_expired(created_at):
            unschedule_alert(app, chat_id, symbol, tf)
            await update_alert_entry(app, chat_id, symbol, tf, delete=True)
            return

        created_at_update = now if created_at is None else _ALERT_ENTRY_MISSING

        session: aiohttp.ClientSession = app.bot_data["http_session"]
        rate_limiter: RateLimiter = app.bot_data["rate_limiter"]

        try:
            sem: asyncio.Semaphore = app.bot_data["http_sem"]
            async with sem:
                valid_symbols = await get_valid_symbols_with_fallback(app)
            if not valid_symbols:
                logging.warning("Valid symbols whitelist unavailable for alert check")
                return
            if not validate_symbol(symbol, valid_symbols):
                logging.warning("Invalid symbol for alert: %s", symbol)
                return
            async with sem:
                closes = await get_kline_closes(session, rate_limiter, symbol, tf, KLINE_LIMIT)
            rsi = rsi_wilder(closes, RSI_PERIOD)
            if rsi is None:
                return

            prev = entry.get("last_above", None)

            if side == "L":
                threshold = ALERT_LONG_THRESHOLD
                condition_met = bool(rsi >= threshold)
                direction_label = "LONG"
                trigger_label = f">= {threshold:.0f}"
            else:
                threshold = ALERT_SHORT_THRESHOLD
                condition_met = bool(rsi <= threshold)
                direction_label = "SHORT"
                trigger_label = f"<= {threshold:.0f}"

            near = abs(rsi - threshold) <= ALERT_EPS

            # First run: do not spam, but alert if already met or near threshold.
            if prev is None:
                await update_alert_entry(
                    app,
                    chat_id,
                    symbol,
                    tf,
                    last_above=condition_met,
                    created_at=created_at_update,
                )
                if condition_met or near:
                    extra = f"hit {trigger_label}" if condition_met else f"hit ≈ {threshold:.0f}"
                    await app.bot.send_message(
                        chat_id=chat_id,
                        text=(
                            f"🔔 ALERT {symbol} {direction_label} RSI{RSI_PERIOD}({tf_label}) {extra}\n"
                            f"Current RSI: {rsi:.2f}"
                        ),
                    )
                    unschedule_alert(app, chat_id, symbol, tf)
                    await update_alert_entry(app, chat_id, symbol, tf, delete=True)
                return

            triggered = (not prev) and condition_met
            if triggered or near:
                extra = f"hit {trigger_label}" if triggered else f"hit ≈ {threshold:.0f}"
                await tg_call_with_retry(
                    app.bot.send_message,
                    chat_id=chat_id,
                    text=(
                        f"🔔 ALERT {symbol} {direction_label} RSI{RSI_PERIOD}({tf_label}) {extra}\n"
                        f"Current RSI: {rsi:.2f}"
                    ),
                )
                unschedule_alert(app, chat_id, symbol, tf)
                await update_alert_entry(app, chat_id, symbol, tf, delete=True)
                return

            await update_alert_entry(
                app,
                chat_id,
                symbol,
                tf,
                last_above=condition_met,
                created_at=created_at_update,
            )

            if backoff_sec > 0:
                data["backoff_sec"] = 0
                data["next_retry_ts"] = 0.0
        except Exception as e:
            logging.exception("alert job failed for %s: %s", symbol, e)
            notify_key = (chat_id, symbol, tf)
            last_sent = notify_cache.get(notify_key, 0.0)
            throttle_sec = ALERT_ERROR_THROTTLE_MIN * 60
            if now - last_sent >= throttle_sec:
                try:
                    await tg_call_with_retry(
                        app.bot.send_message,
                        chat_id=chat_id,
                        text=f"Alert {symbol}({tf_label}) временно не проверяется из-за ошибки API.",
                    )
                    notify_cache[notify_key] = now
                except Exception:
                    logging.error("Failed to send alert error message to chat %s", chat_id)

            next_backoff = max(ALERT_CHECK_SEC, backoff_sec * 2 if backoff_sec else ALERT_CHECK_SEC)
            next_backoff = min(next_backoff, ALERT_MAX_BACKOFF_SEC)
            if next_backoff != backoff_sec:
                data["backoff_sec"] = next_backoff
                data["next_retry_ts"] = now + next_backoff
                try:
                    await tg_call_with_retry(
                        app.bot.send_message,
                        chat_id=chat_id,
                        text=(
                            f"⚠️ Ошибка при проверке алерта {symbol}({tf_label}). "
                            f"Увеличиваю интервал проверки до {int(next_backoff // 60)} мин."
                        ),
                    )
                except Exception:
                    logging.error("Failed to send alert backoff message to chat %s", chat_id)


async def cleanup_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    app = context.application
    now = time.time()

    notify_cache = app.bot_data.setdefault("alert_error_notify", {})
    prune_ttl_dict(notify_cache, ALERT_ERROR_NOTIFY_TTL_HOURS * 3600, now)

    cooldowns = app.bot_data.setdefault("alert_cooldowns", {})
    prune_ttl_dict(cooldowns, ALERT_COOLDOWN_CACHE_TTL_SEC, now)

    heavy_cooldowns = app.bot_data.setdefault("heavy_action_cooldowns", {})
    prune_ttl_dict(heavy_cooldowns, ALERT_COOLDOWN_CACHE_TTL_SEC, now)

    monitor_tasks: Set[asyncio.Task] = app.bot_data.get("monitor_tasks", set())
    if monitor_tasks:
        app.bot_data["monitor_tasks"] = {task for task in monitor_tasks if not task.done()}

    state_lock: asyncio.Lock = app.bot_data["state_lock"]
    async with state_lock:
        state = app.bot_data.get("state") or {
            "subs": {},
            "alerts": {},
            "pair_rsi_alerts": {},
            "price_alerts": {},
        }
        changed = _prune_expired_alerts_unsafe(state, now=now)
        if changed:
            app.bot_data["state"] = state
            await persist_state(app, state)


async def price_alert_job_callback(context: ContextTypes.DEFAULT_TYPE):
    job = context.job
    chat_id = job.chat_id
    app = context.application
    data = job.data or {}

    if not validate_chat_id(chat_id):
        return

    key = data.get("key")
    symbol = data.get("symbol")
    target_price = data.get("target_price")
    direction = data.get("direction")
    if not key or not symbol or target_price is None or direction not in {"UP", "DOWN"}:
        if key:
            unschedule_price_alert(app, chat_id, key)
            await delete_price_alert_state(app, chat_id, key)
        return

    alerts_map = await get_price_alerts_for_chat(app, chat_id)
    entry = alerts_map.get(key)
    if not entry:
        unschedule_price_alert(app, chat_id, key)
        return

    try:
        session: aiohttp.ClientSession = app.bot_data["http_session"]
        rate_limiter: RateLimiter = app.bot_data["rate_limiter"]
        current_price = await get_last_price(session, rate_limiter, symbol)
        target_value = float(entry.get("price", target_price))
        direction = entry.get("direction", direction)
        if direction == "UP":
            condition_met = current_price >= target_value
        else:
            condition_met = current_price <= target_value
        if condition_met:
            price_label = normalize_price_text(Decimal(str(target_value)))
            await app.bot.send_message(
                chat_id=chat_id,
                text=(
                    f"🔔 Цена достигла цели для {symbol}: {price_label}\n"
                    f"Текущая цена: {current_price}"
                ),
            )
            unschedule_price_alert(app, chat_id, key)
            await delete_price_alert_state(app, chat_id, key)
    except Exception as e:
        logging.exception("price alert job failed for %s: %s", symbol, e)


async def pair_rsi_alert_job_callback(context: ContextTypes.DEFAULT_TYPE):
    chat_id = context.job.chat_id
    app = context.application

    alerts = await get_pair_rsi_alerts_for_chat(app, chat_id)
    if not alerts:
        unschedule_pair_rsi_alerts(app, chat_id)
        return

    session: aiohttp.ClientSession = app.bot_data["http_session"]
    rate_limiter: RateLimiter = app.bot_data["rate_limiter"]
    sem: asyncio.Semaphore = app.bot_data["http_sem"]

    try:
        valid_symbols = await get_valid_symbols_with_fallback(app)
        if not valid_symbols:
            logging.warning("Valid symbols whitelist unavailable for pair RSI alerts")
            return

        for key, entry in list(alerts.items()):
            symbol = entry.get("symbol")
            tf_intervals = entry.get("tfs") or []
            tf_labels = entry.get("tf_labels") or [TIMEFRAME_BY_INTERVAL.get(tf, f"{tf}m") for tf in tf_intervals]
            mode = entry.get("mode")
            if not symbol or not tf_intervals or mode not in {"LOW", "HIGH"}:
                await delete_pair_rsi_alert_state(app, chat_id, key)
                continue
            if not validate_symbol(symbol, valid_symbols):
                await delete_pair_rsi_alert_state(app, chat_id, key)
                continue

            async def one_tf(interval: str) -> Tuple[str, float]:
                async with sem:
                    closes = await get_kline_closes(session, rate_limiter, symbol, interval, KLINE_LIMIT)
                value = rsi_wilder(closes, RSI_PERIOD)
                if value is None:
                    raise BybitAPIError(f"Not enough data for RSI{RSI_PERIOD} {symbol} {interval}")
                return interval, value

            results = await asyncio.gather(*(one_tf(tf) for tf in tf_intervals))
            values_by_label = {
                TIMEFRAME_BY_INTERVAL.get(interval, f"{interval}m"): value
                for interval, value in results
            }

            if mode == "LOW":
                threshold = PAIR_RSI_LOW_THRESHOLD
                condition_met = all(value <= threshold for value in values_by_label.values())
                mode_label = f"<= {threshold:.0f}"
            else:
                threshold = PAIR_RSI_HIGH_THRESHOLD
                condition_met = all(value >= threshold for value in values_by_label.values())
                mode_label = f">= {threshold:.0f}"

            if condition_met:
                rsi_line = format_rsi_line_for_labels(values_by_label, tf_labels)
                await app.bot.send_message(
                    chat_id=chat_id,
                    text=(
                        f"🔔 RSI{RSI_PERIOD} {symbol} для таймфреймов {', '.join(tf_labels)} {mode_label}\n"
                        f"{rsi_line}"
                    ),
                    parse_mode="HTML",
                )
                await delete_pair_rsi_alert_state(app, chat_id, key)
    except Exception as e:
        logging.exception("pair RSI alert job failed: %s", e)


# =========================
# CALLBACK HANDLERS
# =========================
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logging.info("=== /start received. update_id=%s ===", getattr(update, "update_id", None))
    if not update.effective_chat or not update.message:
        logging.warning("cmd_start: no chat/message in update=%r", update)
        return
    chat_id = update.effective_chat.id
    logging.info("cmd_start: chat_id=%s", chat_id)
    app = context.application

    sub = await get_sub(app, chat_id)
    has_sub = bool(sub and sub.get("enabled"))

    text = (
        "Меню RSI-бота (Bybit USDT Perpetual)\n\n"
        f"SUM RSI({RSI_PERIOD}) по таймфреймам: {', '.join(lbl for (lbl, _) in TIMEFRAMES)}\n"
        "• Создай подписку (бот будет присылать картинку + список пар)\n"
        "• Нажми на пару в списке — получишь расчёт за последний час + кнопки Alerts\n"
        "• Кнопка «Информация по паре» — вручную введи символ и получи RSI/Stoch RSI\n"
    )
    await update.message.reply_text(text, reply_markup=main_menu_kb(has_sub))
    logging.info("cmd_start: replied to chat_id=%s", chat_id)


async def on_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_chat or not update.message:
        return
    chat_id = update.effective_chat.id
    app = context.application

    if context.user_data.get("price_alert_confirm"):
        await update.message.reply_text(
            "Подтвердите цену кнопками ниже или вернитесь в меню.",
            reply_markup=price_alert_confirm_kb(),
        )
        return

    if context.user_data.get("await_price_alert"):
        raw_price = (update.message.text or "").strip()
        price_value = parse_price_input(raw_price)
        if not price_value:
            await update.message.reply_text("Введите корректную цену, например 0.0015 или 25000.")
            return

        symbol = context.user_data.get("price_alert_symbol")
        if not symbol:
            context.user_data["await_price_alert"] = False
            await update.message.reply_text("Не удалось определить символ. Начните заново через меню.")
            return

        valid_symbols = await get_valid_symbols_with_fallback(app)
        if not valid_symbols:
            await update.message.reply_text("Whitelist временно недоступен. Попробуйте позже.")
            return
        if not validate_symbol(symbol, valid_symbols):
            await update.message.reply_text("Некорректный символ. Попробуйте заново через меню.")
            context.user_data["await_price_alert"] = False
            return

        session: aiohttp.ClientSession = app.bot_data["http_session"]
        rate_limiter: RateLimiter = app.bot_data["rate_limiter"]
        try:
            current_price = await get_last_price(session, rate_limiter, symbol)
        except Exception as e:
            logging.exception("Failed to fetch last price for %s: %s", symbol, e)
            await update.message.reply_text("Не удалось получить текущую цену. Попробуйте позже.")
            return

        direction = "UP" if float(price_value) >= current_price else "DOWN"
        price_text = normalize_price_text(price_value)
        if needs_price_confirmation(current_price, price_value):
            context.user_data["await_price_alert"] = False
            context.user_data["price_alert_confirm"] = {
                "symbol": symbol,
                "price_value": str(price_value),
                "price_text": price_text,
                "direction": direction,
                "current_price": current_price,
            }
            await update.message.reply_text(
                (
                    f"Текущая цена {symbol}: {current_price}\n"
                    f"Вы ввели {price_text}. Подтвердите, что значение введено верно."
                ),
                reply_markup=price_alert_confirm_kb(),
            )
            return

        created = await apply_price_alert(app, chat_id, symbol, price_value, direction, valid_symbols)
        if created:
            context.user_data["await_price_alert"] = False
            context.user_data.pop("price_alert_symbol", None)
        return

    if context.user_data.get("await_pair_info"):
        raw_symbol = (update.message.text or "").strip()
        symbol = normalize_symbol(raw_symbol)
        if not symbol:
            await update.message.reply_text("Введите торговую пару в формате BTCUSDT или BTC/USDT.")
            return

        valid_symbols = await get_valid_symbols_with_fallback(app)
        if not valid_symbols:
            await update.message.reply_text("Whitelist временно недоступен. Попробуйте позже.")
            return

        if not validate_symbol(symbol, valid_symbols):
            await update.message.reply_text("Некорректный символ. Попробуйте ещё раз.")
            return

        context.user_data["await_pair_info"] = False
        await update.message.reply_text(f"Считаю {symbol}…")

        session: aiohttp.ClientSession = app.bot_data["http_session"]
        rate_limiter: RateLimiter = app.bot_data["rate_limiter"]
        sem: asyncio.Semaphore = app.bot_data["http_sem"]

        try:
            rsi_values = await get_rsi_values(session, rate_limiter, sem, symbol)
            stoch_values = await get_stoch_rsi_values(session, rate_limiter, symbol, sem)
            macd_values = await get_macd_values(session, rate_limiter, sem, symbol)
        except Exception as e:
            logging.exception("PAIR INFO calc failed: %s", e)
            await update.message.reply_text(f"Ошибка расчёта {symbol}: {e}")
            return

        rsi_line = format_rsi_line(rsi_values)
        stoch_line = format_stoch_rsi_line(stoch_values)
        macd_line, dif_line, dea_line = format_macd_lines(macd_values)
        msg = f"<b>{symbol}</b>\n{rsi_line}\n{stoch_line}\n{macd_line}\n{dif_line}\n{dea_line}"
        await update.message.reply_text(msg, parse_mode="HTML", reply_markup=pair_info_actions_kb(symbol))
        return

    if context.user_data.get("await_interval"):
        raw = (update.message.text or "").strip()
        try:
            minutes = int(raw)
        except ValueError:
            await update.message.reply_text("Нужно число минут, например 15. Или выбери кнопкой ниже.", reply_markup=interval_picker_kb())
            return

        if not validate_interval(minutes):
            await update.message.reply_text(
                f"Интервал должен быть от {MIN_INTERVAL_MINUTES} до {MAX_INTERVAL_MINUTES} минут.",
                reply_markup=interval_picker_kb()
            )
            return

        await apply_interval(app, chat_id, minutes)
        context.user_data["await_interval"] = False
        return

    sub = await get_sub(app, chat_id)
    has_sub = bool(sub and sub.get("enabled"))
    await update.message.reply_text("Используй кнопки меню:", reply_markup=main_menu_kb(has_sub))


async def apply_interval(app: Application, chat_id: int, minutes: int):
    if not validate_interval(minutes):
        await app.bot.send_message(
            chat_id=chat_id,
            text=f"Интервал должен быть от {MIN_INTERVAL_MINUTES} до {MAX_INTERVAL_MINUTES} минут."
        )
        return

    await set_sub(app, chat_id, minutes, enabled=True)
    schedule_subscription(app, chat_id, minutes)

    await app.bot.send_message(chat_id=chat_id, text=f"✅ Подписка создана/обновлена: каждые {minutes} минут.")
    await app.bot.send_message(chat_id=chat_id, text="Первый прогон — собираю данные…")
    
    try:
        await run_monitor_once(app, chat_id)
    except Exception as e:
        logging.exception("Failed to run initial monitor: %s", e)
        await app.bot.send_message(chat_id=chat_id, text=f"Ошибка при первом запуске: {e}")


async def apply_price_alert(
    app: Application,
    chat_id: int,
    symbol: str,
    price_value: Decimal,
    direction: str,
    valid_symbols: Optional[Set[str]] = None,
) -> bool:
    ok, err, key = await set_price_alert_state(app, chat_id, symbol, price_value, direction, valid_symbols)
    if not ok:
        await app.bot.send_message(chat_id=chat_id, text=err or "Не удалось создать алерт по цене.")
        return False
    price_text = normalize_price_text(price_value)
    schedule_price_alert(app, chat_id, key, symbol, float(price_value), direction)
    direction_label = "выше" if direction == "UP" else "ниже"
    await app.bot.send_message(
        chat_id=chat_id,
        text=(
            f"✅ Alert по цене создан: {symbol} {direction_label} {price_text} "
            f"(проверка каждые {PRICE_ALERT_CHECK_SEC // 60} мин)"
        ),
    )
    return True


async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query or not query.message or not update.effective_chat:
        return
    await query.answer()

    chat_id = query.message.chat_id
    app = context.application
    session: aiohttp.ClientSession = app.bot_data["http_session"]
    rate_limiter: RateLimiter = app.bot_data["rate_limiter"]

    data = query.data or ""
    sub = await get_sub(app, chat_id)
    has_sub = bool(sub and sub.get("enabled"))

    if data == "MENU":
        context.user_data["await_interval"] = False
        context.user_data["await_pair_info"] = False
        context.user_data["await_price_alert"] = False
        context.user_data.pop("price_alert_symbol", None)
        context.user_data.pop("price_alert_confirm", None)
        context.user_data.pop("pair_track_symbol", None)
        context.user_data.pop("pair_track_tfs", None)
        await query.edit_message_text("Меню RSI-бота:", reply_markup=main_menu_kb(has_sub))
        return

    if data == "SUB_CREATE":
        context.user_data["await_interval"] = True
        context.user_data["await_pair_info"] = False
        context.user_data["await_price_alert"] = False
        context.user_data.pop("price_alert_symbol", None)
        context.user_data.pop("price_alert_confirm", None)
        await query.edit_message_text(
            "Введите интервал в минутах (например 15) сообщением в чат — или выберите кнопкой:",
            reply_markup=interval_picker_kb(),
        )
        return

    if data.startswith("SETINT:"):
        context.user_data["await_interval"] = False
        try:
            minutes = int(data.split(":", 1)[1])
        except Exception:
            await app.bot.send_message(chat_id=chat_id, text="Ошибка интервала. Попробуйте ещё раз.")
            return
        
        if not validate_interval(minutes):
            await app.bot.send_message(
                chat_id=chat_id,
                text=f"Интервал должен быть от {MIN_INTERVAL_MINUTES} до {MAX_INTERVAL_MINUTES} минут."
            )
            return
        
        await apply_interval(app, chat_id, minutes)
        return

    if data == "SUB_VIEW":
        context.user_data["await_interval"] = False
        sub = await get_sub(app, chat_id)
        if not sub or not sub.get("enabled"):
            await query.edit_message_text("Подписка не создана.\nНажмите «Создать подписку».", reply_markup=main_menu_kb(False))
            return
        interval_min = sub.get("interval_min")
        await query.edit_message_text(
            f"📋 Ваша подписка активна:\n• Интервал: {interval_min} мин",
            reply_markup=main_menu_kb(True),
        )
        return

    if data == "SUB_DELETE":
        context.user_data["await_interval"] = False
        unschedule_subscription(app, chat_id)
        await delete_sub(app, chat_id)
        await query.edit_message_text("🗑 Подписка удалена.", reply_markup=main_menu_kb(False))
        return

    if data == "RUN_NOW":
        context.user_data["await_interval"] = False
        context.user_data["await_pair_info"] = False
        context.user_data["await_price_alert"] = False
        context.user_data.pop("price_alert_symbol", None)
        context.user_data.pop("price_alert_confirm", None)
        remaining = await check_action_cooldown(app, chat_id, "RUN_NOW", HEAVY_ACTION_COOLDOWN_SEC)
        if remaining > 0:
            await tg_call_with_retry(
                app.bot.send_message,
                chat_id=chat_id,
                text=f"Подожди {int(math.ceil(remaining))} сек перед следующим запросом.",
            )
            return
        await tg_call_with_retry(app.bot.send_message, chat_id=chat_id, text="Собираю данные…")
        try:
            await run_monitor_once(app, chat_id)
        except Exception as e:
            logging.exception("RUN_NOW failed: %s", e)
            await tg_call_with_retry(app.bot.send_message, chat_id=chat_id, text=f"Ошибка: {e}")
        return

    if data == "PAIR_INFO":
        context.user_data["await_pair_info"] = True
        context.user_data["await_interval"] = False
        context.user_data["await_price_alert"] = False
        context.user_data.pop("price_alert_symbol", None)
        context.user_data.pop("price_alert_confirm", None)
        context.user_data.pop("pair_track_symbol", None)
        context.user_data.pop("pair_track_tfs", None)
        await query.edit_message_text("Введите торговую пару (пример BTCUSDT или BTC/USDT):")
        return

    if data.startswith("PRICE_ALERT|"):
        try:
            _, symbol = data.split("|", 1)
        except ValueError:
            return
        valid_symbols = await get_valid_symbols_with_fallback(app)
        if not valid_symbols:
            await app.bot.send_message(chat_id=chat_id, text="Whitelist временно недоступен.")
            return
        if not validate_symbol(symbol, valid_symbols):
            await app.bot.send_message(chat_id=chat_id, text="Некорректный символ.")
            return
        try:
            current_price = await get_last_price(session, rate_limiter, symbol)
        except Exception as e:
            logging.exception("Failed to fetch last price for %s: %s", symbol, e)
            await app.bot.send_message(chat_id=chat_id, text="Не удалось получить текущую цену. Попробуйте позже.")
            return

        context.user_data["await_price_alert"] = True
        context.user_data["await_pair_info"] = False
        context.user_data["price_alert_symbol"] = symbol
        context.user_data.pop("price_alert_confirm", None)
        await query.edit_message_text(
            f"Введите цену для alert по {symbol}.\nТекущая цена: {current_price}",
        )
        return

    if data.startswith("PRICE_ALERT_CONFIRM|"):
        parts = data.split("|")
        if len(parts) != 2:
            return
        action = parts[1]
        confirm_data = context.user_data.get("price_alert_confirm")
        if not confirm_data:
            await app.bot.send_message(chat_id=chat_id, text="Нет активного подтверждения.")
            return
        if action == "NO":
            symbol = confirm_data.get("symbol")
            current_price = confirm_data.get("current_price")
            if not symbol:
                await app.bot.send_message(chat_id=chat_id, text="Данные подтверждения устарели. Начните заново.")
                context.user_data.pop("price_alert_confirm", None)
                return
            context.user_data["await_price_alert"] = True
            context.user_data["price_alert_symbol"] = symbol
            context.user_data.pop("price_alert_confirm", None)
            await query.edit_message_text(
                f"Введите новую цену для {symbol}.\nТекущая цена: {current_price}",
            )
            return
        if action == "YES":
            symbol = confirm_data.get("symbol")
            price_raw = confirm_data.get("price_value")
            direction = confirm_data.get("direction")
            if not symbol or not price_raw or direction not in {"UP", "DOWN"}:
                await app.bot.send_message(chat_id=chat_id, text="Данные подтверждения устарели. Начните заново.")
                context.user_data.pop("price_alert_confirm", None)
                return
            price_value = parse_price_input(str(price_raw))
            if not price_value:
                await app.bot.send_message(chat_id=chat_id, text="Некорректная цена. Начните заново.")
                context.user_data.pop("price_alert_confirm", None)
                return
            created = await apply_price_alert(app, chat_id, symbol, price_value, direction)
            if created:
                context.user_data["await_price_alert"] = False
                context.user_data.pop("price_alert_symbol", None)
                context.user_data.pop("price_alert_confirm", None)
            return

    if data.startswith("PAIRTRACK|"):
        try:
            _, symbol = data.split("|", 1)
        except ValueError:
            return
        context.user_data["await_price_alert"] = False
        context.user_data.pop("price_alert_symbol", None)
        context.user_data.pop("price_alert_confirm", None)
        valid_symbols = await get_valid_symbols_with_fallback(app)
        if not valid_symbols:
            await app.bot.send_message(chat_id=chat_id, text="Whitelist временно недоступен.")
            return
        if not validate_symbol(symbol, valid_symbols):
            await app.bot.send_message(chat_id=chat_id, text="Некорректный символ.")
            return
        context.user_data["pair_track_symbol"] = symbol
        context.user_data["pair_track_tfs"] = set()
        await query.edit_message_text(
            "Выберите таймфреймы для отслеживания (можно несколько):",
            reply_markup=pair_track_tf_kb(symbol, set()),
        )
        return

    if data.startswith("PAIRTRACK_TF|"):
        parts = data.split("|")
        if len(parts) != 3:
            return
        _, symbol, label = parts
        if context.user_data.get("pair_track_symbol") != symbol:
            context.user_data["pair_track_symbol"] = symbol
            context.user_data["pair_track_tfs"] = set()
        selected: Set[str] = context.user_data.get("pair_track_tfs", set())
        if label in selected:
            selected.remove(label)
        else:
            selected.add(label)
        context.user_data["pair_track_tfs"] = selected
        await query.edit_message_text(
            "Выберите таймфреймы для отслеживания (можно несколько):",
            reply_markup=pair_track_tf_kb(symbol, selected),
        )
        return

    if data.startswith("PAIRTRACK_TF_CONFIRM|"):
        _, symbol = data.split("|", 1)
        selected: Set[str] = context.user_data.get("pair_track_tfs", set())
        if not selected:
            await app.bot.send_message(chat_id=chat_id, text="Выберите хотя бы один таймфрейм.")
            return
        await query.edit_message_text(
            "Выберите диапазон для RSI6:",
            reply_markup=pair_track_range_kb(symbol),
        )
        return

    if data.startswith("PAIRTRACK_TF_BACK|"):
        _, symbol = data.split("|", 1)
        selected: Set[str] = context.user_data.get("pair_track_tfs", set())
        await query.edit_message_text(
            "Выберите таймфреймы для отслеживания (можно несколько):",
            reply_markup=pair_track_tf_kb(symbol, selected),
        )
        return

    if data.startswith("PAIRTRACK_RANGE|"):
        parts = data.split("|")
        if len(parts) != 3:
            return
        _, symbol, mode = parts
        selected: Set[str] = context.user_data.get("pair_track_tfs", set())
        if not selected:
            await app.bot.send_message(chat_id=chat_id, text="Выберите таймфреймы для отслеживания.")
            return
        valid_symbols = await get_valid_symbols_with_fallback(app)
        if not valid_symbols:
            await app.bot.send_message(chat_id=chat_id, text="Whitelist временно недоступен.")
            return
        if not validate_symbol(symbol, valid_symbols):
            await app.bot.send_message(chat_id=chat_id, text="Некорректный символ.")
            return
        tf_intervals = [interval for label, interval in normalize_tf_labels(selected)]
        ok, err = await set_pair_rsi_alert_state(app, chat_id, symbol, tf_intervals, mode, valid_symbols)
        if not ok:
            await app.bot.send_message(chat_id=chat_id, text=err or "Не удалось создать отслеживание.")
            return
        schedule_pair_rsi_alerts(app, chat_id)
        context.user_data.pop("pair_track_symbol", None)
        context.user_data.pop("pair_track_tfs", None)
        mode_label = (
            f"<= {PAIR_RSI_LOW_THRESHOLD:.0f}" if mode == "LOW" else f">= {PAIR_RSI_HIGH_THRESHOLD:.0f}"
        )
        tf_labels = [label for label, _ in normalize_tf_labels(selected)]
        await query.edit_message_text(
            f"✅ Отслеживание включено: {symbol} RSI{RSI_PERIOD} {mode_label} "
            f"для таймфреймов {', '.join(tf_labels)} (проверка каждые {ALERT_CHECK_SEC//60} мин)."
        )
        return

    if data.startswith("SECTION|"):
        return

    # Click on symbol from list
    if data.startswith("PAIR|"):
        # format: PAIR|L|BTCUSDT  or PAIR|S|BTCUSDT
        try:
            _, side, symbol = data.split("|", 2)
            if side not in {"L", "S"}:
                await tg_call_with_retry(app.bot.send_message, chat_id=chat_id, text="Некорректное направление.")
                return

            valid_symbols = await get_valid_symbols_with_fallback(app)
            if not valid_symbols:
                await tg_call_with_retry(app.bot.send_message, chat_id=chat_id, text="Whitelist временно недоступен.")
                return

            if not validate_symbol(symbol, valid_symbols):
                await tg_call_with_retry(app.bot.send_message, chat_id=chat_id, text="Некорректный символ.")
                return
        except Exception:
            return

        remaining = await check_action_cooldown(app, chat_id, "PAIR", HEAVY_ACTION_COOLDOWN_SEC)
        if remaining > 0:
            await tg_call_with_retry(
                app.bot.send_message,
                chat_id=chat_id,
                text=f"Подожди {int(math.ceil(remaining))} сек перед следующим расчётом.",
            )
            return

        await tg_call_with_retry(app.bot.send_message, chat_id=chat_id, text=f"Считаю {symbol}…")

        try:
            price = await get_last_price(session, rate_limiter, symbol)
            sem: asyncio.Semaphore = app.bot_data["http_sem"]
            try:
                stoch_values = await get_stoch_rsi_values(session, rate_limiter, symbol, sem)
            except Exception as e:
                logging.warning("Failed to compute Stoch RSI for %s: %s", symbol, e)
                stoch_values = {}
            stoch_line = format_stoch_rsi_line(stoch_values)

            try:
                macd_values = await get_macd_values(session, rate_limiter, sem, symbol)
            except Exception as e:
                logging.warning("Failed to compute MACD for %s: %s", symbol, e)
                macd_values = {}
            macd_line, dif_line, dea_line = format_macd_lines(macd_values)

            direction_label = "LONG" if side == "L" else "SHORT"
            msg = (
                f"<b>{symbol} — {direction_label}</b>\n"
                f"Current: <code>{price:.6f}</code>\n\n"
                f"{stoch_line}\n\n"
                f"{macd_line}\n"
                f"{dif_line}\n"
                f"{dea_line}\n"
            )

            await tg_call_with_retry(
                app.bot.send_message,
                chat_id=chat_id,
                text=msg,
                parse_mode="HTML",
                reply_markup=alerts_kb(symbol, side),
            )

        except Exception as e:
            logging.exception("PAIR calc failed: %s", e)
            await tg_call_with_retry(
                app.bot.send_message,
                chat_id=chat_id,
                text=f"Ошибка расчёта {symbol}: {e}",
            )

        return

    # Alert button from calc message
    if data.startswith("ALERT|"):
        # format: ALERT|15|L|BTCUSDT
        try:
            parts = data.split("|")
            if len(parts) != 4:
                await app.bot.send_message(chat_id=chat_id, text="Не удалось определить направление. Выберите пару заново.")
                return
            _, tf, side, symbol = parts
            if side not in {"L", "S"}:
                await app.bot.send_message(chat_id=chat_id, text="Некорректное направление. Выберите пару заново.")
                return
            if tf not in ALERT_INTERVALS:
                await app.bot.send_message(chat_id=chat_id, text="Некорректный таймфрейм.")
                return
            
            valid_symbols = await get_valid_symbols_with_fallback(app)
            if not valid_symbols:
                await app.bot.send_message(chat_id=chat_id, text="Whitelist временно недоступен.")
                return

            if not validate_symbol(symbol, valid_symbols):
                await app.bot.send_message(chat_id=chat_id, text="Некорректный символ.")
                return
        except Exception:
            return

        tf_label = next((lbl for (lbl, iv) in ALERT_TFS if iv == tf), f"{tf}m")
        direction_label = "LONG" if side == "L" else "SHORT"
        threshold = ALERT_LONG_THRESHOLD if side == "L" else ALERT_SHORT_THRESHOLD
        trigger_label = f">= {threshold:.0f}" if side == "L" else f"<= {threshold:.0f}"

        if ALERT_COOLDOWN_SEC > 0:
            cooldowns = app.bot_data.setdefault("alert_cooldowns", {})
            cd_lock: asyncio.Lock = app.bot_data.setdefault("alert_cooldowns_lock", asyncio.Lock())
            now = time.time()
            allowed = False
            remaining = 0.0
            async with cd_lock:
                last_ts = cooldowns.get(chat_id, 0.0)
                remaining = ALERT_COOLDOWN_SEC - (now - last_ts)
                if remaining <= 0:
                    cooldowns[chat_id] = now
                    allowed = True
            if not allowed:
                await tg_call_with_retry(
                    app.bot.send_message,
                    chat_id=chat_id,
                    text=f"Подожди {int(math.ceil(remaining))} сек перед созданием нового алерта.",
                )
                return

        # Save alert with last_above = None (first check will set)
        ok, err = await set_alert_state(
            app,
            chat_id,
            symbol,
            tf,
            tf_label,
            side,
            last_above=None,
            valid_symbols=valid_symbols,
        )
        if not ok:
            await tg_call_with_retry(
                app.bot.send_message,
                chat_id=chat_id,
                text=err or "Не удалось создать алерт.",
            )
            return
        await schedule_alert(app, chat_id, symbol, tf, tf_label, side, valid_symbols)

        check_interval_sec = alert_check_interval_sec(tf)
        await tg_call_with_retry(
            app.bot.send_message,
            chat_id=chat_id,
            text=(
                f"✅ Alert включён: {symbol} {direction_label} RSI{RSI_PERIOD}({tf_label}) {trigger_label} "
                f"(проверка каждые {check_interval_sec//60} мин)"
            ),
        )
        return

    # Unknown
    await app.bot.send_message(chat_id=chat_id, text="Неизвестная кнопка. Открой меню: /start")


# =========================
# INIT / SHUTDOWN
# =========================
async def post_init(app: Application):
    try:
        app.bot_data["http_session"] = aiohttp.ClientSession()
        app.bot_data["rate_limiter"] = RateLimiter()
        app.bot_data["state_lock"] = asyncio.Lock()
        app.bot_data["state_write_lock"] = asyncio.Lock()
        app.bot_data["state_save_event"] = asyncio.Event()
        app.bot_data["state_save_done"] = asyncio.Event()
        app.bot_data["state_save_done"].set()
        app.bot_data["state_save_seq"] = 0
        app.bot_data["state_save_last_requested_seq"] = 0
        app.bot_data["state_save_seq_lock"] = asyncio.Lock()
        app.bot_data["state_save_task"] = asyncio.create_task(state_save_worker(app))
        app.bot_data["perp_symbols_lock"] = asyncio.Lock()
        app.bot_data["tickers_lock"] = asyncio.Lock()
        app.bot_data["innovation_symbols_lock"] = asyncio.Lock()
        app.bot_data["http_sem"] = asyncio.Semaphore(MAX_CONCURRENCY)
        app.bot_data["alert_sem"] = asyncio.Semaphore(ALERT_MAX_CONCURRENCY)
        app.bot_data["monitor_tasks"] = weakref.WeakSet()
        app.bot_data["monitor_locks"] = {}
        app.bot_data["state"] = load_state()
        app.bot_data["state"].setdefault("pair_rsi_alerts", {})
        app.bot_data["state"].setdefault("price_alerts", {})
        app.bot_data["perp_symbols_cache"] = None
        cached_symbols = load_perp_symbols()
        if cached_symbols:
            app.bot_data["perp_symbols_cache"] = CacheItem(ts=0.0, value=cached_symbols)
        app.bot_data["tickers_cache"] = None
        app.bot_data["innovation_symbols_cache"] = None
        app.bot_data["innovation_symbols_refresh_task"] = None
        app.bot_data["alert_cooldowns"] = {}
        app.bot_data.setdefault("alert_cooldowns_lock", asyncio.Lock())
        app.bot_data.setdefault("heavy_action_cooldowns", {})
        app.bot_data.setdefault("heavy_action_cooldowns_lock", asyncio.Lock())
        app.bot_data["alert_error_notify"] = {}
        app.job_queue.run_repeating(cleanup_job, interval=CLEANUP_INTERVAL_SEC, first=CLEANUP_INTERVAL_SEC)

        # restore subscriptions with proper validation
        subs: dict = (app.bot_data["state"] or {}).get("subs", {})
        for chat_id_str, sub in subs.items():
            try:
                if not sub.get("enabled", False):
                    continue
                interval_min = int(sub.get("interval_min", 0))
                chat_id = int(chat_id_str)

                # Validate both chat_id and interval
                if not validate_chat_id(chat_id) or not validate_interval(interval_min):
                    logging.warning("Skipping invalid subscription: chat_id=%s, interval=%s", chat_id, interval_min)
                    continue

                schedule_subscription(app, chat_id, interval_min)
                logging.info("Restored subscription chat_id=%s interval=%s", chat_id, interval_min)
            except Exception as e:
                logging.warning("Failed to restore sub %s: %s", chat_id_str, e)

        # restore alerts
        await restore_alerts(app)
        logging.info("Restored alerts")

        await restore_pair_rsi_alerts(app)
        logging.info("Restored pair RSI alerts")

        await restore_price_alerts(app)
        logging.info("Restored price alerts")
    except Exception as e:
        logging.exception("post_init failed; applying safe defaults", exc_info=e)
        session = app.bot_data.get("http_session")
        if session:
            with contextlib.suppress(Exception):
                await session.close()
        app.bot_data["http_session"] = aiohttp.ClientSession()
        app.bot_data["rate_limiter"] = RateLimiter()
        app.bot_data["state_lock"] = asyncio.Lock()
        app.bot_data["state_write_lock"] = asyncio.Lock()
        app.bot_data["state_save_event"] = asyncio.Event()
        app.bot_data["state_save_done"] = asyncio.Event()
        app.bot_data["state_save_done"].set()
        app.bot_data["state_save_seq"] = 0
        app.bot_data["state_save_last_requested_seq"] = 0
        app.bot_data["state_save_seq_lock"] = asyncio.Lock()
        app.bot_data["state_save_task"] = None
        app.bot_data["perp_symbols_lock"] = asyncio.Lock()
        app.bot_data["tickers_lock"] = asyncio.Lock()
        app.bot_data["innovation_symbols_lock"] = asyncio.Lock()
        app.bot_data["http_sem"] = asyncio.Semaphore(MAX_CONCURRENCY)
        app.bot_data["alert_sem"] = asyncio.Semaphore(ALERT_MAX_CONCURRENCY)
        app.bot_data["monitor_tasks"] = weakref.WeakSet()
        app.bot_data["monitor_locks"] = {}
        app.bot_data["state"] = load_state()
        app.bot_data["state"].setdefault("pair_rsi_alerts", {})
        app.bot_data["state"].setdefault("price_alerts", {})
        app.bot_data["perp_symbols_cache"] = None
        app.bot_data["tickers_cache"] = None
        app.bot_data["innovation_symbols_cache"] = None
        app.bot_data["innovation_symbols_refresh_task"] = None
        app.bot_data["alert_cooldowns"] = {}
        app.bot_data.setdefault("alert_cooldowns_lock", asyncio.Lock())
        app.bot_data.setdefault("heavy_action_cooldowns", {})
        app.bot_data.setdefault("heavy_action_cooldowns_lock", asyncio.Lock())
        app.bot_data["alert_error_notify"] = {}


async def post_shutdown(app: Application):
    global BINANCE_SYMBOLS_CACHE
    global BINANCE_SYMBOLS_CACHE_LOCK
    monitor_tasks = cast(weakref.WeakSet, app.bot_data.get("monitor_tasks", weakref.WeakSet()))
    for task in list(monitor_tasks):
        if not task.done():
            task.cancel()
    if monitor_tasks:
        gather_task = asyncio.gather(*monitor_tasks, return_exceptions=True)
        try:
            with contextlib.suppress(asyncio.CancelledError):
                await asyncio.wait_for(gather_task, timeout=SHUTDOWN_TASK_TIMEOUT)
        except asyncio.TimeoutError:
            logging.warning("Monitor task shutdown timed out after %ss", SHUTDOWN_TASK_TIMEOUT)
        monitor_tasks.clear()

    sess: aiohttp.ClientSession = app.bot_data.get("http_session")
    if sess:
        await sess.close()
    BINANCE_SYMBOLS_CACHE = None
    BINANCE_SYMBOLS_CACHE_LOCK = None
    await flush_state(app)


# =========================
# MAIN
# =========================
def main():
    try:
        validate_config()
    except Exception as e:
        logging.error("Config validation failed: %s", e)
        sys.exit(1)

    tg_request = HTTPXRequest(
        connect_timeout=30,
        read_timeout=30,
        write_timeout=30,
        pool_timeout=30,
    )

    app = (
        ApplicationBuilder()
        .token(TELEGRAM_BOT_TOKEN)
        .request(tg_request)
        .post_init(post_init)
        .post_shutdown(post_shutdown)
        .build()
    )

    async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
        err = context.error
        if isinstance(err, Forbidden):
            logging.warning("Forbidden (bot blocked / no rights). update=%r", update)
            return
        if isinstance(err, BadRequest):
            logging.warning("BadRequest while processing update=%r", update, exc_info=err)
            return
        if isinstance(err, (TimedOut, NetworkError)):
            logging.warning("Telegram network error while processing update=%r", update, exc_info=err)
            return
        logging.exception("Unhandled error while processing update=%r", update, exc_info=err)

    app.add_error_handler(error_handler)

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CallbackQueryHandler(on_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))

    logging.info("Bot started")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
