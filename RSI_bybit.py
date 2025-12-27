#!/usr/bin/env python3
import asyncio
import copy
import contextlib
import json
import logging
import os
import random
from math import ceil
import time
import math
from decimal import Decimal, InvalidOperation
from dataclasses import dataclass
from datetime import datetime
from io import BytesIO
from typing import Dict, List, Optional, Tuple, Set
from zoneinfo import ZoneInfo

import aiohttp
from dotenv import load_dotenv
from PIL import Image, ImageDraw, ImageFont

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
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
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")

RSI_PERIOD = 6
STOCH_RSI_PERIOD = int(os.getenv("STOCH_RSI_PERIOD", "14"))
LEVERAGE = float(os.getenv("LEVERAGE", "20"))

# Unified timeframe registry (label -> interval in minutes)
TIMEFRAME_REGISTRY: Dict[str, str] = {
    "5m": "5",
    "15m": "15",
    "30m": "30",
    "1h": "60",
    "2h": "120",
    "4h": "240",
}
TIMEFRAME_BY_INTERVAL = {interval: label for label, interval in TIMEFRAME_REGISTRY.items()}

# RSI timeframes for SUM (now includes 5m)
TIMEFRAMES: List[Tuple[str, str]] = [(label, interval) for label, interval in TIMEFRAME_REGISTRY.items()]

# Alert options (fixed to 5m/15m/30m)
ALERT_TFS: List[Tuple[str, str]] = [
    (label, interval)
    for label, interval in TIMEFRAME_REGISTRY.items()
    if label in {"5m", "15m", "30m"}
]
ALERT_INTERVALS = {iv for _, iv in ALERT_TFS}

STOCH_RSI_TFS: List[Tuple[str, str]] = list(TIMEFRAMES)

PAIR_RSI_LOW_THRESHOLD = float(os.getenv("PAIR_RSI_LOW_THRESHOLD", "15"))
PAIR_RSI_HIGH_THRESHOLD = float(os.getenv("PAIR_RSI_HIGH_THRESHOLD", "90"))

ALERT_CHECK_SEC = int(os.getenv("ALERT_CHECK_SEC", "300"))  # check every 5 minutes by default
ALERT_EPS = float(os.getenv("ALERT_EPS", "0.10"))  # "equals threshold" tolerance
ALERT_LONG_THRESHOLD = float(os.getenv("ALERT_LONG_THRESHOLD", "40"))
ALERT_SHORT_THRESHOLD = float(os.getenv("ALERT_SHORT_THRESHOLD", "60"))
PRICE_ALERT_CHECK_SEC = int(os.getenv("PRICE_ALERT_CHECK_SEC", "180"))
ALERT_ERROR_THROTTLE_MIN = int(os.getenv("ALERT_ERROR_THROTTLE_MIN", "30"))
ALERT_ERROR_NOTIFY_TTL_HOURS = float(os.getenv("ALERT_ERROR_NOTIFY_TTL_HOURS", "24"))
MAX_ALERTS_PER_CHAT = int(os.getenv("MAX_ALERTS_PER_CHAT", "50"))
MAX_ALERTS_GLOBAL = int(os.getenv("MAX_ALERTS_GLOBAL", "1000"))
ALERT_TTL_HOURS = float(os.getenv("ALERT_TTL_HOURS", "24"))
ALERT_COOLDOWN_SEC = int(os.getenv("ALERT_COOLDOWN_SEC", "0"))
SHUTDOWN_TASK_TIMEOUT = int(os.getenv("SHUTDOWN_TASK_TIMEOUT", "10"))

MAX_SYMBOLS = int(os.getenv("MAX_SYMBOLS", "200"))
MAX_CONCURRENCY = int(os.getenv("MAX_CONCURRENCY", "20"))  # Increased from 10 for better performance
KLINE_LIMIT = int(os.getenv("KLINE_LIMIT", "120"))

TICKERS_CACHE_TTL = int(os.getenv("TICKERS_CACHE_TTL", "60"))
PERP_SYMBOLS_CACHE_TTL = int(os.getenv("PERP_SYMBOLS_CACHE_TTL", "3600"))

LOCAL_TZ = ZoneInfo(os.getenv("LOCAL_TZ", "Asia/Ho_Chi_Minh"))

FONT_SIZE = int(os.getenv("FONT_SIZE", "20"))
IMAGE_PADDING = int(os.getenv("IMAGE_PADDING", "26"))
LINE_SPACING = int(os.getenv("LINE_SPACING", "6"))

STATE_FILE = os.getenv("STATE_FILE", "bot_state.json")
if not os.path.isabs(STATE_FILE):
    STATE_FILE = os.path.join(BASE_DIR, STATE_FILE)

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
    if MAX_SYMBOLS <= 0:
        errors.append("MAX_SYMBOLS must be > 0")
    if MAX_CONCURRENCY <= 0:
        errors.append("MAX_CONCURRENCY must be > 0")
    if KLINE_LIMIT <= 0:
        errors.append("KLINE_LIMIT must be > 0")
    if RSI_PERIOD <= 0:
        errors.append("RSI_PERIOD must be > 0")
    if STOCH_RSI_PERIOD <= 0:
        errors.append("STOCH_RSI_PERIOD must be > 0")
    if not (0 <= PAIR_RSI_LOW_THRESHOLD < PAIR_RSI_HIGH_THRESHOLD <= 100):
        errors.append("PAIR_RSI thresholds must be 0 <= low < high <= 100")
    if RATE_LIMIT_DELAY <= 0:
        errors.append("RATE_LIMIT_DELAY must be > 0")
    if RATE_LIMIT_MAX_DELAY < RATE_LIMIT_DELAY:
        errors.append("RATE_LIMIT_MAX_DELAY must be >= RATE_LIMIT_DELAY")
    if RATE_LIMIT_QUEUE_TIMEOUT <= 0:
        errors.append("RATE_LIMIT_QUEUE_TIMEOUT must be > 0")
    if RATE_LIMIT_MIN_SPACING <= 0:
        errors.append("RATE_LIMIT_MIN_SPACING must be > 0")
    if MONITOR_TIMEOUT <= 0:
        errors.append("MONITOR_TIMEOUT must be > 0")

    if errors:
        msg = "Config validation failed:\n- " + "\n- ".join(errors)
        raise RuntimeError(msg)

# =========================
# LOGGING
# =========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

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


def save_state_json(state_json: str) -> None:
    try:
        tmp = STATE_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(state_json)
        os.replace(tmp, STATE_FILE)
    except OSError as e:
        if e.errno == 28:
            logging.error("Failed to save state: no space left on device")
            with contextlib.suppress(Exception):
                if os.path.exists(tmp):
                    os.remove(tmp)
            return
        logging.warning("Failed to save state: %s", e)
    except Exception as e:
        logging.warning("Failed to save state: %s", e)


def save_state(state: dict) -> None:
    try:
        state_json = json.dumps(state, ensure_ascii=False, indent=2)
    except Exception as e:
        logging.warning("Failed to serialize state: %s", e)
        return
    save_state_json(state_json)


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
        return set(perp_cache.value)

    try:
        symbols = await get_cached_perp_symbols(app)
    except Exception as e:
        logging.warning("Failed to refresh perp symbols cache: %s", e)
        return None

    return set(symbols)


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
    cleaned = "".join(ch for ch in raw.strip().upper() if ch.isalnum())
    if not cleaned or len(cleaned) > 30:
        return None
    if not cleaned.endswith("USDT"):
        return None
    return cleaned


def parse_price_input(raw: str) -> Optional[Decimal]:
    if not isinstance(raw, str):
        return None
    cleaned = raw.strip().replace(",", ".")
    if not cleaned:
        return None
    try:
        value = Decimal(cleaned)
    except InvalidOperation:
        return None
    if not value.is_finite() or value <= 0:
        return None
    return value


def normalize_price_text(value: Decimal) -> str:
    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


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
) -> dict:
    backoff = 0.8
    last_err: Optional[Exception] = None

    for attempt in range(retries):
        try:
            await rate_limiter.acquire()
            
            async with session.get(
                url,
                params=params,
                timeout=aiohttp.ClientTimeout(total=20),
            ) as resp:
                if resp.status in {429, 503}:
                    retry_after = resp.headers.get("Retry-After")
                    await rate_limiter.report_rate_limit()
                    if retry_after:
                        try:
                            await asyncio.sleep(float(retry_after))
                        except (TypeError, ValueError):
                            pass
                data = await resp.json(content_type=None)

                if not isinstance(data, dict):
                    raise BybitAPIError(f"Unexpected response payload type: {type(data).__name__}")

                # Common rate limit code
                if isinstance(data, dict) and data.get("retCode") == 10006:
                    await rate_limiter.report_rate_limit()
                    raise BybitAPIError("Bybit rate limit: retCode=10006 (Too many visits!)")

                if resp.status >= 400:
                    raise BybitAPIError(f"HTTP {resp.status}: {data}")

                if data.get("retCode") not in (0, None):
                    raise BybitAPIError(f"Bybit retCode={data.get('retCode')} retMsg={data.get('retMsg')}")

                # Report success to gradually reduce delays
                await rate_limiter.report_success()
                return data

        except (asyncio.TimeoutError, aiohttp.ClientError, BybitAPIError) as e:
            last_err = e
            if attempt == retries - 1:
                break
            await asyncio.sleep(backoff)
            backoff *= 1.7

    raise BybitAPIError(f"Request failed after retries: {last_err}")


# =========================
# BYBIT DATA: Perpetual symbols only
# =========================
async def get_all_usdt_linear_perp_symbols(session: aiohttp.ClientSession, rate_limiter: RateLimiter) -> List[str]:
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

    return sorted(set(out))


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
) -> List[str]:
    tickers = await get_linear_tickers(session, rate_limiter)
    rows: List[Tuple[str, float]] = []

    for it in tickers:
        sym = it.get("symbol", "")
        if sym not in perp_symbols_set:
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
    payload = await api_get_json(session, url, params, rate_limiter)
    rows = ((payload.get("result") or {}).get("list")) or []
    # API returns newest first
    return rows


async def get_kline_closes(session: aiohttp.ClientSession, rate_limiter: RateLimiter, symbol: str, interval: str, limit: int) -> List[float]:
    rows = await get_kline_rows(session, rate_limiter, symbol, interval, limit)
    if not rows:
        raise BybitAPIError(f"No kline rows for {symbol} interval={interval} limit={limit}")
    closes: List[float] = []
    for c in rows:
        # [startTime, open, high, low, close, volume, turnover]
        if not isinstance(c, list) or len(c) < 5:
            continue
        try:
            value = float(c[4])
            if not math.isfinite(value):
                continue
            closes.append(value)
        except Exception:
            continue
    # Reverse to get oldest first for RSI calculation
    closes.reverse()
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


def stoch_rsi_from_closes(
    closes: List[float],
    rsi_period: int,
    stoch_period: int,
) -> Optional[float]:
    rsi_values = rsi_wilder_series(closes, rsi_period)
    if len(rsi_values) < stoch_period:
        return None
    window = rsi_values[-stoch_period:]
    low = min(window)
    high = max(window)
    if high == low:
        return 0.0
    return (rsi_values[-1] - low) / (high - low) * 100.0


def is_long_candidate(rsis: Dict[str, float]) -> bool:
    if any(val > 50 for val in rsis.values()):
        return False
    return rsis["5m"] <= 35 and rsis["15m"] <= 35


def is_short_candidate(rsis: Dict[str, float]) -> bool:
    if any(val < 70 for val in rsis.values()):
        return False
    return rsis["5m"] >= 80 and rsis["15m"] >= 80


async def compute_symbol_rsi_sum(
    session: aiohttp.ClientSession,
    rate_limiter: RateLimiter,
    sem: asyncio.Semaphore,
    symbol: str,
) -> Optional[Tuple[str, float, Dict[str, float]]]:
    rsis: Dict[str, float] = {}

    async def one_tf(tf_label: str, interval: str):
        async with sem:
            closes = await get_kline_closes(session, rate_limiter, symbol, interval, KLINE_LIMIT)
        val = rsi_wilder(closes, RSI_PERIOD)
        if val is None:
            raise BybitAPIError(f"Not enough data for RSI{RSI_PERIOD} {symbol} {tf_label}")
        rsis[tf_label] = val

    try:
        await asyncio.gather(*(one_tf(lbl, iv) for (lbl, iv) in TIMEFRAMES))
    except Exception as e:
        logging.warning("Failed to compute RSI for %s: %s", symbol, e)
        return None

    s = sum(rsis[lbl] for (lbl, _) in TIMEFRAMES)
    return symbol, s, rsis


# =========================
# EXTRA: symbol click calc (1h high/low + RR)
# =========================
async def get_last_price(session: aiohttp.ClientSession, rate_limiter: RateLimiter, symbol: str) -> float:
    items = await get_linear_tickers(session, rate_limiter, symbol=symbol)
    if not items:
        raise BybitAPIError(f"Ticker empty for {symbol}")
    it = items[0]
    try:
        return float(it.get("lastPrice"))
    except Exception:
        raise BybitAPIError(f"Bad lastPrice for {symbol}: {it.get('lastPrice')}")


async def get_last_hour_high_low(session: aiohttp.ClientSession, rate_limiter: RateLimiter, symbol: str) -> Tuple[float, float]:
    # 1m candles, last 60 minutes
    rows = await get_kline_rows(session, rate_limiter, symbol, interval="1", limit=60)
    if not rows:
        raise BybitAPIError(f"No 1m kline for {symbol}")
    hi = None
    lo = None
    for c in rows:
        if not isinstance(c, list) or len(c) < 5:
            continue
        try:
            h = float(c[2])
            l = float(c[3])
        except Exception:
            continue
        hi = h if hi is None else max(hi, h)
        lo = l if lo is None else min(lo, l)
    if hi is None or lo is None:
        raise BybitAPIError(f"Bad 1m kline rows for {symbol}")
    return hi, lo


async def get_stoch_rsi_values(
    session: aiohttp.ClientSession,
    rate_limiter: RateLimiter,
    symbol: str,
    sem: Optional[asyncio.Semaphore] = None,
) -> Dict[str, float]:
    async def one_tf(tf_label: str, interval: str) -> Tuple[str, float]:
        if sem is None:
            closes = await get_kline_closes(session, rate_limiter, symbol, interval, KLINE_LIMIT)
        else:
            async with sem:
                closes = await get_kline_closes(session, rate_limiter, symbol, interval, KLINE_LIMIT)
        value = stoch_rsi_from_closes(closes, RSI_PERIOD, STOCH_RSI_PERIOD)
        if value is None:
            raise BybitAPIError(
                f"Not enough data for Stoch RSI ({RSI_PERIOD},{STOCH_RSI_PERIOD}) {symbol} {tf_label}"
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


def format_stoch_rsi_line(values: Dict[str, float]) -> str:
    parts = []
    for label, _ in STOCH_RSI_TFS:
        value = values.get(label)
        if value is None:
            parts.append(f"{label} <code>n/a</code>")
        else:
            parts.append(f"{label} <code>{value:.2f}</code>")
    return "Stoch RSI: " + " | ".join(parts)


def levered_pct(delta: float, price: float, lev: float) -> float:
    """Calculate leveraged percentage change."""
    if price == 0:
        return 0.0
    return (delta / price) * 100.0 * lev


def safe_rr(reward: float, risk: float) -> Optional[float]:
    """Calculate risk/reward ratio safely."""
    if risk <= 0:
        return None
    return reward / risk


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


def format_table_lines(title: str, rows: List[Tuple[str, float, Dict[str, float]]]) -> List[str]:
    """Format table lines for rendering. Returns empty list if no rows."""
    if not rows:
        return []
    
    sym_w = 16
    num_w = 6

    header_cols = ["SUM"] + [lbl for (lbl, _) in TIMEFRAMES]
    header = f"{'SYMBOL'.ljust(sym_w)} " + " ".join(c.rjust(num_w) for c in header_cols)

    lines = [title, header, "-" * len(header)]
    for sym, s, rsis in rows:
        parts = [f"{s:>{num_w}.1f}"]
        for (lbl, _) in TIMEFRAMES:
            parts.append(f"{rsis[lbl]:>{num_w}.1f}")
        lines.append(f"{sym.ljust(sym_w)} " + " ".join(parts))
    return lines


def render_png(
    long_rows: List[Tuple[str, float, Dict[str, float]]],
    short_rows: List[Tuple[str, float, Dict[str, float]]],
    symbols_scanned: int,
) -> BytesIO:
    ts = datetime.now(LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S %Z")

    lines: List[str] = []
    lines += [
        "Bybit USDT Perpetuals — RSI(6) sum monitor",
        f"Timeframes: {', '.join(lbl for (lbl, _) in TIMEFRAMES)}",
        f"Generated: {ts}",
        f"Symbols scanned: {symbols_scanned} (top by turnover24h)",
        "",
    ]
    
    # Format LONG table
    long_lines = format_table_lines("TOP-10 LONG (min SUM RSI6)", long_rows)
    if long_lines:
        lines += long_lines
    else:
        lines += ["TOP-10 LONG (min SUM RSI6)", "No candidates found"]
    
    lines += ["", ""]
    
    # Format SHORT table
    short_lines = format_table_lines("TOP-10 SHORT (max SUM RSI6)", short_rows)
    if short_lines:
        lines += short_lines
    else:
        lines += ["TOP-10 SHORT (max SUM RSI6)", "No candidates found"]

    font = load_monospace_font(FONT_SIZE)

    dummy = Image.new("RGB", (10, 10), (255, 255, 255))
    d = ImageDraw.Draw(dummy)

    max_w = 0
    line_h = 0
    for line in lines:
        bbox = d.textbbox((0, 0), line, font=font)
        w = bbox[2] - bbox[0]
        h = bbox[3] - bbox[1]
        max_w = max(max_w, w)
        line_h = max(line_h, h)

    total_h = IMAGE_PADDING * 2 + len(lines) * (line_h + LINE_SPACING) - LINE_SPACING
    total_w = IMAGE_PADDING * 2 + max_w

    img = Image.new("RGB", (total_w, total_h), (255, 255, 255))
    d = ImageDraw.Draw(img)

    y = IMAGE_PADDING
    for line in lines:
        d.text((IMAGE_PADDING, y), line, fill=(0, 0, 0), font=font)
        y += line_h + LINE_SPACING

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
            InlineKeyboardButton("120 мин", callback_data="SETINT:120"),
            InlineKeyboardButton("240 мин", callback_data="SETINT:240"),
        ],
        [InlineKeyboardButton("⬅️ Назад", callback_data="MENU")],
    ]
    return InlineKeyboardMarkup(rows)


def alerts_kb(symbol: str, side: str) -> InlineKeyboardMarkup:
    # 3 кнопки, как просили
    return InlineKeyboardMarkup(
        [[
            InlineKeyboardButton("Alert 5m", callback_data=f"ALERT|5|{side}|{symbol}"),
            InlineKeyboardButton("Alert 15m", callback_data=f"ALERT|15|{side}|{symbol}"),
            InlineKeyboardButton("Alert 30m", callback_data=f"ALERT|30|{side}|{symbol}"),
        ]]
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
        save_state_json(json.dumps(state, ensure_ascii=False, indent=2))


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
        save_state_json(json.dumps(state, ensure_ascii=False, indent=2))


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
        save_state_json(json.dumps(state, ensure_ascii=False, indent=2))
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
        save_state_json(json.dumps(state, ensure_ascii=False, indent=2))
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
            save_state_json(json.dumps(state, ensure_ascii=False, indent=2))


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
            save_state_json(json.dumps(state, ensure_ascii=False, indent=2))


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


def prune_expired_alerts(state: dict, now: Optional[float] = None) -> bool:
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
        changed = prune_expired_alerts(state, now=now)
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
        save_state_json(json.dumps(state, ensure_ascii=False, indent=2))
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
            save_state_json(json.dumps(state, ensure_ascii=False, indent=2))


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
            save_state_json(json.dumps(state, ensure_ascii=False, indent=2))


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
            save_state_json(json.dumps(state, ensure_ascii=False, indent=2))


# =========================
# CACHES
# =========================
async def get_cached_perp_symbols(app: Application) -> List[str]:
    cache_lock: asyncio.Lock = app.bot_data["perp_symbols_lock"]
    async with cache_lock:
        now = time.time()
        cached = app.bot_data.get("perp_symbols_cache")
        if cached and (now - cached.ts) <= PERP_SYMBOLS_CACHE_TTL:
            return cached.value  # type: ignore

        session: aiohttp.ClientSession = app.bot_data["http_session"]
        rate_limiter: RateLimiter = app.bot_data["rate_limiter"]
        symbols = await get_all_usdt_linear_perp_symbols(session, rate_limiter)
        app.bot_data["perp_symbols_cache"] = CacheItem(ts=now, value=symbols)
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
            top = await pick_top_symbols_by_turnover(session, rate_limiter, set(perp_symbols), MAX_SYMBOLS)
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

    app.job_queue.run_repeating(
        alert_job_callback,
        interval=ALERT_CHECK_SEC,
        first=2,
        chat_id=chat_id,
        name=name,
        data={"symbol": symbol, "tf": tf, "tf_label": tf_label, "side": side},
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
        changed = prune_expired_alerts(state)
        alerts_all = copy.deepcopy(state.get("alerts", {}) or {})
        if changed:
            app.bot_data["state"] = state
            save_state_json(json.dumps(state, ensure_ascii=False, indent=2))

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
                if symbol and tf and side:
                    await schedule_alert(app, chat_id, symbol, tf, tf_label, side)
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


async def send_scan_result(app: Application, chat_id: int, long_rows, short_rows, symbols_scanned: int):
    long_syms = [r[0] for r in long_rows]
    short_syms = [r[0] for r in short_rows]

    png = render_png(long_rows, short_rows, symbols_scanned=symbols_scanned)
    await app.bot.send_photo(chat_id=chat_id, photo=png)

    text = build_pairs_text(long_syms, short_syms)
    kb = pairs_keyboard(long_syms, short_syms)
    await app.bot.send_message(chat_id=chat_id, text=text, reply_markup=kb)


async def run_monitor_once_internal(app: Application, chat_id: int):
    """Internal monitor function without timeout wrapper."""
    session: aiohttp.ClientSession = app.bot_data["http_session"]
    rate_limiter: RateLimiter = app.bot_data["rate_limiter"]
    symbols = await get_cached_top_symbols(app)
    sem: asyncio.Semaphore = app.bot_data["http_sem"]

    results: List[Tuple[str, float, Dict[str, float]]] = []
    queue: asyncio.Queue[str] = asyncio.Queue()
    for symbol in symbols:
        queue.put_nowait(symbol)

    # Track task statistics
    total_tasks = len(symbols)
    success_count = 0
    error_count = 0

    async def worker() -> Tuple[List[Tuple[str, float, Dict[str, float]]], int, int]:
        local_results: List[Tuple[str, float, Dict[str, float]]] = []
        local_success = 0
        local_error = 0
        while True:
            try:
                symbol = queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            try:
                r = await compute_symbol_rsi_sum(session, rate_limiter, sem, symbol)
                if r is not None:
                    local_results.append(r)
                    local_success += 1
                else:
                    local_error += 1
            except asyncio.CancelledError:
                logging.warning("Task cancelled during monitor scan")
                local_error += 1
                raise
            except Exception as e:
                logging.warning("Failed to compute RSI for %s: %s", symbol, e)
                local_error += 1
            finally:
                queue.task_done()
        return local_results, local_success, local_error

    worker_count = min(MAX_CONCURRENCY, total_tasks) if total_tasks else 0
    tasks = [asyncio.create_task(worker()) for _ in range(worker_count)]

    try:
        for local_results, local_success, local_error in await asyncio.gather(*tasks):
            results.extend(local_results)
            success_count += local_success
            error_count += local_error
    finally:
        for task in tasks:
            task.cancel()
        if tasks:
            gather_task = asyncio.gather(*tasks, return_exceptions=True)
            try:
                await asyncio.shield(gather_task)
            except asyncio.CancelledError:
                await gather_task
                raise

    # Calculate error rate
    error_rate = error_count / total_tasks if total_tasks > 0 else 0
    
    # Notify user if error rate is high
    if error_rate > ERROR_RATE_THRESHOLD:
        await app.bot.send_message(
            chat_id=chat_id,
            text=f"⚠️ Внимание: {error_count}/{total_tasks} символов не удалось обработать ({error_rate*100:.1f}% ошибок). "
                 f"Возможны проблемы с API или rate limiting."
        )

    if not results:
        await app.bot.send_message(chat_id=chat_id, text="Не получилось собрать RSI (rate limit/временная ошибка).")
        return

    long_candidates = [r for r in results if is_long_candidate(r[2])]
    short_candidates = [r for r in results if is_short_candidate(r[2])]

    long_candidates.sort(key=lambda x: x[1])
    short_candidates.sort(key=lambda x: x[1], reverse=True)

    long_rows = long_candidates[:10]
    short_rows = short_candidates[:10]

    await send_scan_result(app, chat_id, long_rows, short_rows, symbols_scanned=total_tasks)


async def run_monitor_once(app: Application, chat_id: int):
    """Run monitor with timeout protection."""
    monitor_tasks: Set[asyncio.Task] = app.bot_data.setdefault("monitor_tasks", set())
    current_task = asyncio.current_task()
    if current_task:
        monitor_tasks.add(current_task)
    try:
        rate_limiter: RateLimiter = app.bot_data["rate_limiter"]
        current_delay = await rate_limiter.get_current_delay()
        delay_factor = max(1.0, current_delay / RATE_LIMIT_DELAY)
        adaptive_timeout = int(MONITOR_TIMEOUT * delay_factor)
        await asyncio.wait_for(run_monitor_once_internal(app, chat_id), timeout=adaptive_timeout)
    except asyncio.TimeoutError:
        logging.error("Monitor timeout for chat_id=%s after %ds", chat_id, adaptive_timeout)
        await app.bot.send_message(
            chat_id=chat_id,
            text=f"⚠️ Превышено время ожидания ({adaptive_timeout}с). Попробуйте позже."
        )
    finally:
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
    chat_id = context.job.chat_id
    data = context.job.data or {}
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
    key = f"{symbol}|{tf}"
    alerts_map = await get_alerts_for_chat(app, chat_id)
    entry = alerts_map.get(key)
    if not entry:
        unschedule_alert(app, chat_id, symbol, tf)
        return

    created_at = entry.get("created_at")
    if created_at is None:
        await update_alert_created_at(app, chat_id, symbol, tf, time.time())
    elif is_alert_expired(created_at):
        unschedule_alert(app, chat_id, symbol, tf)
        await delete_alert_state(app, chat_id, symbol, tf, validate_symbol_check=False)
        return
    
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
            await update_alert_last_above(app, chat_id, symbol, tf, condition_met)
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
                await delete_alert_state(app, chat_id, symbol, tf)
            return

        triggered = (not prev) and condition_met
        if triggered or near:
            extra = f"hit {trigger_label}" if triggered else f"hit ≈ {threshold:.0f}"
            await app.bot.send_message(
                chat_id=chat_id,
                text=(
                    f"🔔 ALERT {symbol} {direction_label} RSI{RSI_PERIOD}({tf_label}) {extra}\n"
                    f"Current RSI: {rsi:.2f}"
                ),
            )
            unschedule_alert(app, chat_id, symbol, tf)
            await delete_alert_state(app, chat_id, symbol, tf)

        await update_alert_last_above(app, chat_id, symbol, tf, condition_met)

    except Exception as e:
        logging.exception("alert job failed for %s: %s", symbol, e)
        notify_key = (chat_id, symbol, tf)
        notify_cache = app.bot_data.setdefault("alert_error_notify", {})
        now = time.time()
        ttl_sec = ALERT_ERROR_NOTIFY_TTL_HOURS * 3600
        if ttl_sec > 0 and notify_cache:
            for key, last_seen in list(notify_cache.items()):
                if now - last_seen >= ttl_sec:
                    del notify_cache[key]
        last_sent = notify_cache.get(notify_key, 0.0)
        throttle_sec = ALERT_ERROR_THROTTLE_MIN * 60
        if now - last_sent >= throttle_sec:
            try:
                await app.bot.send_message(
                    chat_id=chat_id,
                    text=f"Alert {symbol}({tf_label}) временно не проверяется из-за ошибки API.",
                )
                notify_cache[notify_key] = now
            except Exception:
                logging.error("Failed to send alert error message to chat %s", chat_id)


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
    chat_id = update.effective_chat.id
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


async def on_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
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
        except Exception as e:
            logging.exception("PAIR INFO calc failed: %s", e)
            await update.message.reply_text(f"Ошибка расчёта {symbol}: {e}")
            return

        rsi_line = format_rsi_line(rsi_values)
        stoch_line = format_stoch_rsi_line(stoch_values)
        msg = f"<b>{symbol}</b>\n{rsi_line}\n{stoch_line}"
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
        await app.bot.send_message(chat_id=chat_id, text="Собираю данные…")
        try:
            await run_monitor_once(app, chat_id)
        except Exception as e:
            logging.exception("RUN_NOW failed: %s", e)
            await app.bot.send_message(chat_id=chat_id, text=f"Ошибка: {e}")
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
                await app.bot.send_message(chat_id=chat_id, text="Некорректное направление.")
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

        await app.bot.send_message(chat_id=chat_id, text=f"Считаю {symbol} (last 1h)…")

        try:
            price = await get_last_price(session, rate_limiter, symbol)
            hi, lo = await get_last_hour_high_low(session, rate_limiter, symbol)
            try:
                sem: asyncio.Semaphore = app.bot_data["http_sem"]
                stoch_values = await get_stoch_rsi_values(session, rate_limiter, symbol, sem)
            except Exception as e:
                logging.warning("Failed to compute Stoch RSI for %s: %s", symbol, e)
                stoch_values = {}
            stoch_line = format_stoch_rsi_line(stoch_values)

            if side == "L":
                # LONG: risk is current to low, reward is current to high
                dd = levered_pct(lo - price, price, LEVERAGE)     # negative (to low)
                up = levered_pct(hi - price, price, LEVERAGE)     # positive (to high)
                risk = (price - lo)  # distance to stop loss
                reward = (hi - price)  # distance to take profit
                rr = safe_rr(reward, risk)

                msg = (
                    f"<b>{symbol} — LONG</b>\n"
                    f"Current: <code>{price:.6f}</code>\n"
                    f"1h Low:  <code>{lo:.6f}</code>\n"
                    f"1h High: <code>{hi:.6f}</code>\n\n"
                    f"{stoch_line}\n\n"
                    f"To 1h low ({LEVERAGE:.0f}x): <b>{dd:.2f}%</b>\n"
                    f"To 1h high ({LEVERAGE:.0f}x): <b>{up:.2f}%</b>\n"
                )
                if rr is None:
                    msg += "Risk/Reward: <b>∞</b>\n"
                else:
                    msg += f"Risk/Reward: <b>{rr:.2f}</b>\n"

            else:
                # SHORT: entry at current price
                # If price goes UP to 1h high → loss (risk)
                # If price goes DOWN to 1h low → profit (reward)
                risk = (hi - price)  # distance to stop loss (upward movement)
                reward = (price - lo)  # distance to take profit (downward movement)
                
                # Calculate percentages for display
                loss_pct = abs(levered_pct(hi - price, price, LEVERAGE))  # show as positive number
                profit_pct = abs(levered_pct(price - lo, price, LEVERAGE))  # show as positive number
                
                rr = safe_rr(reward, risk)

                msg = (
                    f"<b>{symbol} — SHORT</b>\n"
                    f"Current: <code>{price:.6f}</code>\n"
                    f"1h High: <code>{hi:.6f}</code>\n"
                    f"1h Low:  <code>{lo:.6f}</code>\n\n"
                    f"{stoch_line}\n\n"
                    f"<b>If price rises to 1h high:</b> <b>-{loss_pct:.2f}%</b> (LOSS at {LEVERAGE:.0f}x)\n"
                    f"<b>If price falls to 1h low:</b> <b>+{profit_pct:.2f}%</b> (PROFIT at {LEVERAGE:.0f}x)\n"
                )
                if rr is None:
                    msg += "Risk/Reward: <b>∞</b>\n"
                else:
                    msg += f"Risk/Reward: <b>{rr:.2f}</b>\n"

            await app.bot.send_message(
                chat_id=chat_id,
                text=msg,
                parse_mode="HTML",
                reply_markup=alerts_kb(symbol, side),
            )

        except Exception as e:
            logging.exception("PAIR calc failed: %s", e)
            await app.bot.send_message(chat_id=chat_id, text=f"Ошибка расчёта {symbol}: {e}")

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
            now = time.time()
            last_ts = cooldowns.get(chat_id, 0.0)
            remaining = ALERT_COOLDOWN_SEC - (now - last_ts)
            if remaining > 0:
                await app.bot.send_message(
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
            await app.bot.send_message(chat_id=chat_id, text=err or "Не удалось создать алерт.")
            return
        if ALERT_COOLDOWN_SEC > 0:
            cooldowns = app.bot_data.setdefault("alert_cooldowns", {})
            cooldowns[chat_id] = time.time()
        await schedule_alert(app, chat_id, symbol, tf, tf_label, side, valid_symbols)

        await app.bot.send_message(
            chat_id=chat_id,
            text=(
                f"✅ Alert включён: {symbol} {direction_label} RSI{RSI_PERIOD}({tf_label}) {trigger_label} "
                f"(проверка каждые {ALERT_CHECK_SEC//60} мин)"
            ),
        )
        return

    # Unknown
    await app.bot.send_message(chat_id=chat_id, text="Неизвестная кнопка. Открой меню: /start")


# =========================
# INIT / SHUTDOWN
# =========================
async def post_init(app: Application):
    app.bot_data["http_session"] = aiohttp.ClientSession()
    app.bot_data["rate_limiter"] = RateLimiter()
    app.bot_data["state_lock"] = asyncio.Lock()
    app.bot_data["perp_symbols_lock"] = asyncio.Lock()
    app.bot_data["tickers_lock"] = asyncio.Lock()
    app.bot_data["http_sem"] = asyncio.Semaphore(MAX_CONCURRENCY)
    app.bot_data["monitor_tasks"] = set()
    app.bot_data["state"] = load_state()
    app.bot_data["state"].setdefault("pair_rsi_alerts", {})
    app.bot_data["state"].setdefault("price_alerts", {})
    app.bot_data["perp_symbols_cache"] = None
    app.bot_data["tickers_cache"] = None
    app.bot_data["alert_cooldowns"] = {}

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


async def post_shutdown(app: Application):
    monitor_tasks: Set[asyncio.Task] = app.bot_data.get("monitor_tasks", set())
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

    sess: aiohttp.ClientSession = app.bot_data.get("http_session")
    if sess:
        await sess.close()
    save_state(
        app.bot_data.get(
            "state",
            {"subs": {}, "alerts": {}, "pair_rsi_alerts": {}, "price_alerts": {}},
        )
    )


# =========================
# MAIN
# =========================
def main():
    validate_config()

    app = (
        ApplicationBuilder()
        .token(TELEGRAM_BOT_TOKEN)
        .post_init(post_init)
        .post_shutdown(post_shutdown)
        .build()
    )

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CallbackQueryHandler(on_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))

    logging.info("Bot started")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
