#!/usr/bin/env python3
import asyncio
import contextlib
import json
import logging
import os
import time
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
LEVERAGE = float(os.getenv("LEVERAGE", "20"))

# RSI timeframes for SUM (now includes 5m)
TIMEFRAMES: List[Tuple[str, str]] = [
    ("5m", "5"),
    ("15m", "15"),
    ("30m", "30"),
    ("1h", "60"),
    ("2h", "120"),
    ("4h", "240"),
]

# Alert options (fixed to 5m/15m/30m)
ALERT_TFS: List[Tuple[str, str]] = [
    ("5m", "5"),
    ("15m", "15"),
    ("30m", "30"),
]
ALERT_INTERVALS = {iv for _, iv in ALERT_TFS}

ALERT_CHECK_SEC = int(os.getenv("ALERT_CHECK_SEC", "300"))  # check every 5 minutes by default
ALERT_EPS = float(os.getenv("ALERT_EPS", "0.10"))  # "equals threshold" tolerance
ALERT_LONG_THRESHOLD = float(os.getenv("ALERT_LONG_THRESHOLD", "40"))
ALERT_SHORT_THRESHOLD = float(os.getenv("ALERT_SHORT_THRESHOLD", "60"))

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

# Timeout for monitor scanning (seconds)
MONITOR_TIMEOUT = 900  # 15 minutes (increased from 10)

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
    if MAX_SYMBOLS <= 0:
        errors.append("MAX_SYMBOLS must be > 0")
    if MAX_CONCURRENCY <= 0:
        errors.append("MAX_CONCURRENCY must be > 0")
    if KLINE_LIMIT <= 0:
        errors.append("KLINE_LIMIT must be > 0")
    if RATE_LIMIT_DELAY <= 0:
        errors.append("RATE_LIMIT_DELAY must be > 0")
    if RATE_LIMIT_MAX_DELAY < RATE_LIMIT_DELAY:
        errors.append("RATE_LIMIT_MAX_DELAY must be >= RATE_LIMIT_DELAY")
    if RATE_LIMIT_QUEUE_TIMEOUT <= 0:
        errors.append("RATE_LIMIT_QUEUE_TIMEOUT must be > 0")
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
#               "last_above": true/false/null
#           }
#       }
#   }
# }
def load_state() -> dict:
    try:
        if os.path.exists(STATE_FILE):
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception as e:
        logging.warning("Failed to load state: %s", e)
    return {"subs": {}, "alerts": {}}


def save_state(state: dict) -> None:
    try:
        tmp = STATE_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
        os.replace(tmp, STATE_FILE)
    except Exception as e:
        logging.warning("Failed to save state: %s", e)


def get_sub_job_name(chat_id: int) -> str:
    return f"rsi_sub:{chat_id}"


def get_alert_job_name(chat_id: int, symbol: str, tf: str) -> str:
    return f"rsi_alert:{chat_id}:{symbol}:{tf}"


# =========================
# INPUT VALIDATION
# =========================
def validate_chat_id(chat_id: int) -> bool:
    """Validate chat_id to prevent injection attacks."""
    return isinstance(chat_id, int) and abs(chat_id) < 10**15


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
    
    async def acquire(self):
        """Acquire permission to make a request with timeout."""
        try:
            async with asyncio.timeout(RATE_LIMIT_QUEUE_TIMEOUT):
                async with self._lock:
                    now = time.time()
                    if self._rate_limited:
                        wait_time = self._delay - (now - self._last_request)
                        if wait_time > 0:
                            await asyncio.sleep(wait_time)
                    self._last_request = time.time()
        except asyncio.TimeoutError:
            raise BybitAPIError(f"Rate limiter queue timeout after {RATE_LIMIT_QUEUE_TIMEOUT}s")
    
    async def report_rate_limit(self):
        """Report that a rate limit was hit."""
        async with self._lock:
            self._rate_limited = True
            self._delay = min(self._delay * 2, RATE_LIMIT_MAX_DELAY)
            logging.warning("Rate limit hit, increasing delay to %.2fs", self._delay)
    
    async def report_success(self):
        """Report a successful request."""
        async with self._lock:
            if self._rate_limited:
                # Gradually decrease delay on success
                self._delay = max(self._delay * 0.8, RATE_LIMIT_DELAY)
                if self._delay <= RATE_LIMIT_DELAY:
                    self._rate_limited = False
                    logging.info("Rate limiter reset")


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
                data = await resp.json(content_type=None)

                # Common rate limit code
                if isinstance(data, dict) and data.get("retCode") == 10006:
                    await rate_limiter.report_rate_limit()
                    raise BybitAPIError("Bybit rate limit: retCode=10006 (Too many visits!)")

                if resp.status >= 400:
                    raise BybitAPIError(f"HTTP {resp.status}: {data}")

                if isinstance(data, dict) and data.get("retCode") not in (0, None):
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
            closes.append(float(c[4]))
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
        state = app.bot_data.get("state") or {"subs": {}, "alerts": {}}
        subs = state.get("subs", {})
        return subs.get(str(chat_id))


async def set_sub(app: Application, chat_id: int, interval_min: int, enabled: bool = True) -> None:
    if not validate_chat_id(chat_id) or not validate_interval(interval_min):
        logging.warning("Invalid chat_id or interval: %s, %s", chat_id, interval_min)
        return
    
    state_lock: asyncio.Lock = app.bot_data["state_lock"]
    async with state_lock:
        state = app.bot_data.get("state") or {"subs": {}, "alerts": {}}
        state.setdefault("subs", {})
        state["subs"][str(chat_id)] = {"interval_min": int(interval_min), "enabled": bool(enabled)}
        app.bot_data["state"] = state
        save_state(state)


async def delete_sub(app: Application, chat_id: int) -> None:
    if not validate_chat_id(chat_id):
        return
    
    state_lock: asyncio.Lock = app.bot_data["state_lock"]
    async with state_lock:
        state = app.bot_data.get("state") or {"subs": {}, "alerts": {}}
        subs = state.setdefault("subs", {})
        subs.pop(str(chat_id), None)
        save_state(state)


async def get_alerts_for_chat(app: Application, chat_id: int) -> Dict[str, dict]:
    """Get alerts for chat_id with thread-safe state access."""
    if not validate_chat_id(chat_id):
        return {}
    state_lock: asyncio.Lock = app.bot_data["state_lock"]
    async with state_lock:
        state = app.bot_data.get("state") or {"subs": {}, "alerts": {}}
        alerts = state.get("alerts", {})
        return alerts.get(str(chat_id), {})


async def set_alert_state(
    app: Application,
    chat_id: int,
    symbol: str,
    tf: str,
    tf_label: str,
    side: str,
    last_above: Optional[bool] = None,
) -> None:
    if not validate_chat_id(chat_id):
        logging.warning("Invalid chat_id for alert: %s", chat_id)
        return
    
    # Get valid symbols for validation
    valid_symbols = None
    perp_cache = app.bot_data.get("perp_symbols_cache")
    if perp_cache:
        valid_symbols = set(perp_cache.value)
    
    if not validate_symbol(symbol, valid_symbols):
        logging.warning("Invalid symbol for alert: %s", symbol)
        return
    
    state_lock: asyncio.Lock = app.bot_data["state_lock"]
    async with state_lock:
        state = app.bot_data.get("state") or {"subs": {}, "alerts": {}}
        alerts = state.setdefault("alerts", {}).setdefault(str(chat_id), {})
        key = f"{symbol}|{tf}"
        alerts[key] = {
            "symbol": symbol,
            "tf": tf,
            "tf_label": tf_label,
            "side": side,
            "last_above": last_above,  # can be None
        }
        app.bot_data["state"] = state
        save_state(state)


async def update_alert_last_above(app: Application, chat_id: int, symbol: str, tf: str, last_above: bool) -> None:
    if not validate_chat_id(chat_id):
        return
    
    state_lock: asyncio.Lock = app.bot_data["state_lock"]
    async with state_lock:
        state = app.bot_data.get("state") or {"subs": {}, "alerts": {}}
        alerts = state.setdefault("alerts", {}).setdefault(str(chat_id), {})
        key = f"{symbol}|{tf}"
        if key in alerts:
            alerts[key]["last_above"] = bool(last_above)
            app.bot_data["state"] = state
            save_state(state)


async def delete_alert_state(app: Application, chat_id: int, symbol: str, tf: str) -> None:
    if not validate_chat_id(chat_id):
        return

    valid_symbols = None
    perp_cache = app.bot_data.get("perp_symbols_cache")
    if perp_cache:
        valid_symbols = set(perp_cache.value)

    if not validate_symbol(symbol, valid_symbols):
        return

    state_lock: asyncio.Lock = app.bot_data["state_lock"]
    async with state_lock:
        state = app.bot_data.get("state") or {"subs": {}, "alerts": {}}
        alerts = state.setdefault("alerts", {}).setdefault(str(chat_id), {})
        key = f"{symbol}|{tf}"
        if key in alerts:
            alerts.pop(key, None)
            app.bot_data["state"] = state
            save_state(state)


# =========================
# CACHES
# =========================
async def get_cached_perp_symbols(app: Application) -> List[str]:
    now = time.time()
    cached: Optional[CacheItem] = app.bot_data.get("perp_symbols_cache")
    if cached and (now - cached.ts) <= PERP_SYMBOLS_CACHE_TTL:
        return cached.value  # type: ignore

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
    now = time.time()
    cached: Optional[CacheItem] = app.bot_data.get("tickers_cache")
    if cached and (now - cached.ts) <= TICKERS_CACHE_TTL:
        return cached.value  # type: ignore

    cache_lock: asyncio.Lock = app.bot_data["tickers_lock"]
    async with cache_lock:
        now = time.time()
        cached = app.bot_data.get("tickers_cache")
        if cached and (now - cached.ts) <= TICKERS_CACHE_TTL:
            return cached.value  # type: ignore

        session: aiohttp.ClientSession = app.bot_data["http_session"]
        rate_limiter: RateLimiter = app.bot_data["rate_limiter"]
        perp_symbols = await get_cached_perp_symbols(app)
        top = await pick_top_symbols_by_turnover(session, rate_limiter, set(perp_symbols), MAX_SYMBOLS)
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


def schedule_alert(app: Application, chat_id: int, symbol: str, tf: str, tf_label: str, side: str) -> None:
    if not validate_chat_id(chat_id):
        logging.warning("Invalid chat_id for alert: %s", chat_id)
        return
    
    # Get valid symbols for validation
    valid_symbols = None
    perp_cache = app.bot_data.get("perp_symbols_cache")
    if perp_cache:
        valid_symbols = set(perp_cache.value)
    
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


def restore_alerts(app: Application) -> None:
    state = app.bot_data.get("state") or {"subs": {}, "alerts": {}}
    alerts_all = state.get("alerts", {}) or {}
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
                    schedule_alert(app, chat_id, symbol, tf, tf_label, side)
            except Exception as e:
                logging.warning("Failed restoring alert: %s", e)


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
    sem = asyncio.Semaphore(MAX_CONCURRENCY)

    results: List[Tuple[str, float, Dict[str, float]]] = []
    tasks = [asyncio.create_task(compute_symbol_rsi_sum(session, rate_limiter, sem, s)) for s in symbols]

    # Track task statistics
    total_tasks = len(tasks)
    success_count = 0
    error_count = 0

    try:
        for task in asyncio.as_completed(tasks):
            try:
                r = await task
                if r is not None:
                    results.append(r)
                    success_count += 1
                else:
                    error_count += 1
            except asyncio.CancelledError:
                logging.warning("Task cancelled during monitor scan")
                error_count += 1
                raise
            except Exception as e:
                logging.warning("Failed to compute RSI for a symbol: %s", e)
                error_count += 1
                continue
    finally:
        for task in tasks:
            task.cancel()
        if tasks:
            with contextlib.suppress(asyncio.CancelledError):
                await asyncio.shield(asyncio.gather(*tasks, return_exceptions=True))

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
        await asyncio.wait_for(run_monitor_once_internal(app, chat_id), timeout=MONITOR_TIMEOUT)
    except asyncio.TimeoutError:
        logging.error("Monitor timeout for chat_id=%s after %ds", chat_id, MONITOR_TIMEOUT)
        await app.bot.send_message(
            chat_id=chat_id,
            text=f"⚠️ Превышено время ожидания ({MONITOR_TIMEOUT}с). Попробуйте позже."
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
    
    # Validate symbol with whitelist
    app = context.application
    valid_symbols = None
    perp_cache = app.bot_data.get("perp_symbols_cache")
    if perp_cache:
        valid_symbols = set(perp_cache.value)
    
    if not validate_symbol(symbol, valid_symbols):
        logging.warning("Invalid symbol for alert: %s", symbol)
        return

    session: aiohttp.ClientSession = app.bot_data["http_session"]
    rate_limiter: RateLimiter = app.bot_data["rate_limiter"]

    try:
        closes = await get_kline_closes(session, rate_limiter, symbol, tf, KLINE_LIMIT)
        rsi = rsi_wilder(closes, RSI_PERIOD)
        if rsi is None:
            return

        alerts_map = await get_alerts_for_chat(app, chat_id)
        key = f"{symbol}|{tf}"
        prev = alerts_map.get(key, {}).get("last_above", None)

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
        # keep it quiet; optionally notify user:
        # await app.bot.send_message(chat_id=chat_id, text=f"Alert error {symbol}({tf_label}): {e}")


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
    )
    await update.message.reply_text(text, reply_markup=main_menu_kb(has_sub))


async def on_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    app = context.application

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
        await query.edit_message_text("Меню RSI-бота:", reply_markup=main_menu_kb(has_sub))
        return

    if data == "SUB_CREATE":
        context.user_data["await_interval"] = True
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
        await app.bot.send_message(chat_id=chat_id, text="Собираю данные…")
        try:
            await run_monitor_once(app, chat_id)
        except Exception as e:
            logging.exception("RUN_NOW failed: %s", e)
            await app.bot.send_message(chat_id=chat_id, text=f"Ошибка: {e}")
        return

    if data.startswith("SECTION|"):
        return

    # Click on symbol from list
    if data.startswith("PAIR|"):
        # format: PAIR|L|BTCUSDT  or PAIR|S|BTCUSDT
        try:
            _, side, symbol = data.split("|", 2)
            
            # Get valid symbols for validation
            valid_symbols = None
            perp_cache = app.bot_data.get("perp_symbols_cache")
            if perp_cache:
                valid_symbols = set(perp_cache.value)
            
            if not validate_symbol(symbol, valid_symbols):
                await app.bot.send_message(chat_id=chat_id, text="Некорректный символ.")
                return
        except Exception:
            return

        await app.bot.send_message(chat_id=chat_id, text=f"Считаю {symbol} (last 1h)…")

        try:
            price = await get_last_price(session, rate_limiter, symbol)
            hi, lo = await get_last_hour_high_low(session, rate_limiter, symbol)

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
            
            # Get valid symbols for validation
            valid_symbols = None
            perp_cache = app.bot_data.get("perp_symbols_cache")
            if perp_cache:
                valid_symbols = set(perp_cache.value)
            
            if not validate_symbol(symbol, valid_symbols):
                await app.bot.send_message(chat_id=chat_id, text="Некорректный символ.")
                return
        except Exception:
            return

        tf_label = next((lbl for (lbl, iv) in ALERT_TFS if iv == tf), f"{tf}m")
        direction_label = "LONG" if side == "L" else "SHORT"
        threshold = ALERT_LONG_THRESHOLD if side == "L" else ALERT_SHORT_THRESHOLD
        trigger_label = f">= {threshold:.0f}" if side == "L" else f"<= {threshold:.0f}"

        # Save alert with last_above = None (first check will set)
        await set_alert_state(app, chat_id, symbol, tf, tf_label, side, last_above=None)
        schedule_alert(app, chat_id, symbol, tf, tf_label, side)

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
    app.bot_data["monitor_tasks"] = set()
    app.bot_data["state"] = load_state()
    app.bot_data["perp_symbols_cache"] = None
    app.bot_data["tickers_cache"] = None

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
    restore_alerts(app)
    logging.info("Restored alerts")


async def post_shutdown(app: Application):
    monitor_tasks: Set[asyncio.Task] = app.bot_data.get("monitor_tasks", set())
    for task in list(monitor_tasks):
        if not task.done():
            task.cancel()
    if monitor_tasks:
        await asyncio.gather(*monitor_tasks, return_exceptions=True)

    sess: aiohttp.ClientSession = app.bot_data.get("http_session")
    if sess:
        await sess.close()
    save_state(app.bot_data.get("state", {"subs": {}, "alerts": {}}))


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
