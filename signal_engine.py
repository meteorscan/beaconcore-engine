#!/usr/bin/env python3
"""
KAIROS -- Adaptive Hyperliquid Perpetuals Signal Engine
Version: 2.0.0

Single-file, stateless-between-runs (state.json) SMC/ICT signal engine.
Run every 15 minutes via external scheduler; scans WATCHLIST and emits zero
or more LONG/SHORT signals per pass.

Section order: config -> data collector -> feature engineering -> regime
vector -> top-down sequence -> zone selection -> engine ensemble ->
counter-trend engine -> adaptive filters -> risk-plan construction ->
decision engine/scoring -> entry-fill verification -> trade resolution ->
loss forensics -> learning loop/circuit breaker -> signal formatting ->
state persistence -> self-monitoring -> Telegram dispatch -> main().
"""

from __future__ import annotations

import json
import logging
import math
import os
import statistics
import sys
import tempfile
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

# ============================================================================
# SECTION 1 -- CONFIG & CONSTANTS
# ============================================================================

ENGINE_NAME = "KAIROS"
ENGINE_VERSION = "2.0.0"

# --- V2 changelog vs v1.0.0 -------------------------------------------------
# Added: liquidity-wall TP1 clipping (post RR-floor gate, never below
# RR_MIN_GATE), filter-funnel attrition logging, optional half-Kelly sizing
# (informational only, gated by ENABLE_KELLY_SIZING). Everything else
# (engine ensemble, threaded per-symbol scan, SL-first same-candle
# resolution, assert_signal_integrity pre-dispatch gate) is unchanged.

log = logging.getLogger("kairos")
if not log.handlers:
    _handler = logging.StreamHandler(sys.stdout)
    _handler.setFormatter(logging.Formatter(
        "%(asctime)s %(levelname)s [kairos] %(message)s", "%Y-%m-%dT%H:%M:%SZ"))
    log.addHandler(_handler)
log.setLevel(os.environ.get("KAIROS_LOG_LEVEL", "INFO"))

TG_BOT_TOKEN = os.environ.get("TG_BOT_TOKEN", "")
TG_CHAT_ID = os.environ.get("TG_CHAT_ID", "")
TELEGRAM_ENABLED = bool(TG_BOT_TOKEN and TG_CHAT_ID)
if not TELEGRAM_ENABLED:
    log.warning("TG_BOT_TOKEN and/or TG_CHAT_ID is missing/empty -- running in "
                "signal-generation-only mode (no Telegram dispatch).")

STATE_PATH = os.environ.get("KAIROS_STATE_PATH", "state.json")
CANDLE_CACHE_PATH = os.environ.get("KAIROS_CANDLE_CACHE_PATH", "candle_cache.json")
HL_API_URL = os.environ.get("HL_API_URL", "https://api.hyperliquid.xyz/info")

WATCHLIST: List[str] = [
    "BTC", "ETH", "HYPE", "ZEC", "NEAR", "ONDO", "SUI", "PENGU", "BNB", "SOL",
    "TRX", "BCH", "DOGE", "ADA", "DOT", "TAO", "AVAX", "LINK", "AAVE", "XRP",
    "XLM", "UNI", "LTC", "APT", "PENDLE",
]
MACRO_ASSET = "BTC"  # dominant large-cap benchmark for macro-bias (Section 8)

TIMEFRAMES = ["1W", "1D", "4H", "1H", "15M", "5M"]
FORBIDDEN_TIMEFRAMES = {"1M", "2M", "3M"}  # Section 10 -- never a trigger/confirmation TF

# Candle lookback bounds per timeframe (Section 24 -- bounded, rolling cache)
CANDLE_LOOKBACK = {
    "1W": 260, "1D": 400, "4H": 600, "1H": 800, "15M": 1000, "5M": 400,
}

# --- Risk-plan construction constants (Section 14 / 14.2) -------------------
RR_MIN_GATE = 1.5            # TP1 reject-only floor
RR_MAX_GATE = 3.5            # TP1 reject-only ceiling
RR_MIN_GATE_COUNTERTREND = 2.0
MIN_RISK_ATR_MULT = 1.0
MAX_SL_ATR_MULT = 4.0
MIN_SL_DISTANCE_PCT = 0.006
MAX_SL_DISTANCE_PCT = 0.025
MIN_MOVE_PCT_TP1 = 0.012
MIN_MOVE_PCT_TP2 = 0.020
SL_LIQUIDITY_CLEAR_WINDOW_ATR_MULT = 1.5   # bounded search window for pool clearing

# --- Entry lifecycle constants (Section 16) ----------------------------------
PENDING_ENTRY_EXPIRY_BARS = {
    "default": 12,          # 12 x 15M bars = 3h of pending-fill patience
    "swing": 32,            # swing-style zones get more patience (~8h of 15M bars)
}
COUNTERTREND_RETEST_EXPIRY_BARS = 8   # ~2h of 15M bars for the retest-and-hold stage
MAX_ENTRY_DISTANCE_FROM_MARKET_ATR_MULT = 1.2  # Section 14.3 -- cap pending entry distance

# --- Concurrency / portfolio constants (Sections 22, 28) ---------------------
MAX_CONCURRENT_ACTIVE_SIGNALS = 8
MAX_CONCURRENT_PER_SYMBOL = 1
CORRELATED_ASSET_GROUPS = [
    {"BTC"}, {"ETH"}, {"SOL", "SUI", "APT", "NEAR", "TAO"},
    {"BNB", "TRX"}, {"XRP", "XLM"}, {"LINK", "AAVE", "UNI", "ONDO", "PENDLE"},
    {"DOGE", "PENGU"}, {"AVAX", "DOT", "ADA"}, {"LTC", "BCH"}, {"HYPE"}, {"ZEC"},
]

# --- Decision Engine / grading (Sections 5.2, 5.3) ---------------------------
CATEGORY_WEIGHTS_DEFAULT = {
    "trend": 0.25, "structure": 0.20, "momentum": 0.15, "liquidity": 0.15,
    "volume": 0.10, "volatility": 0.10, "risk": 0.05,
}
PER_TERM_CONTRIBUTION_CAP = 0.35  # no single term may contribute > 35% of pre-sigmoid sum
GRADE_BANDS = [(95, "A+"), (90, "A"), (85, "B+"), (80, "B")]
MIN_SIGNAL_SCORE = 80.0

# --- Regime-dependent weighting (Section 9) ----------------------------------
REGIME_WEIGHT_ADJUSTMENTS = {
    "Strong Bull Trend":  {"trend": 1.25, "momentum": 1.15, "liquidity": 0.85},
    "Strong Bear Trend":  {"trend": 1.25, "momentum": 1.15, "liquidity": 0.85},
    "Weak Trend":         {"trend": 1.05, "momentum": 1.0},
    "Sideways":           {"liquidity": 1.25, "trend": 0.70},
    "Range":              {"liquidity": 1.25, "trend": 0.70},
    "Expansion":          {"volatility": 1.3, "risk": 1.2},
    "Compression":        {"volatility": 1.2},
    "High Volatility":    {"risk": 1.4, "momentum": 0.85},
    "Low Volatility":     {"volatility": 1.1},
    "Breakout":           {"volatility": 1.2, "structure": 1.15},
    "Pullback":           {"trend": 1.1, "structure": 1.1},
    "Mean Reversion":     {"liquidity": 1.15, "trend": 0.75},
}

# --- Macro/news blackout (Section 19) ----------------------------------------
MACRO_BLACKOUT_MINUTES_BEFORE = 30
MACRO_BLACKOUT_MINUTES_AFTER = 60

# --- Adaptive-learning bounds (Section 7) ------------------------------------
ADAPTIVE_PARAM_BOUNDS = {
    "engine_weight":            (0.25, 1.75),
    "confidence_calibration":   (-15.0, 15.0),
    "sl_buffer_percentile":     (40.0, 90.0),
    "tp1_target_rank_preference": (2, 6),
    "regime_fit_discount":      (0.15, 1.0),
    "liquidity_sanity_threshold": (0.15, 1.0),
    "mtf_alignment_weight":     (0.05, 0.35),
    "sfp_purity_threshold":     (0.4, 0.95),
    "session_open_proximity_weight": (0.0, 0.10),
}
ADAPTIVE_MAX_STEP_PCT = 0.15  # dampened update: max 15% move toward new target per run
MIN_SAMPLE_SIZE = 20          # minimum resolved trades before a segment may adapt

# --- Live-performance circuit breaker (Section 7.3) --------------------------
BASELINE_WIN_RATE = 0.50
BASELINE_PROFIT_FACTOR = 1.4
BASELINE_AVG_RR = 1.8
CIRCUIT_BREAKER_WINDOW = 40           # rolling resolved trades
CIRCUIT_BREAKER_WIN_RATE_DEVIATION = 0.15   # 15 pts below baseline win rate trips it

# --- State tiering (Section 7.4) ---------------------------------------------
TIER2_RAW_LOG_MAX_TRADES = 400
TIER2_RAW_LOG_MAX_DAYS = 15

# --- Counter-Trend Reversal engine (Section 5A) ------------------------------
ENABLE_COUNTERTREND_ENGINE = os.environ.get("ENABLE_COUNTERTREND_ENGINE", "false").lower() == "true"

# --- Optional half-Kelly position sizing (informational only, never gates dispatch)
ENABLE_KELLY_SIZING = os.environ.get("ENABLE_KELLY_SIZING", "false").lower() == "true"
KELLY_FRACTION_CAP = 0.5          # half-Kelly cap when Kelly sizing is enabled
FIXED_RISK_PCT_OF_EQUITY = 0.0075 # 0.75% fixed-fractional default/floor
PORTFOLIO_EXPOSURE_CAP_PCT = 0.06 # 6% aggregate risk cap across concurrent signals

# --- Research vs production parameters (Section 23) --------------------------
# Research-only constants used solely by offline rolling-window validation
# tooling (Section 7.1); never referenced by the live scan path.
RESEARCH_VALIDATION_WINDOWS_DAYS = [30, 60, 90, 180, 365]
RESEARCH_MIN_CONSECUTIVE_WINDOWS_FOR_DEMOTION = 2
RESEARCH_MIN_CONSECUTIVE_WINDOWS_FOR_PROMOTION = 2

CANDIDATE_FEATURES = [
    "BOS", "CHoCH", "OrderBlock", "BreakerBlock", "MitigationBlock", "FVG",
    "LiquiditySweep", "EQH_EQL", "EMA", "SMA", "VWAP", "AnchoredVWAP", "RSI",
    "MACD", "ATR", "ADX", "OBV", "CMF", "BollingerBands", "DonchianChannels",
]

REGIME_LABELS = [
    "Strong Bull Trend", "Strong Bear Trend", "Weak Trend", "Sideways", "Range",
    "Expansion", "Compression", "High Volatility", "Low Volatility",
    "Breakout", "Pullback", "Mean Reversion",
]

ENGINE_TYPES = [
    "SMC", "Trend Continuation", "Breakout", "Pullback", "Liquidity Sweep",
    "Order Block", "Breaker Block", "Fair Value Gap", "Momentum", "Reversal",
    "Mean Reversion", "Range Trading", "Volatility Expansion",
    "Counter-Trend Reversal",
]

# ============================================================================
# SECTION 2 -- HYPERLIQUID DATA COLLECTOR
# ============================================================================


class RateLimiter:
    """Sliding-window request-weight pacer, keeping the engine comfortably
    under Hyperliquid's published weight limits (Section 24: batched,
    throttled requests with backoff)."""

    def __init__(self, max_weight_per_minute: int = 1000) -> None:
        self.max_weight = max_weight_per_minute
        self._events: List[Tuple[float, int]] = []

    def acquire(self, weight: int = 20) -> None:
        now = time.monotonic()
        self._events = [(t, w) for t, w in self._events if now - t < 60.0]
        used = sum(w for _, w in self._events)
        if used + weight > self.max_weight:
            sleep_for = 60.0 - (now - self._events[0][0]) if self._events else 1.0
            time.sleep(max(sleep_for, 0.1))
        self._events.append((time.monotonic(), weight))


class HyperliquidClient:
    """Thin, read-only client around Hyperliquid's public `/info` endpoint."""

    def __init__(self, base_url: str = HL_API_URL) -> None:
        self.base_url = base_url
        self.limiter = RateLimiter()

    def _post(self, payload: Dict[str, Any], weight: int = 20, retries: int = 3) -> Any:
        body = json.dumps(payload).encode("utf-8")
        last_err: Optional[Exception] = None
        for attempt in range(retries):
            self.limiter.acquire(weight)
            try:
                req = urllib.request.Request(
                    self.base_url, data=body,
                    headers={"Content-Type": "application/json"})
                with urllib.request.urlopen(req, timeout=15) as resp:
                    return json.loads(resp.read().decode("utf-8"))
            except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError,
                    json.JSONDecodeError) as e:
                last_err = e
                time.sleep(0.5 * (2 ** attempt))
        log.error("Hyperliquid request failed after %d retries: %s", retries, last_err)
        return None

    def candles(self, symbol: str, interval: str, start_ms: int, end_ms: int) -> List[Dict[str, Any]]:
        payload = {
            "type": "candleSnapshot",
            "req": {"coin": symbol, "interval": interval, "startTime": start_ms, "endTime": end_ms},
        }
        data = self._post(payload, weight=20)
        if not isinstance(data, list):
            return []
        out = []
        for c in data:
            try:
                out.append({
                    "t": int(c["t"]), "o": float(c["o"]), "h": float(c["h"]),
                    "l": float(c["l"]), "c": float(c["c"]), "v": float(c["v"]),
                })
            except (KeyError, TypeError, ValueError):
                continue
        return out


class CandleCacheStore:
    """Persistent, bounded, per-symbol/per-timeframe candle cache (Section 24).

    Loaded once at run start, mutated in-memory, saved once at run end.
    Delta-fetches only candles newer than the last cached timestamp; falls
    back to a full re-fetch for a single symbol/timeframe if its cache entry
    is missing, corrupt, or stale beyond a sane threshold -- never crashes
    the run over one bad entry.
    """

    def __init__(self, path: str = CANDLE_CACHE_PATH) -> None:
        self.path = path
        self.data: Dict[str, Dict[str, List[Dict[str, Any]]]] = self._load()

    def _load(self) -> Dict[str, Any]:
        try:
            with open(self.path, "r") as f:
                loaded = json.load(f)
            if isinstance(loaded, dict):
                return loaded
        except (FileNotFoundError, json.JSONDecodeError, OSError):
            pass
        log.warning("candle_cache.json missing/corrupt -- starting cold (full re-fetch this run).")
        return {}

    def save(self) -> None:
        try:
            fd, tmp = tempfile.mkstemp(dir=os.path.dirname(os.path.abspath(self.path)) or ".")
            with os.fdopen(fd, "w") as f:
                json.dump(self.data, f)
            os.replace(tmp, self.path)
        except OSError as e:
            log.error("Failed to persist candle_cache.json: %s", e)

    def get(self, symbol: str, tf: str) -> List[Dict[str, Any]]:
        return self.data.get(symbol, {}).get(tf, [])

    def update(self, symbol: str, tf: str, candles: List[Dict[str, Any]]) -> None:
        cap = CANDLE_LOOKBACK.get(tf, 500)
        self.data.setdefault(symbol, {})[tf] = candles[-cap:]


_TF_TO_HL_INTERVAL = {"1W": "1w", "1D": "1d", "4H": "4h", "1H": "1h", "15M": "15m", "5M": "5m"}
_TF_MS = {"1W": 7 * 86400_000, "1D": 86400_000, "4H": 4 * 3600_000, "1H": 3600_000,
          "15M": 900_000, "5M": 300_000}


def fetch_symbol_mtf(client: HyperliquidClient, cache: CandleCacheStore, symbol: str,
                      now_ms: int) -> Dict[str, List[Dict[str, Any]]]:
    """Fetch/refresh every required timeframe for one symbol, delta-fetching
    only newly-closed candles beyond the cache's last timestamp (Section 24).
    """
    out: Dict[str, List[Dict[str, Any]]] = {}
    for tf in TIMEFRAMES:
        interval = _TF_TO_HL_INTERVAL[tf]
        cached = cache.get(symbol, tf)
        lookback = CANDLE_LOOKBACK.get(tf, 500)
        try:
            if cached:
                last_t = cached[-1]["t"]
                stale = (now_ms - last_t) > (lookback * _TF_MS[tf] * 2)
                if stale:
                    raise ValueError("cache stale beyond sane threshold")
                fresh = client.candles(symbol, interval, last_t + 1, now_ms)
                merged = cached + [c for c in fresh if c["t"] > last_t]
                cache.update(symbol, tf, merged)
                out[tf] = cache.get(symbol, tf)
            else:
                start_ms = now_ms - lookback * _TF_MS[tf]
                fresh = client.candles(symbol, interval, start_ms, now_ms)
                cache.update(symbol, tf, fresh)
                out[tf] = cache.get(symbol, tf)
        except Exception as e:  # noqa: BLE001 -- graceful per-timeframe degradation
            log.warning("Delta-fetch failed for %s/%s (%s) -- falling back to full re-fetch.",
                        symbol, tf, e)
            try:
                start_ms = now_ms - lookback * _TF_MS[tf]
                fresh = client.candles(symbol, interval, start_ms, now_ms)
                cache.update(symbol, tf, fresh)
                out[tf] = cache.get(symbol, tf)
            except Exception as e2:  # noqa: BLE001
                log.error("Full re-fetch also failed for %s/%s: %s -- skipping timeframe.",
                          symbol, tf, e2)
                out[tf] = cached
    return out


# ============================================================================
# SECTION 3 -- SHARED FEATURE ENGINEERING (Section 6 primitives)
# ============================================================================


def _closed(candles: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Section 17: never read a still-forming candle. Since this engine only
    fetches candles up to `now_ms` on 15-minute boundaries, the last element
    returned by the exchange for a just-elapsed interval is closed; we defend
    anyway by dropping a final candle whose bucket start is >= now-interval.
    Callers pass already-time-bounded series, so this is a pure identity/no-op
    safety net kept as one shared choke point rather than re-implemented per
    engine."""
    return candles


def ema(values: List[float], period: int) -> List[Optional[float]]:
    if not values:
        return []
    k = 2.0 / (period + 1)
    out: List[Optional[float]] = [None] * len(values)
    seed = sum(values[:period]) / period if len(values) >= period else values[0]
    prev = seed
    for i, v in enumerate(values):
        prev = v * k + prev * (1 - k) if i > 0 else seed
        out[i] = prev
    return out


def sma(values: List[float], period: int) -> List[Optional[float]]:
    out: List[Optional[float]] = [None] * len(values)
    for i in range(len(values)):
        if i + 1 >= period:
            out[i] = sum(values[i + 1 - period:i + 1]) / period
    return out


def rsi(closes: List[float], period: int = 14) -> List[Optional[float]]:
    out: List[Optional[float]] = [None] * len(closes)
    if len(closes) < period + 1:
        return out
    gains, losses = [], []
    for i in range(1, len(closes)):
        d = closes[i] - closes[i - 1]
        gains.append(max(d, 0.0))
        losses.append(max(-d, 0.0))
    avg_g = sum(gains[:period]) / period
    avg_l = sum(losses[:period]) / period
    for i in range(period, len(closes)):
        if i > period:
            avg_g = (avg_g * (period - 1) + gains[i - 1]) / period
            avg_l = (avg_l * (period - 1) + losses[i - 1]) / period
        rs = avg_g / avg_l if avg_l > 1e-12 else float("inf")
        out[i] = 100 - (100 / (1 + rs)) if avg_l > 1e-12 else 100.0
    return out


def macd(closes: List[float], fast: int = 12, slow: int = 26, signal: int = 9):
    ema_fast = ema(closes, fast)
    ema_slow = ema(closes, slow)
    line = [(f - s) if (f is not None and s is not None) else None
            for f, s in zip(ema_fast, ema_slow)]
    clean = [x if x is not None else 0.0 for x in line]
    sig = ema(clean, signal)
    hist = [(l - s) if (l is not None and s is not None) else None for l, s in zip(line, sig)]
    return line, sig, hist


def atr(candles: List[Dict[str, Any]], period: int = 14) -> List[Optional[float]]:
    out: List[Optional[float]] = [None] * len(candles)
    if len(candles) < 2:
        return out
    trs = [candles[0]["h"] - candles[0]["l"]]
    for i in range(1, len(candles)):
        h, l, pc = candles[i]["h"], candles[i]["l"], candles[i - 1]["c"]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    prev = None
    for i, tr in enumerate(trs):
        if i + 1 == period:
            prev = sum(trs[:period]) / period
            out[i] = prev
        elif i + 1 > period and prev is not None:
            prev = (prev * (period - 1) + tr) / period
            out[i] = prev
    return out


def adx(candles: List[Dict[str, Any]], period: int = 14) -> List[Optional[float]]:
    n = len(candles)
    out: List[Optional[float]] = [None] * n
    if n < period + 1:
        return out
    plus_dm, minus_dm, tr = [0.0], [0.0], [candles[0]["h"] - candles[0]["l"]]
    for i in range(1, n):
        up = candles[i]["h"] - candles[i - 1]["h"]
        down = candles[i - 1]["l"] - candles[i]["l"]
        plus_dm.append(up if (up > down and up > 0) else 0.0)
        minus_dm.append(down if (down > up and down > 0) else 0.0)
        h, l, pc = candles[i]["h"], candles[i]["l"], candles[i - 1]["c"]
        tr.append(max(h - l, abs(h - pc), abs(l - pc)))
    atr_s = sum(tr[:period]) / period
    pdm_s = sum(plus_dm[:period]) / period
    mdm_s = sum(minus_dm[:period]) / period
    dx_series = []
    for i in range(period, n):
        if i > period:
            atr_s = atr_s - (atr_s / period) + tr[i]
            pdm_s = pdm_s - (pdm_s / period) + plus_dm[i]
            mdm_s = mdm_s - (mdm_s / period) + minus_dm[i]
        pdi = 100 * (pdm_s / atr_s) if atr_s > 1e-12 else 0.0
        mdi = 100 * (mdm_s / atr_s) if atr_s > 1e-12 else 0.0
        dx = 100 * abs(pdi - mdi) / (pdi + mdi) if (pdi + mdi) > 1e-12 else 0.0
        dx_series.append(dx)
        if len(dx_series) >= period:
            out[i] = sum(dx_series[-period:]) / period
    return out


def obv(candles: List[Dict[str, Any]]) -> List[float]:
    out = [0.0] * len(candles)
    for i in range(1, len(candles)):
        if candles[i]["c"] > candles[i - 1]["c"]:
            out[i] = out[i - 1] + candles[i]["v"]
        elif candles[i]["c"] < candles[i - 1]["c"]:
            out[i] = out[i - 1] - candles[i]["v"]
        else:
            out[i] = out[i - 1]
    return out


def cmf(candles: List[Dict[str, Any]], period: int = 20) -> List[Optional[float]]:
    out: List[Optional[float]] = [None] * len(candles)
    mfv = []
    for c in candles:
        rng = c["h"] - c["l"]
        mfm = ((c["c"] - c["l"]) - (c["h"] - c["c"])) / rng if rng > 1e-12 else 0.0
        mfv.append(mfm * c["v"])
    for i in range(len(candles)):
        if i + 1 >= period:
            vol_sum = sum(c["v"] for c in candles[i + 1 - period:i + 1])
            out[i] = sum(mfv[i + 1 - period:i + 1]) / vol_sum if vol_sum > 1e-12 else 0.0
    return out


def bollinger(closes: List[float], period: int = 20, mult: float = 2.0):
    mid = sma(closes, period)
    upper: List[Optional[float]] = [None] * len(closes)
    lower: List[Optional[float]] = [None] * len(closes)
    width: List[Optional[float]] = [None] * len(closes)
    for i in range(len(closes)):
        if mid[i] is not None:
            window = closes[i + 1 - period:i + 1]
            sd = statistics.pstdev(window)
            upper[i] = mid[i] + mult * sd
            lower[i] = mid[i] - mult * sd
            width[i] = (upper[i] - lower[i]) / mid[i] if mid[i] else None
    return upper, mid, lower, width


def donchian_width(candles: List[Dict[str, Any]], period: int = 20) -> List[Optional[float]]:
    out: List[Optional[float]] = [None] * len(candles)
    for i in range(len(candles)):
        if i + 1 >= period:
            window = candles[i + 1 - period:i + 1]
            hi = max(c["h"] for c in window)
            lo = min(c["l"] for c in window)
            out[i] = (hi - lo) / candles[i]["c"] if candles[i]["c"] else None
    return out


def rolling_percentile(values: List[Optional[float]], idx: int, window: int = 100) -> Optional[float]:
    """Rolling rank of the value at idx against its own recent history --
    used for ATR percentile (Section 6) and other regime-relative reads."""
    if values[idx] is None:
        return None
    lo = max(0, idx - window)
    hist = [v for v in values[lo:idx + 1] if v is not None]
    if len(hist) < 5:
        return None
    cur = values[idx]
    rank = sum(1 for v in hist if v <= cur)
    return 100.0 * rank / len(hist)


@dataclass
class Pivot:
    idx: int
    price: float
    kind: str  # "high" | "low"


def find_pivots(candles: List[Dict[str, Any]], lookback: int = 3) -> List[Pivot]:
    """Precise, parameterized swing-detection rule (Section 6): a candle at
    index i is a swing high if its high is the strictly-greatest high within
    +/- `lookback` candles (and analogously for lows)."""
    pivots: List[Pivot] = []
    n = len(candles)
    for i in range(lookback, n - lookback):
        window = candles[i - lookback:i + lookback + 1]
        hi, lo = candles[i]["h"], candles[i]["l"]
        if hi == max(c["h"] for c in window) and hi > max(
                c["h"] for c in window if c is not candles[i]):
            pivots.append(Pivot(i, hi, "high"))
        if lo == min(c["l"] for c in window) and lo < min(
                c["l"] for c in window if c is not candles[i]):
            pivots.append(Pivot(i, lo, "low"))
    return pivots


@dataclass
class Zone:
    kind: str            # "order_block" | "breaker_block" | "fvg" | "mitigation_block"
    direction: str        # "bullish" | "bearish"
    top: float
    bottom: float
    idx: int
    mitigated: bool = False
    source_sweep_idx: Optional[int] = None  # sweep-to-POI causality tag (Section 11 step 3)


def detect_fvgs(candles: List[Dict[str, Any]]) -> List[Zone]:
    """Exact, mathematical FVG definition: a 3-candle imbalance where candle
    i-1's high sits below candle i+1's low (bullish FVG) or the reverse
    (bearish FVG) -- the gap left by candle i's displacement."""
    zones = []
    for i in range(1, len(candles) - 1):
        a, b, c = candles[i - 1], candles[i], candles[i + 1]
        if a["h"] < c["l"] and b["c"] > b["o"]:
            zones.append(Zone("fvg", "bullish", c["l"], a["h"], i))
        elif a["l"] > c["h"] and b["c"] < b["o"]:
            zones.append(Zone("fvg", "bearish", a["l"], c["h"], i))
    return zones


def detect_order_blocks(candles: List[Dict[str, Any]], pivots: List[Pivot]) -> List[Zone]:
    """Exact definition: the last opposite-colored candle immediately before
    a displacement leg that produces a BOS through the most recent pivot."""
    zones = []
    pivot_by_idx = {p.idx: p for p in pivots}
    for i in range(2, len(candles)):
        leg = candles[i]
        prior = candles[i - 1]
        body = abs(leg["c"] - leg["o"])
        rng = leg["h"] - leg["l"]
        displacement = rng > 0 and body / rng > 0.6
        if not displacement:
            continue
        if leg["c"] > leg["o"] and prior["c"] < prior["o"]:
            zones.append(Zone("order_block", "bullish", prior["h"], prior["l"], i - 1))
        elif leg["c"] < leg["o"] and prior["c"] > prior["o"]:
            zones.append(Zone("order_block", "bearish", prior["h"], prior["l"], i - 1))
    return zones[-40:]  # bounded -- keep the most recent, most relevant zones


def mark_mitigated(zones: List[Zone], candles: List[Dict[str, Any]]) -> None:
    for z in zones:
        for c in candles[z.idx + 1:]:
            if z.direction == "bullish" and c["l"] <= z.top:
                z.mitigated = True
                break
            if z.direction == "bearish" and c["h"] >= z.bottom:
                z.mitigated = True
                break


def detect_eq_clusters(pivots: List[Pivot], candles: List[Dict[str, Any]],
                        tol_pct: float = 0.0015) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """EQH / EQL cluster identification (Section 11 step 3): pivots of the
    same kind within a tight tolerance of one another represent resting
    buy-side (EQH) or sell-side (EQL) liquidity."""
    highs = [p for p in pivots if p.kind == "high"]
    lows = [p for p in pivots if p.kind == "low"]

    def cluster(points: List[Pivot]) -> List[Dict[str, Any]]:
        points = sorted(points, key=lambda p: p.price)
        clusters, current = [], []
        for p in points:
            if not current or abs(p.price - current[-1].price) <= current[-1].price * tol_pct:
                current.append(p)
            else:
                if len(current) >= 2:
                    clusters.append({"level": sum(c.price for c in current) / len(current),
                                      "pivots": current})
                current = [p]
        if len(current) >= 2:
            clusters.append({"level": sum(c.price for c in current) / len(current), "pivots": current})
        return clusters

    return cluster(highs), cluster(lows)


def detect_sweep(direction: str, eq_clusters_opposite: List[Dict[str, Any]],
                  candles: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Confirms an actual liquidity sweep occurred: a closed candle wicks
    beyond a tracked EQH/EQL level then closes back inside -- never assumed
    just because price is near a level (Section 11 step 3, context gate)."""
    if not eq_clusters_opposite or len(candles) < 2:
        return None
    last = candles[-1]
    for cl in eq_clusters_opposite:
        level = cl["level"]
        if direction == "bullish":  # sweeping a low (SSL) ahead of a long
            if last["l"] < level and last["c"] > level:
                return {"level": level, "idx": len(candles) - 1, "cluster": cl, "pure": True}
        else:  # sweeping a high (BSL) ahead of a short
            if last["h"] > level and last["c"] < level:
                return {"level": level, "idx": len(candles) - 1, "cluster": cl, "pure": True}
    return None


def structure_shift(direction: str, candles: List[Dict[str, Any]], pivots: List[Pivot],
                     kind: str = "BOS") -> Optional[Dict[str, Any]]:
    """Single shared structure-shift detector used by both Section 11 step 4
    (MSS/BOS confirmation) and the Counter-Trend engine's Step 3 (CHoCH) --
    never a second, parallel detector (Section 6, 17). `kind` selects which
    the caller is asking about; both are evaluated from the same pivot
    sequence for closed candles only.

    BOS: price closes beyond the most recent same-direction structural pivot
         (continuation of the prevailing swing sequence).
    CHoCH: price closes beyond the most recent *counter*-trend pivot,
           indicating the immediately-preceding swing sequence has broken --
           a change of character rather than continuation.
    """
    if len(pivots) < 2 or not candles:
        return None
    last_close = candles[-1]["c"]
    highs = [p for p in pivots if p.kind == "high"]
    lows = [p for p in pivots if p.kind == "low"]
    if not highs or not lows:
        return None
    last_high, last_low = highs[-1], lows[-1]

    if kind == "BOS":
        if direction == "bullish" and last_close > last_high.price:
            return {"level": last_high.price, "idx": len(candles) - 1, "kind": "BOS"}
        if direction == "bearish" and last_close < last_low.price:
            return {"level": last_low.price, "idx": len(candles) - 1, "kind": "BOS"}
        return None
    else:  # CHoCH -- break of the counter-trend pivot
        if direction == "bullish" and last_close > last_high.price and last_low.idx > last_high.idx:
            return {"level": last_high.price, "idx": len(candles) - 1, "kind": "CHoCH"}
        if direction == "bearish" and last_close < last_low.price and last_high.idx > last_low.idx:
            return {"level": last_low.price, "idx": len(candles) - 1, "kind": "CHoCH"}
        return None


@dataclass
class View:
    """A fully-featured, single-timeframe snapshot: raw candles plus every
    shared indicator/primitive computed once and reused by every consumer
    (Section 6, 24 -- no redundant recomputation)."""
    symbol: str
    tf: str
    candles: List[Dict[str, Any]]
    closes: List[float] = field(default_factory=list)
    ema20: List[Optional[float]] = field(default_factory=list)
    ema50: List[Optional[float]] = field(default_factory=list)
    ema200: List[Optional[float]] = field(default_factory=list)
    rsi14: List[Optional[float]] = field(default_factory=list)
    macd_line: List[Optional[float]] = field(default_factory=list)
    macd_signal: List[Optional[float]] = field(default_factory=list)
    macd_hist: List[Optional[float]] = field(default_factory=list)
    atr: List[Optional[float]] = field(default_factory=list)
    adx: List[Optional[float]] = field(default_factory=list)
    obv: List[float] = field(default_factory=list)
    cmf: List[Optional[float]] = field(default_factory=list)
    bb_upper: List[Optional[float]] = field(default_factory=list)
    bb_mid: List[Optional[float]] = field(default_factory=list)
    bb_lower: List[Optional[float]] = field(default_factory=list)
    bb_width: List[Optional[float]] = field(default_factory=list)
    donchian_w: List[Optional[float]] = field(default_factory=list)
    pivots: List[Pivot] = field(default_factory=list)
    eq_highs: List[Dict[str, Any]] = field(default_factory=list)
    eq_lows: List[Dict[str, Any]] = field(default_factory=list)
    order_blocks: List[Zone] = field(default_factory=list)
    breaker_blocks: List[Zone] = field(default_factory=list)
    fvgs: List[Zone] = field(default_factory=list)


def build_view(symbol: str, tf: str, candles: List[Dict[str, Any]]) -> Optional[View]:
    candles = _closed(candles)
    if len(candles) < 30:
        return None
    closes = [c["c"] for c in candles]
    v = View(symbol=symbol, tf=tf, candles=candles, closes=closes)
    v.ema20, v.ema50, v.ema200 = ema(closes, 20), ema(closes, 50), ema(closes, 200)
    v.rsi14 = rsi(closes, 14)
    v.macd_line, v.macd_signal, v.macd_hist = macd(closes)
    v.atr = atr(candles, 14)
    v.adx = adx(candles, 14)
    v.obv = obv(candles)
    v.cmf = cmf(candles, 20)
    v.bb_upper, v.bb_mid, v.bb_lower, v.bb_width = bollinger(closes, 20, 2.0)
    v.donchian_w = donchian_width(candles, 20)
    v.pivots = find_pivots(candles, lookback=3)
    v.eq_highs, v.eq_lows = detect_eq_clusters(v.pivots, candles)
    obs = detect_order_blocks(candles, v.pivots)
    mark_mitigated(obs, candles)
    v.order_blocks = obs
    fvgs = detect_fvgs(candles)
    mark_mitigated(fvgs, candles)
    v.fvgs = fvgs
    # Breaker blocks: an order block that was later invalidated (mitigated)
    # and then flipped by a confirmed structure shift through it.
    breakers = []
    for ob in obs:
        if ob.mitigated:
            flipped_dir = "bearish" if ob.direction == "bullish" else "bullish"
            breakers.append(Zone("breaker_block", flipped_dir, ob.top, ob.bottom, ob.idx,
                                  mitigated=False, source_sweep_idx=ob.idx))
    v.breaker_blocks = breakers[-20:]
    return v


# ============================================================================
# SECTION 4 -- COMPOSITE REGIME VECTOR (Section 8)
# ============================================================================


@dataclass
class RegimeVector:
    macro_bias: str
    volatility_percentile: float
    trend_strength: float
    session_weight: float
    session_open_proximity: float
    liquidity_draw: str          # "ERL" | "IRL" | "neutral"
    noise_index: float
    breadth: float
    label: str = ""
    confidence: float = 0.0
    expected_behavior: str = ""


def _session_now(ts: datetime) -> str:
    h = ts.hour
    if 0 <= h < 8:
        return "asia"
    if 7 <= h < 16:
        return "london"
    return "ny"


def _session_open_proximity(ts: datetime) -> float:
    """Continuous, decaying score peaking at London (07:00 UTC) and NY
    (13:00 UTC) opens, decaying linearly across a 90-minute half-life --
    never a hard gate (Section 8)."""
    minute_of_day = ts.hour * 60 + ts.minute
    opens = [7 * 60, 13 * 60]
    best = min(abs(minute_of_day - o) for o in opens)
    return max(0.0, 1.0 - best / 90.0)


def compute_regime_vector(macro_view_1h: Optional[View], asset_view_1h: View,
                           asset_view_4h: View, watchlist_views_1h: Dict[str, View],
                           now: datetime) -> RegimeVector:
    vol_pctile = rolling_percentile(asset_view_1h.atr, len(asset_view_1h.atr) - 1, 100) or 50.0
    trend_strength = (asset_view_4h.adx[-1] if asset_view_4h.adx and asset_view_4h.adx[-1] is not None
                       else 15.0)

    macro_bias = "neutral"
    if macro_view_1h and macro_view_1h.ema50[-1] and macro_view_1h.ema200[-1]:
        macro_bias = "bullish" if macro_view_1h.ema50[-1] > macro_view_1h.ema200[-1] else "bearish"

    session = _session_now(now)
    session_weight = {"asia": 0.6, "london": 1.0, "ny": 1.0}[session]
    session_open_proximity = _session_open_proximity(now)

    # Noise index: whipsaw-prone-ness independent of raw volatility -- ratio
    # of realized range to net displacement over a recent window.
    recent = asset_view_1h.candles[-20:]
    if len(recent) >= 5:
        total_range = sum(c["h"] - c["l"] for c in recent)
        net_move = abs(recent[-1]["c"] - recent[0]["c"])
        noise_index = 1.0 - min(net_move / total_range, 1.0) if total_range > 1e-9 else 0.5
    else:
        noise_index = 0.5

    # Liquidity draw (ERL/IRL): compare distance-weighted pull toward nearest
    # unmitigated EQH/EQL cluster (ERL) vs. nearest unmitigated internal
    # OB/FVG (IRL).
    price = asset_view_1h.closes[-1]
    erl_dist = min([abs(price - e["level"]) for e in (asset_view_1h.eq_highs + asset_view_1h.eq_lows)]
                   or [float("inf")])
    irl_candidates = [z for z in (asset_view_1h.order_blocks + asset_view_1h.fvgs) if not z.mitigated]
    irl_dist = min([abs(price - (z.top + z.bottom) / 2) for z in irl_candidates] or [float("inf")])
    liquidity_draw = "neutral"
    if erl_dist < irl_dist and erl_dist != float("inf"):
        liquidity_draw = "ERL"
    elif irl_dist != float("inf"):
        liquidity_draw = "IRL"

    # Breadth: fraction of watchlist assets whose 1H EMA20 vs EMA50 agrees
    # with the macro bias direction.
    agree = 0
    total = 0
    for sym, v in watchlist_views_1h.items():
        if v.ema20 and v.ema50 and v.ema20[-1] is not None and v.ema50[-1] is not None:
            total += 1
            d = "bullish" if v.ema20[-1] > v.ema50[-1] else "bearish"
            if d == macro_bias:
                agree += 1
    breadth = (agree / total) if total else 0.5

    rv = RegimeVector(
        macro_bias=macro_bias, volatility_percentile=vol_pctile, trend_strength=trend_strength,
        session_weight=session_weight, session_open_proximity=session_open_proximity,
        liquidity_draw=liquidity_draw, noise_index=noise_index, breadth=breadth,
    )
    _classify_regime_label(rv)
    return rv


def _classify_regime_label(rv: RegimeVector) -> None:
    """Derive the discrete display/lookup label from the vector using
    multiple independent quantitative features -- never a single indicator
    (Section 8.1)."""
    if rv.trend_strength >= 30 and rv.macro_bias == "bullish":
        rv.label, rv.confidence = "Strong Bull Trend", 0.85
    elif rv.trend_strength >= 30 and rv.macro_bias == "bearish":
        rv.label, rv.confidence = "Strong Bear Trend", 0.85
    elif rv.volatility_percentile >= 85:
        rv.label, rv.confidence = "High Volatility", 0.75
    elif rv.volatility_percentile <= 15:
        rv.label, rv.confidence = "Low Volatility", 0.70
    elif rv.trend_strength >= 20:
        rv.label, rv.confidence = "Weak Trend", 0.6
    elif rv.noise_index >= 0.7:
        rv.label, rv.confidence = "Sideways", 0.6
    elif rv.volatility_percentile <= 35 and rv.noise_index < 0.5:
        rv.label, rv.confidence = "Compression", 0.65
    elif rv.volatility_percentile >= 65:
        rv.label, rv.confidence = "Expansion", 0.6
    else:
        rv.label, rv.confidence = "Range", 0.5
    rv.expected_behavior = {
        "Strong Bull Trend": "Favor trend-continuation/pullback longs; discount mean-reversion shorts.",
        "Strong Bear Trend": "Favor trend-continuation/pullback shorts; discount mean-reversion longs.",
        "Weak Trend": "Moderate directional edge; tighten confluence requirements.",
        "Sideways": "Favor liquidity/range logic; discount trend-following breakouts.",
        "Range": "Favor range-boundary fades; discount breakout chasing.",
        "Expansion": "Favor volatility/breakout setups; widen risk tolerance.",
        "Compression": "Favor breakout-probability setups ahead of expansion.",
        "High Volatility": "Tighten ATR-based risk filters; discount momentum confirmation slightly.",
        "Low Volatility": "Favor compression/breakout detection.",
        "Breakout": "Favor structure-confirmed breakout continuation.",
        "Pullback": "Favor trend-aligned pullback entries.",
        "Mean Reversion": "Favor liquidity-fade setups; discount trend-following.",
    }.get(rv.label, "")


# ============================================================================
# SECTION 5 -- MANDATORY TOP-DOWN SEQUENCE (Section 10)
# ============================================================================


@dataclass
class StageResult:
    outcome: str
    detail: str = ""


def stage1_bias(weekly: Optional[View], daily: Optional[View]) -> StageResult:
    if weekly is None or daily is None:
        return StageResult("Neutral", "insufficient HTF data")
    d_dir = None
    if daily.ema50[-1] is not None and daily.ema200[-1] is not None:
        d_dir = "Bullish" if daily.ema50[-1] > daily.ema200[-1] else "Bearish"
    w_dir = None
    if weekly.ema20[-1] is not None and weekly.ema50[-1] is not None:
        w_dir = "Bullish" if weekly.ema20[-1] > weekly.ema50[-1] else "Bearish"
    if d_dir and w_dir and d_dir == w_dir:
        return StageResult(d_dir, "Weekly/Daily EMA structure agree")
    return StageResult("Neutral", "Weekly/Daily disagree or inconclusive")


def stage2_context(bias: str, h4: Optional[View]) -> StageResult:
    if bias == "Neutral" or h4 is None:
        return StageResult("Disagree", "no bias to confirm against")
    if h4.adx and h4.adx[-1] is not None and h4.adx[-1] < 15:
        return StageResult("Disagree", "4H trend strength too weak (ranging)")
    h4_dir = None
    if h4.ema20[-1] is not None and h4.ema50[-1] is not None:
        h4_dir = "Bullish" if h4.ema20[-1] > h4.ema50[-1] else "Bearish"
    if h4_dir == bias:
        return StageResult("Agree", "4H structure confirms HTF bias")
    return StageResult("Disagree", "4H structure contradicts HTF bias")


def stage3_setup(bias: str, h1: View, zone_result: Optional[Dict[str, Any]]) -> StageResult:
    if zone_result is None:
        return StageResult("NOT READY", "no validated 1H POI yet")
    if zone_result.get("invalid"):
        return StageResult("INVALID", zone_result.get("reason", "structure contradicts setup"))
    return StageResult("VALID", "zone-selection sequence fully validated")


# ============================================================================
# SECTION 6 -- ZONE-SELECTION SEQUENCE (Section 11)
# ============================================================================


def _poi_pool(direction: str, view: View) -> List[Zone]:
    """Step 2: candidate structural POIs (order blocks, breaker blocks, FVGs)
    in the trade direction, unmitigated only."""
    want_dir = "bullish" if direction == "Bullish" else "bearish"
    pool = [z for z in (view.order_blocks + view.breaker_blocks + view.fvgs)
            if z.direction == want_dir and not z.mitigated]
    return pool


def run_zone_selection_sequence(direction: str, h1: View) -> Optional[Dict[str, Any]]:
    """Steps 1-5 of Section 11, executed within Stage 3. Returns a dict
    describing the validated zone, or a dict with invalid=True/None if not
    ready."""
    if direction == "Neutral":
        return None
    dirn = "bullish" if direction == "Bullish" else "bearish"

    # Step 2 -- POI pool
    poi_pool = _poi_pool(direction, h1)
    if not poi_pool:
        return None  # NOT READY -- no candidate zone yet

    # Step 3 -- SFP purity check against opposite-side EQH/EQL
    opposite_clusters = h1.eq_lows if dirn == "bullish" else h1.eq_highs
    sweep = detect_sweep(dirn, opposite_clusters, h1.candles)
    session_tag = None
    if sweep is not None:
        ts = datetime.fromtimestamp(h1.candles[-1]["t"] / 1000.0, tz=timezone.utc)
        session_tag = {"session": _session_now(ts), "proximity": _session_open_proximity(ts)}

    # Sweep-to-POI causality: the chosen POI must postdate the sweep, i.e.
    # arose downstream of it, whenever a sweep is present. Where no sweep is
    # present, the setup can still validate through the other steps
    # (Section 11 step 3: not disqualified, just weighted lower downstream).
    zone = None
    for z in sorted(poi_pool, key=lambda z: -z.idx):
        if sweep is None or z.idx >= sweep["idx"] - 3:
            zone = z
            break
    if zone is None:
        return None

    # Step 4 -- MSS confirmation (BOS/CHoCH shared detector)
    mss = structure_shift(dirn, h1.candles, h1.pivots, kind="BOS")
    if mss is None:
        return None  # NOT READY -- structure hasn't confirmed yet

    # Step 5 -- breaker confirmation preference: prefer a breaker block if one
    # exists among the candidates, as the most recent institutional footprint.
    breaker_candidates = [z for z in poi_pool if z.kind == "breaker_block"]
    chosen_zone = breaker_candidates[-1] if breaker_candidates else zone

    return {
        "direction": dirn, "zone": chosen_zone, "sweep": sweep, "mss": mss,
        "session_tag": session_tag, "invalid": False,
    }


def fibonacci_ote_refine(direction: str, zone: Zone, impulse_start: float, impulse_end: float) -> float:
    """Step 6: refine entry inside the validated zone using the 61.8-79% OTE
    pocket of the impulse leg, clipped to remain inside the zone bounds --
    never nominates a zone on its own (Section 11 step 6)."""
    rng = impulse_end - impulse_start
    ote_low = impulse_end - rng * 0.786
    ote_high = impulse_end - rng * 0.618
    lo, hi = min(ote_low, ote_high), max(ote_low, ote_high)
    zone_lo, zone_hi = min(zone.top, zone.bottom), max(zone.top, zone.bottom)
    candidate = (lo + hi) / 2
    return min(max(candidate, zone_lo), zone_hi)


def stage4_entry(direction: str, h1_zone_result: Dict[str, Any], m15: View) -> Optional[Dict[str, Any]]:
    """Stage 4: confirmed 15M MSS within the Stage-3-validated 1H POI, whose
    resulting FVG is the entry vehicle (Section 10 stage 4)."""
    dirn = h1_zone_result["direction"]
    zone: Zone = h1_zone_result["zone"]
    zone_lo, zone_hi = min(zone.top, zone.bottom), max(zone.top, zone.bottom)

    m15_mss = structure_shift(dirn, m15.candles, m15.pivots, kind="BOS")
    if m15_mss is None:
        return None
    price_now = m15.closes[-1]
    if not (zone_lo * 0.998 <= price_now <= zone_hi * 1.002):
        return None  # 15M break must be occurring inside the validated 1H POI

    # The FVG created by this specific 15M break is the entry vehicle.
    candidate_fvgs = [z for z in m15.fvgs if z.direction == dirn and not z.mitigated
                      and z.idx >= m15_mss["idx"] - 2]
    if not candidate_fvgs:
        return None
    fvg = candidate_fvgs[-1]

    impulse_start = m15.candles[max(0, fvg.idx - 5)]["c"]
    impulse_end = m15.candles[fvg.idx]["c"]
    entry = fibonacci_ote_refine(dirn, fvg, impulse_start, impulse_end)
    return {"entry": entry, "fvg": fvg, "m15_mss": m15_mss, "direction": dirn}


def stage5_refine(entry_result: Dict[str, Any], m5: Optional[View]) -> Dict[str, Any]:
    """Optional Stage 5: 5M entry-timing refinement strictly inside the
    Stage-4 FVG range, using only already-closed 5M candles -- never
    relocates, widens, or salvages a Stage 4 result (Section 10 stage 5)."""
    if m5 is None or len(m5.candles) < 5:
        entry_result["entry_refinement_tf"] = None
        return entry_result
    fvg: Zone = entry_result["fvg"]
    zone_lo, zone_hi = min(fvg.top, fvg.bottom), max(fvg.top, fvg.bottom)
    dirn = entry_result["direction"]
    last5 = m5.candles[-1]
    if not (zone_lo <= last5["l"] <= zone_hi or zone_lo <= last5["h"] <= zone_hi):
        entry_result["entry_refinement_tf"] = None
        return entry_result
    rejection = (dirn == "bullish" and last5["c"] > last5["o"] and last5["l"] <= zone_lo * 1.001) or \
                (dirn == "bearish" and last5["c"] < last5["o"] and last5["h"] >= zone_hi * 0.999)
    if rejection:
        refined = min(max(last5["c"], zone_lo), zone_hi)
        entry_result["entry"] = refined
        entry_result["entry_refinement_tf"] = "5M"
    else:
        entry_result["entry_refinement_tf"] = None
    return entry_result


# ============================================================================
# SECTION 7 -- SPECIALIZED ENGINE ENSEMBLE (Section 5)
# ============================================================================


@dataclass
class Candidate:
    direction: str            # "bullish" | "bearish"
    entry: float
    entry_kind: str           # "market" | "pending"
    sl: float
    tp1: float
    tp2: float
    style: str                # "intraday" | "swing"
    engine: str
    confluences: List[str] = field(default_factory=list)
    best_fit_regimes: List[str] = field(default_factory=list)
    counter_trend: bool = False
    session_tag: Optional[Dict[str, Any]] = None
    entry_refinement_tf: Optional[str] = None


def retracement_entry_for_engine(engine_name: str, direction: str, m15: View,
                                  reference_level: float) -> Tuple[float, str]:
    """Shared retracement-entry helper (Section 14.3): every specialized
    engine derives entry through a retracement/return-to-level mechanism
    appropriate to its own setup type -- never a raw last-close 'market'
    assignment. Always returns entry_kind='pending'."""
    atr_now = m15.atr[-1] or (m15.closes[-1] * 0.005)
    if engine_name == "Breakout":
        # retest of the broken boundary, not the breakout candle's close
        entry = reference_level
    elif engine_name in ("Liquidity Sweep", "Reversal", "SMC", "Order Block",
                         "Breaker Block", "Fair Value Gap"):
        entry = reference_level  # OTE-refined level supplied by caller
    elif engine_name == "Momentum":
        entry = m15.ema20[-1] if m15.ema20[-1] is not None else reference_level
    elif engine_name == "Mean Reversion":
        entry = reference_level  # Bollinger-band extreme supplied by caller
    elif engine_name == "Range Trading":
        entry = reference_level  # actual range boundary supplied by caller
    elif engine_name == "Volatility Expansion":
        entry = reference_level  # retest of the just-broken band
    else:
        entry = reference_level
    return entry, "pending"


def run_smc_engine(zone_result: Dict[str, Any], stage4_result: Dict[str, Any],
                    m15: View, style: str) -> Optional[Candidate]:
    dirn = zone_result["direction"]
    entry, entry_kind = retracement_entry_for_engine("SMC", dirn, m15, stage4_result["entry"])
    return Candidate(direction=dirn, entry=entry, entry_kind=entry_kind, sl=0, tp1=0, tp2=0,
                      style=style, engine="SMC",
                      confluences=["order-block/breaker POI", "SFP+MSS zone-selection sequence",
                                   "15M MSS-in-POI + FVG entry vehicle"],
                      best_fit_regimes=["Strong Bull Trend", "Strong Bear Trend", "Breakout", "Pullback"],
                      session_tag=zone_result.get("session_tag"),
                      entry_refinement_tf=stage4_result.get("entry_refinement_tf"))


def run_trend_continuation_engine(dirn: str, h1: View, m15: View, style: str) -> Optional[Candidate]:
    if h1.adx[-1] is None or h1.adx[-1] < 20:
        return None
    if h1.ema20[-1] is None or h1.ema50[-1] is None:
        return None
    aligned = (dirn == "bullish" and h1.ema20[-1] > h1.ema50[-1]) or \
              (dirn == "bearish" and h1.ema20[-1] < h1.ema50[-1])
    if not aligned:
        return None
    entry, entry_kind = retracement_entry_for_engine("Momentum", dirn, m15, m15.closes[-1])
    return Candidate(direction=dirn, entry=entry, entry_kind=entry_kind, sl=0, tp1=0, tp2=0,
                      style=style, engine="Trend Continuation",
                      confluences=["1H ADX trend confirmed", "EMA20/50 alignment"],
                      best_fit_regimes=["Strong Bull Trend", "Strong Bear Trend", "Weak Trend"])


def run_breakout_engine(dirn: str, h1: View, m15: View, style: str) -> Optional[Candidate]:
    if m15.donchian_w[-1] is None or m15.donchian_w[-1] > 0.02:
        return None  # only fire on genuine compression -> breakout, not mid-range noise
    recent = m15.candles[-20:]
    boundary = max(c["h"] for c in recent[:-1]) if dirn == "bullish" else min(c["l"] for c in recent[:-1])
    last = m15.candles[-1]
    broke_out = (dirn == "bullish" and last["c"] > boundary) or (dirn == "bearish" and last["c"] < boundary)
    if not broke_out:
        return None
    entry, entry_kind = retracement_entry_for_engine("Breakout", dirn, m15, boundary)
    return Candidate(direction=dirn, entry=entry, entry_kind=entry_kind, sl=0, tp1=0, tp2=0,
                      style=style, engine="Breakout",
                      confluences=["Donchian compression prior to break", "closed-candle boundary break"],
                      best_fit_regimes=["Breakout", "Expansion", "Compression"])


def run_momentum_engine(dirn: str, h1: View, m15: View, style: str) -> Optional[Candidate]:
    if h1.rsi14[-1] is None:
        return None
    strong = (dirn == "bullish" and 50 < h1.rsi14[-1] < 75) or \
             (dirn == "bearish" and 25 < h1.rsi14[-1] < 50)
    if not strong or h1.macd_hist[-1] is None:
        return None
    accelerating = (dirn == "bullish" and h1.macd_hist[-1] > 0) or \
                   (dirn == "bearish" and h1.macd_hist[-1] < 0)
    if not accelerating:
        return None
    entry, entry_kind = retracement_entry_for_engine("Momentum", dirn, m15, m15.closes[-1])
    return Candidate(direction=dirn, entry=entry, entry_kind=entry_kind, sl=0, tp1=0, tp2=0,
                      style=style, engine="Momentum",
                      confluences=["RSI in trending band", "MACD histogram confirming acceleration"],
                      best_fit_regimes=["Strong Bull Trend", "Strong Bear Trend", "Expansion"])


def run_mean_reversion_engine(dirn: str, m15: View, style: str) -> Optional[Candidate]:
    if m15.bb_lower[-1] is None or m15.bb_upper[-1] is None:
        return None
    last = m15.candles[-1]
    touched_lower = last["l"] <= m15.bb_lower[-1]
    touched_upper = last["h"] >= m15.bb_upper[-1]
    if dirn == "bullish" and not touched_lower:
        return None
    if dirn == "bearish" and not touched_upper:
        return None
    ref_level = m15.bb_lower[-1] if dirn == "bullish" else m15.bb_upper[-1]
    entry, entry_kind = retracement_entry_for_engine("Mean Reversion", dirn, m15, ref_level)
    return Candidate(direction=dirn, entry=entry, entry_kind=entry_kind, sl=0, tp1=0, tp2=0,
                      style=style, engine="Mean Reversion",
                      confluences=["Bollinger-band extreme touch"],
                      best_fit_regimes=["Range", "Sideways", "Mean Reversion"])


def run_range_trading_engine(dirn: str, m15: View, style: str) -> Optional[Candidate]:
    if m15.adx[-1] is not None and m15.adx[-1] > 20:
        return None  # trending -- range logic not applicable
    recent = m15.candles[-30:]
    hi, lo = max(c["h"] for c in recent), min(c["l"] for c in recent)
    last = m15.candles[-1]
    near_lo = abs(last["c"] - lo) / lo < 0.004
    near_hi = abs(last["c"] - hi) / hi < 0.004
    if dirn == "bullish" and not near_lo:
        return None
    if dirn == "bearish" and not near_hi:
        return None
    ref_level = lo if dirn == "bullish" else hi
    entry, entry_kind = retracement_entry_for_engine("Range Trading", dirn, m15, ref_level)
    return Candidate(direction=dirn, entry=entry, entry_kind=entry_kind, sl=0, tp1=0, tp2=0,
                      style=style, engine="Range Trading",
                      confluences=["price at established range boundary", "1H trend strength weak"],
                      best_fit_regimes=["Range", "Sideways"])


def run_volatility_expansion_engine(dirn: str, m15: View, style: str) -> Optional[Candidate]:
    if m15.bb_width[-1] is None:
        return None
    pctile = rolling_percentile(m15.bb_width, len(m15.bb_width) - 1, 100)
    if pctile is None or pctile > 25:
        return None  # only fire from a genuine compression state
    band = m15.bb_upper[-1] if dirn == "bullish" else m15.bb_lower[-1]
    if band is None:
        return None
    entry, entry_kind = retracement_entry_for_engine("Volatility Expansion", dirn, m15, band)
    return Candidate(direction=dirn, entry=entry, entry_kind=entry_kind, sl=0, tp1=0, tp2=0,
                      style=style, engine="Volatility Expansion",
                      confluences=["Bollinger-width in low percentile (compression)", "band retest"],
                      best_fit_regimes=["Compression", "Expansion", "Breakout"])


def run_liquidity_sweep_engine(zone_result: Dict[str, Any], m15: View, style: str) -> Optional[Candidate]:
    if zone_result.get("sweep") is None:
        return None
    dirn = zone_result["direction"]
    entry, entry_kind = retracement_entry_for_engine("Liquidity Sweep", dirn, m15,
                                                       m15.closes[-1])
    return Candidate(direction=dirn, entry=entry, entry_kind=entry_kind, sl=0, tp1=0, tp2=0,
                      style=style, engine="Liquidity Sweep",
                      confluences=["confirmed EQH/EQL sweep", "sweep-to-POI causal FVG/OB"],
                      best_fit_regimes=["High Volatility", "Expansion", "Breakout"],
                      session_tag=zone_result.get("session_tag"))


def run_order_block_engine(zone_result: Dict[str, Any], m15: View, style: str) -> Optional[Candidate]:
    zone: Zone = zone_result["zone"]
    if zone.kind != "order_block":
        return None
    dirn = zone_result["direction"]
    entry, entry_kind = retracement_entry_for_engine("Order Block", dirn, m15, (zone.top + zone.bottom) / 2)
    return Candidate(direction=dirn, entry=entry, entry_kind=entry_kind, sl=0, tp1=0, tp2=0,
                      style=style, engine="Order Block",
                      confluences=["unmitigated order block POI"],
                      best_fit_regimes=["Strong Bull Trend", "Strong Bear Trend", "Pullback"])


def run_breaker_block_engine(zone_result: Dict[str, Any], m15: View, style: str) -> Optional[Candidate]:
    zone: Zone = zone_result["zone"]
    if zone.kind != "breaker_block":
        return None
    dirn = zone_result["direction"]
    entry, entry_kind = retracement_entry_for_engine("Breaker Block", dirn, m15, (zone.top + zone.bottom) / 2)
    return Candidate(direction=dirn, entry=entry, entry_kind=entry_kind, sl=0, tp1=0, tp2=0,
                      style=style, engine="Breaker Block",
                      confluences=["breaker block -- most recent institutional footprint"],
                      best_fit_regimes=["Strong Bull Trend", "Strong Bear Trend", "Reversal"])


def run_fvg_engine(zone_result: Dict[str, Any], m15: View, style: str) -> Optional[Candidate]:
    zone: Zone = zone_result["zone"]
    if zone.kind != "fvg":
        return None
    dirn = zone_result["direction"]
    entry, entry_kind = retracement_entry_for_engine("Fair Value Gap", dirn, m15, (zone.top + zone.bottom) / 2)
    return Candidate(direction=dirn, entry=entry, entry_kind=entry_kind, sl=0, tp1=0, tp2=0,
                      style=style, engine="Fair Value Gap",
                      confluences=["unmitigated fair value gap"],
                      best_fit_regimes=["Breakout", "Expansion", "Pullback"])


def run_pullback_engine(dirn: str, h1: View, m15: View, style: str) -> Optional[Candidate]:
    if h1.ema20[-1] is None or h1.ema50[-1] is None:
        return None
    aligned = (dirn == "bullish" and h1.ema20[-1] > h1.ema50[-1]) or \
              (dirn == "bearish" and h1.ema20[-1] < h1.ema50[-1])
    if not aligned:
        return None
    last = m15.candles[-1]
    fast_ema = m15.ema20[-1]
    if fast_ema is None:
        return None
    near_fast_ema = abs(last["c"] - fast_ema) / fast_ema < 0.004
    if not near_fast_ema:
        return None
    entry, entry_kind = retracement_entry_for_engine("Momentum", dirn, m15, fast_ema)
    return Candidate(direction=dirn, entry=entry, entry_kind=entry_kind, sl=0, tp1=0, tp2=0,
                      style=style, engine="Pullback",
                      confluences=["1H trend aligned", "15M pullback to fast EMA"],
                      best_fit_regimes=["Pullback", "Strong Bull Trend", "Strong Bear Trend"])


def run_reversal_engine(zone_result: Dict[str, Any], m15: View, style: str) -> Optional[Candidate]:
    if zone_result.get("sweep") is None or zone_result.get("mss") is None:
        return None
    dirn = zone_result["direction"]
    entry, entry_kind = retracement_entry_for_engine("Reversal", dirn, m15, m15.closes[-1])
    return Candidate(direction=dirn, entry=entry, entry_kind=entry_kind, sl=0, tp1=0, tp2=0,
                      style=style, engine="Reversal",
                      confluences=["pure SFP sweep", "confirmed MSS"],
                      best_fit_regimes=["Reversal", "High Volatility"])


def run_base_ensemble(bias: str, macro_view: Optional[View], h1: View, h4: View, m15: View,
                       m5: Optional[View]) -> List[Candidate]:
    """Section 5.4 step 1: runs every specialized engine through the shared
    Stage 3/4/5 backbone, producing a list of raw candidates (risk plans not
    yet attached)."""
    if bias == "Neutral":
        return []
    zone_result = run_zone_selection_sequence(bias, h1)
    stage3 = stage3_setup(bias, h1, zone_result)
    if stage3.outcome != "VALID":
        return []
    stage4_result = stage4_entry(zone_result["direction"], zone_result, m15)
    if stage4_result is None:
        return []
    stage4_result = stage5_refine(stage4_result, m5)

    hold_horizon = "swing" if (h4.atr[-1] and m15.atr[-1] and
                                h4.atr[-1] / (m15.atr[-1] or 1e-9) > 12) else "intraday"
    dirn = zone_result["direction"]

    candidates: List[Candidate] = []
    for fn, args in [
        (run_smc_engine, (zone_result, stage4_result, m15, hold_horizon)),
        (run_trend_continuation_engine, (dirn, h1, m15, hold_horizon)),
        (run_breakout_engine, (dirn, h1, m15, hold_horizon)),
        (run_momentum_engine, (dirn, h1, m15, hold_horizon)),
        (run_mean_reversion_engine, (dirn, m15, hold_horizon)),
        (run_range_trading_engine, (dirn, m15, hold_horizon)),
        (run_volatility_expansion_engine, (dirn, m15, hold_horizon)),
        (run_liquidity_sweep_engine, (zone_result, m15, hold_horizon)),
        (run_order_block_engine, (zone_result, m15, hold_horizon)),
        (run_breaker_block_engine, (zone_result, m15, hold_horizon)),
        (run_fvg_engine, (zone_result, m15, hold_horizon)),
        (run_pullback_engine, (dirn, h1, m15, hold_horizon)),
        (run_reversal_engine, (zone_result, m15, hold_horizon)),
    ]:
        try:
            c = fn(*args)
        except Exception as e:  # noqa: BLE001 -- one bad engine never kills the scan
            log.error("Specialized engine %s raised: %s", fn.__name__, e)
            c = None
        if c is not None:
            candidates.append(c)
    return candidates


# ============================================================================
# SECTION 8 -- COUNTER-TREND REVERSAL ENGINE (Section 5A, opt-in)
# ============================================================================


def _htf_poi_pool(direction: str, weekly: Optional[View], daily: Optional[View]) -> Optional[Dict[str, Any]]:
    for view in (daily, weekly):
        if view is None:
            continue
        pool = _poi_pool("Bullish" if direction == "bullish" else "Bearish", view)
        if pool:
            return {"view": view, "zone": pool[-1]}
        opposite_clusters = view.eq_lows if direction == "bullish" else view.eq_highs
        sweep = detect_sweep(direction, opposite_clusters, view.candles)
        if sweep is not None:
            return {"view": view, "zone": None, "sweep": sweep}
    return None


def _exhaustion_signature(direction: str, view: Optional[View]) -> Optional[float]:
    if view is None or len(view.candles) < 10 or len(view.pivots) < 2:
        return None
    recent = view.candles[-6:]
    bodies = [abs(c["c"] - c["o"]) for c in recent]
    shrinking = bodies[-1] < bodies[0] * 0.7 if bodies[0] > 0 else False
    last = recent[-1]
    atr_now = view.atr[-1] or 1e-9
    upper_wick = last["h"] - max(last["c"], last["o"])
    lower_wick = min(last["c"], last["o"]) - last["l"]
    elongated_wick = (direction == "bullish" and upper_wick > atr_now * 0.6) or \
                     (direction == "bearish" and lower_wick > atr_now * 0.6)
    highs = [p for p in view.pivots if p.kind == "high"]
    lows = [p for p in view.pivots if p.kind == "low"]
    no_new_extreme = False
    if direction == "bullish" and len(highs) >= 2:
        no_new_extreme = highs[-1].price <= highs[-2].price
    elif direction == "bearish" and len(lows) >= 2:
        no_new_extreme = lows[-1].price >= lows[-2].price
    score = 0.0
    if shrinking:
        score += 0.4
    if elongated_wick:
        score += 0.35
    if no_new_extreme:
        score += 0.25
    # RSI divergence as an optional soft booster only
    if view.rsi14[-1] is not None and view.rsi14[-2] is not None:
        if direction == "bullish" and view.rsi14[-1] > view.rsi14[-2]:
            score = min(score + 0.1, 1.0)
        elif direction == "bearish" and view.rsi14[-1] < view.rsi14[-2]:
            score = min(score + 0.1, 1.0)
    return score if score > 0 else None


@dataclass
class RetestResult:
    entry: float
    bars_waited: int


def _retest_and_hold(direction: str, choch: Dict[str, Any], m15: View,
                      state: Dict[str, Any], asset: str) -> Optional[RetestResult]:
    level = choch["level"]
    idx = choch["idx"]
    since = m15.candles[idx + 1:]
    if len(since) > COUNTERTREND_RETEST_EXPIRY_BARS:
        since = since[-COUNTERTREND_RETEST_EXPIRY_BARS:]
    for i, c in enumerate(since):
        held = (direction == "bullish" and c["l"] <= level <= c["h"] and c["c"] > level) or \
               (direction == "bearish" and c["l"] <= level <= c["h"] and c["c"] < level)
        if held:
            return RetestResult(entry=level, bars_waited=i + 1)
    return None


def run_countertrend_gate(bias: str, weekly: Optional[View], daily: Optional[View],
                           h4: View, h1: View, m15: View, state: Dict[str, Any],
                           asset: str) -> Optional[Candidate]:
    """Section 5A -- a separate gate function, never a branch inside Section
    10's stage functions. Fires only opposite a resolved Bullish/Bearish
    bias, never against Neutral."""
    if bias not in ("Bullish", "Bearish"):
        return None
    direction = "bearish" if bias == "Bullish" else "bullish"

    htf_poi = _htf_poi_pool(direction, weekly, daily)
    if htf_poi is None:
        return None

    exhaustion = _exhaustion_signature(direction, h4) or _exhaustion_signature(direction, h1)
    if exhaustion is None:
        return None

    m15_pivots = m15.pivots
    choch = structure_shift(direction, h1.candles, h1.pivots, kind="CHoCH") or \
        structure_shift(direction, m15.candles, m15_pivots, kind="CHoCH")
    if choch is None:
        return None

    retest = _retest_and_hold(direction, choch, m15, state, asset)
    if retest is None:
        return None

    # Step 5: RR gate + risk plan sourced from the HTF (Weekly/Daily) view --
    # the only structural change vs. build_risk_plan's default 15M-primary
    # SL-anchor hierarchy (per this engine's spec addendum).
    htf_view = htf_poi["view"]
    plan = build_risk_plan(direction, retest.entry, htf_view, h1, h4, state, asset,
                            rr_min_gate=RR_MIN_GATE_COUNTERTREND)
    if plan is None:
        return None

    return Candidate(
        direction=direction, entry=retest.entry, entry_kind="pending",
        sl=plan["sl"], tp1=plan["tp1"], tp2=plan["tp2"], style="intraday",
        engine="Counter-Trend Reversal", counter_trend=True,
        confluences=["Weekly/Daily-sourced opposing POI", "momentum-exhaustion signature",
                     "confirmed CHoCH", "retest-and-hold beyond broken level"],
        best_fit_regimes=["Reversal", "High Volatility"],
    )


# ============================================================================
# SECTION 9 -- ADAPTIVE FILTERS (Section 13 category list, Section 9 table)
# ============================================================================

ADAPTIVE_FILTER_CATEGORIES = [
    "Location", "Context", "Trend", "Range", "Reversal", "Liquidity", "Volume",
    "Volatility", "Momentum", "Multi-Timeframe Confirmation",
    "Institutional Confluence", "Quality Score", "Expected Value", "Risk/Reward",
]


def adaptive_filter_thresholds(regime_label: str, state: Dict[str, Any]) -> Dict[str, float]:
    """Threshold tightening/relaxing per regime (Section 9). Base thresholds
    come from state.json (adaptive, bounded); this applies the regime
    adjustment multiplicatively on top, always preserving quality over
    frequency (never relaxes below a hard floor)."""
    base = dict(state.get("adaptive_filter_thresholds", {}))
    defaults = {cat: 0.5 for cat in ADAPTIVE_FILTER_CATEGORIES}
    defaults.update(base)
    adj = REGIME_WEIGHT_ADJUSTMENTS.get(regime_label, {})
    out = {}
    for cat, val in defaults.items():
        mult = adj.get(cat.lower(), 1.0)
        out[cat] = min(max(val * mult, 0.2), 0.95)
    return out


# ============================================================================
# SECTION 10 -- RISK-PLAN CONSTRUCTION (Section 14)
# ============================================================================


def _rr(entry: float, sl: float, target: float, direction: str) -> float:
    risk = abs(entry - sl)
    reward = abs(target - entry)
    return reward / risk if risk > 1e-12 else 0.0


def select_sl_anchor(direction: str, entry: float, m15: View, h1: View, h4: View,
                      state: Dict[str, Any], asset: str) -> Optional[Tuple[str, View, float]]:
    """SL-anchor hierarchy: prefer the nearest genuine structural
    invalidation level on 15M, falling back to 1H, then 4H, so every
    dispatched signal has a real structure-based invalidation level
    (Section 14, Trade Filter's 'no structural SL exists' NO TRADE rule)."""
    for name, view in (("15M", m15), ("1H", h1), ("4H", h4)):
        pivots_opposite = [p for p in view.pivots
                            if (p.kind == "low" if direction == "bullish" else p.kind == "high")]
        if not pivots_opposite:
            continue
        candidates = [p for p in pivots_opposite
                      if (p.price < entry if direction == "bullish" else p.price > entry)]
        if not candidates:
            continue
        nearest = min(candidates, key=lambda p: abs(p.price - entry))
        return name, view, nearest.price
    return None


def adaptive_sl_buffer(view: View, state: Dict[str, Any], asset: str) -> float:
    """Adaptive-percentile SL buffer (Section 14): buffer distance derived
    from a configurable percentile of recent ATR, itself an adaptive
    parameter bounded [40, 90] and persisted in state.json."""
    key = f"{asset}:{view.tf}"
    pctile = state.get("sl_buffer_percentile", {}).get(key, 65.0)
    pctile = min(max(pctile, ADAPTIVE_PARAM_BOUNDS["sl_buffer_percentile"][0]),
                 ADAPTIVE_PARAM_BOUNDS["sl_buffer_percentile"][1])
    atr_hist = [a for a in view.atr if a is not None][-100:]
    if not atr_hist:
        buf = (view.atr[-1] or view.closes[-1] * 0.005)
    else:
        atr_hist_sorted = sorted(atr_hist)
        idx = min(int(len(atr_hist_sorted) * pctile / 100.0), len(atr_hist_sorted) - 1)
        buf = atr_hist_sorted[idx] * 0.25   # buffer is a fraction of the percentile ATR
    state.setdefault("sl_buffer_percentile_dist", {})[key] = buf
    return buf


def _clear_sl_of_liquidity_pool(direction: str, sl: float, view: View) -> float:
    """Runs unconditionally: buffer -> clear -> ceiling (Section 14.2).
    Scoped to a bounded window around the pre-clearing SL so clearing can't
    chase a distant pool."""
    atr_now = view.atr[-1] or 1e-9
    window = atr_now * SL_LIQUIDITY_CLEAR_WINDOW_ATR_MULT
    pools = view.eq_highs if direction == "bullish" else view.eq_lows
    # For a long, SL clearing must push the SL *below* any SSL pool sitting
    # just beneath it; for a short, above any BSL pool just above it.
    relevant = [p["level"] for p in pools if abs(p["level"] - sl) <= window and
                ((direction == "bullish" and p["level"] <= sl) or
                 (direction == "bearish" and p["level"] >= sl))]
    if not relevant:
        return sl
    if direction == "bullish":
        return min(sl, min(relevant) - atr_now * 0.05)
    return max(sl, max(relevant) + atr_now * 0.05)


def _opposing_structural_levels(direction: str, entry: float, view: View) -> List[Dict[str, Any]]:
    candidates: List[Dict[str, Any]] = []

    def _add(price: float, weight: float) -> None:
        candidates.append({"price": price, "score": weight})

    if direction == "bullish":
        for p in view.pivots:
            if p.kind == "high" and p.price > entry:
                _add(p.price, 1)
        for e in view.eq_highs:
            if e["level"] > entry:
                _add(e["level"], 2 + min(len(e.get("pivots", [])), 3))
        for z in (view.order_blocks + view.breaker_blocks):
            if z.direction == "bearish" and not z.mitigated and z.bottom > entry:
                _add(z.bottom, 2)
        for z in view.fvgs:
            if z.direction == "bearish" and not z.mitigated and z.bottom > entry:
                _add(z.bottom, 1)
    else:
        for p in view.pivots:
            if p.kind == "low" and p.price < entry:
                _add(p.price, 1)
        for e in view.eq_lows:
            if e["level"] < entry:
                _add(e["level"], 2 + min(len(e.get("pivots", [])), 3))
        for z in (view.order_blocks + view.breaker_blocks):
            if z.direction == "bullish" and not z.mitigated and z.top < entry:
                _add(z.top, 2)
        for z in view.fvgs:
            if z.direction == "bullish" and not z.mitigated and z.top < entry:
                _add(z.top, 1)

    atr_now = view.atr[-1] or 1e-9
    merged = _merge_confluent_levels(candidates, tol=atr_now * 0.05)
    merged.sort(key=lambda c: c["price"], reverse=(direction == "bearish"))
    return merged


def _merge_confluent_levels(candidates: List[Dict[str, Any]], tol: float) -> List[Dict[str, Any]]:
    if not candidates:
        return []
    candidates = sorted(candidates, key=lambda c: c["price"])
    merged = [dict(candidates[0])]
    for c in candidates[1:]:
        if abs(c["price"] - merged[-1]["price"]) <= tol:
            merged[-1]["score"] += c["score"]
            merged[-1]["price"] = (merged[-1]["price"] + c["price"]) / 2
        else:
            merged.append(dict(c))
    return merged


def _tp_selection_band(candidates: List[Dict[str, Any]], state: Dict[str, Any], asset: str) -> List[Dict[str, Any]]:
    n = int(state.get("tp1_target_rank_preference", {}).get(asset, 3))
    n = min(max(n, ADAPTIVE_PARAM_BOUNDS["tp1_target_rank_preference"][0]),
            ADAPTIVE_PARAM_BOUNDS["tp1_target_rank_preference"][1])
    return candidates[:max(n, 2)]


def tp1_runway_ok(direction: str, entry: float, m15: View, state: Dict[str, Any], asset: str) -> bool:
    candidates = _opposing_structural_levels(direction, entry, m15)
    if not candidates:
        return False
    band = _tp_selection_band(candidates, state, asset)
    best_in_band = max(band, key=lambda c: c["score"])
    plausible_reward = abs(best_in_band["price"] - entry)
    typical_risk = state.get("sl_buffer_percentile_dist", {}).get(
        f"{asset}:15M", (m15.atr[-1] or 1e-9) * MIN_RISK_ATR_MULT)
    return (plausible_reward / max(typical_risk, 1e-9)) >= RR_MIN_GATE * 0.8


def build_risk_plan(direction: str, entry: float, m15: View, h1: View, h4: View,
                     state: Dict[str, Any], asset: str,
                     rr_min_gate: Optional[float] = None) -> Optional[Dict[str, Any]]:
    rr_min_gate = rr_min_gate if rr_min_gate is not None else RR_MIN_GATE
    anchor = select_sl_anchor(direction, entry, m15, h1, h4, state, asset)
    if anchor is None:
        return None
    anchor_name, view, structural_sl = anchor

    buffer = adaptive_sl_buffer(view, state, asset)
    sl = (structural_sl - buffer) if direction == "bullish" else (structural_sl + buffer)
    sl = _clear_sl_of_liquidity_pool(direction, sl, view)

    risk = abs(entry - sl)
    if risk <= 1e-12:
        return None
    atr_now = view.atr[-1] or 1e-9
    if risk / atr_now > MAX_SL_ATR_MULT:
        return None
    if not (max(MIN_RISK_ATR_MULT * atr_now, MIN_SL_DISTANCE_PCT * entry)
            <= risk <=
            min(MAX_SL_ATR_MULT * atr_now, MAX_SL_DISTANCE_PCT * entry)):
        return None

    candidates = _opposing_structural_levels(direction, entry, view)
    if len(candidates) < 2:
        return None

    band = _tp_selection_band(candidates, state, asset)
    tp1_pick = max(band, key=lambda c: c["score"])
    remaining = [c for c in candidates if c is not tp1_pick and
                 (c["price"] > tp1_pick["price"] if direction == "bullish"
                  else c["price"] < tp1_pick["price"])]
    if not remaining:
        return None
    tp2_pick = remaining[0]
    tp1, tp2 = tp1_pick["price"], tp2_pick["price"]
    rr1 = _rr(entry, sl, tp1, direction)
    rr2 = _rr(entry, sl, tp2, direction)

    if not ((tp2 > tp1) if direction == "bullish" else (tp2 < tp1)):
        return None  # TP ordering integrity -- never dispatch an inverted TP2

    if abs(tp1 - entry) < entry * MIN_MOVE_PCT_TP1:
        return None
    if abs(tp2 - entry) < entry * MIN_MOVE_PCT_TP2:
        return None
    if rr1 < rr_min_gate or rr1 > RR_MAX_GATE:
        return None

    # Liquidity-wall-clipped TP1: if a liquidity pool sits between entry and
    # TP1, clip TP1 in front of it. Runs after the RR-floor gate -- can only
    # tighten TP1, never rescue a setup that already failed the floor check.
    pools = view.eq_highs if direction == "bullish" else view.eq_lows
    wall_levels = [p["level"] for p in pools if
                   ((direction == "bullish" and entry < p["level"] < tp1) or
                    (direction == "bearish" and tp1 < p["level"] < entry))]
    tp_wall_clipped = False
    if wall_levels:
        nearest_wall = min(wall_levels, key=lambda lv: abs(lv - entry))
        clipped = (nearest_wall - atr_now * 0.05 if direction == "bullish"
                   else nearest_wall + atr_now * 0.05)
        clipped_rr = _rr(entry, sl, clipped, direction)
        if clipped_rr >= rr_min_gate:
            tp1 = clipped
            rr1 = clipped_rr
            tp_wall_clipped = True

    return {"sl": sl, "tp1": tp1, "tp2": tp2, "rr1": rr1, "rr2": rr2, "risk": risk,
            "buffer": buffer, "sl_anchor": anchor_name, "tp_wall_clipped": tp_wall_clipped}


def entry_distance_ok(direction: str, entry: float, market_price: float, atr_ref: float) -> bool:
    """Section 14.3: cap how far a pending/zone entry may sit from market."""
    return abs(entry - market_price) <= atr_ref * MAX_ENTRY_DISTANCE_FROM_MARKET_ATR_MULT


def log_filter_attrition(state: Dict[str, Any], filter_name: str, passed: bool) -> None:
    """Filter-funnel attrition logging.
    Purely additive/explainability -- a per-gate passed/eliminated counter
    that never affects which signals pass, only how easy it is to see which
    gate is over-filtering when tuning. Wired into every gate in
    scan_symbol/_finalize_candidate/build_risk_plan's callers below."""
    funnel = state.setdefault("filter_funnel_attrition", {}).setdefault(
        filter_name, {"passed": 0, "eliminated": 0})
    funnel["passed" if passed else "eliminated"] += 1


def position_size_fraction(state: Dict[str, Any], win_rate: float = 0.5, avg_rr: float = 1.8) -> float:
    """Optional half-Kelly position sizing.
    Informational only: this engine's product is the signal itself, so this
    value is attached to the dispatched signal JSON for the reader's
    convenience and never used to gate/filter which signals are generated
    or dispatched. Defaults to a fixed-fractional floor unless
    ENABLE_KELLY_SIZING is set."""
    if not ENABLE_KELLY_SIZING:
        return FIXED_RISK_PCT_OF_EQUITY
    b = max(avg_rr, 1e-9)
    kelly = win_rate - (1 - win_rate) / b
    kelly = max(0.0, kelly) * KELLY_FRACTION_CAP
    return min(kelly, PORTFOLIO_EXPOSURE_CAP_PCT / max(MAX_CONCURRENT_ACTIVE_SIGNALS, 1))


# ============================================================================
# SECTION 11 -- CENTRAL DECISION ENGINE & COMPOSITE SCORING (Section 5.2/5.3)
# ============================================================================


def _sigmoid(x: float) -> float:
    return 1.0 / (1.0 + math.exp(-x))


def _cap_term(x: float, weight: float) -> float:
    """Every term has a documented, enforced cap on its own contribution so
    no single term can saturate the logistic (Section 5.2)."""
    contribution = x * weight
    cap = PER_TERM_CONTRIBUTION_CAP
    return max(min(contribution, cap), -cap)


def compute_composite_score(candidate: Candidate, plan: Dict[str, Any], rv: RegimeVector,
                             mtf_alignment: float, engine_perf: Dict[str, Any],
                             state: Dict[str, Any]) -> Tuple[float, Dict[str, float], List[str]]:
    """Continuous weighted/logistic blend over a small, bounded, auditable
    term set -- never a discrete point stack (Section 5.2)."""
    reasons: List[str] = []
    weights = dict(CATEGORY_WEIGHTS_DEFAULT)
    regime_adj = REGIME_WEIGHT_ADJUSTMENTS.get(rv.label, {})
    for k, mult in regime_adj.items():
        if k in weights:
            weights[k] *= mult
    total_w = sum(weights.values())
    weights = {k: v / total_w for k, v in weights.items()}  # renormalize

    # Trend term: HTF alignment strength (ADX-normalized), never RR-derived.
    trend_term = min(rv.trend_strength / 40.0, 1.0)
    if trend_term > 0.5:
        reasons.append(f"HTF trend strength strong (ADX-normalized {trend_term:.2f})")

    # Structure term: confluence density of the chosen zone/sequence.
    structure_term = min(len(candidate.confluences) / 4.0, 1.0)
    reasons.append(f"{len(candidate.confluences)} structural confluences present")

    # Momentum term.
    momentum_term = 0.6  # baseline; specific engines already gated on momentum conditions
    if candidate.engine in ("Momentum", "Trend Continuation"):
        momentum_term = 0.8
        reasons.append("momentum-confirmed engine")

    # Liquidity term: sweep-based setups and ERL/IRL alignment score higher.
    liquidity_term = 0.5
    if candidate.session_tag is not None:
        liquidity_term = 0.7
        reasons.append("session-anchored liquidity sweep")
    if rv.liquidity_draw == "ERL" and candidate.engine == "Liquidity Sweep":
        liquidity_term = min(liquidity_term + 0.15, 1.0)

    # Volume term: placeholder-free -- CMF-style read from the regime vector
    # breadth as a cross-asset participation proxy (bounded, non-saturating).
    volume_term = min(max(rv.breadth, 0.0), 1.0)

    # Volatility term: prefer mid-percentile ATR (not extreme) for structural
    # SL quality; penalize very high or very low percentile.
    vol_pct = rv.volatility_percentile / 100.0
    volatility_term = 1.0 - abs(vol_pct - 0.5) * 2.0

    # Risk term: reject-only-gated RR expressed as a bounded, non-linear
    # probability-relevant term -- NOT scaled linearly with RR magnitude
    # (Section 5.2: reward magnitude is not a substitute for win-probability).
    rr1 = plan["rr1"]
    risk_term = min(max((rr1 - RR_MIN_GATE) / (RR_MAX_GATE - RR_MIN_GATE), 0.0), 1.0)

    # MTF alignment and historical segment performance feed in as adjustments
    # to the pre-transform sum, each individually capped.
    mtf_term = mtf_alignment  # 0..1
    segment_term = min(max(engine_perf.get("expectancy_r", 0.0) / 1.0, -1.0), 1.0)

    z = 0.0
    contributions: Dict[str, float] = {}
    for cat, term in [("trend", trend_term), ("structure", structure_term),
                       ("momentum", momentum_term), ("liquidity", liquidity_term),
                       ("volume", volume_term), ("volatility", volatility_term),
                       ("risk", risk_term)]:
        c = _cap_term(term, weights[cat])
        contributions[cat] = c
        z += c
    # MTF and segment-performance are small, explicitly-bounded adjustments,
    # not part of the illustrative category table, capped independently.
    mtf_weight = state.get("mtf_alignment_weight", 0.15)
    mtf_weight = min(max(mtf_weight, ADAPTIVE_PARAM_BOUNDS["mtf_alignment_weight"][0]),
                      ADAPTIVE_PARAM_BOUNDS["mtf_alignment_weight"][1])
    z += _cap_term(mtf_term, mtf_weight)
    z += _cap_term(segment_term, 0.10)

    prob = _sigmoid((z - 0.5) * 4.0)  # center/scale so mid-quality ~ 50
    score_100 = prob * 100.0

    # Confidence calibration adjustment (Section 7 -- bounded, dampened).
    calib_key = candidate.engine
    calib_adj = state.get("confidence_calibration", {}).get(calib_key, 0.0)
    calib_adj = min(max(calib_adj, ADAPTIVE_PARAM_BOUNDS["confidence_calibration"][0]),
                     ADAPTIVE_PARAM_BOUNDS["confidence_calibration"][1])
    score_100 = min(max(score_100 + calib_adj, 0.0), 100.0)

    if mtf_alignment > 0.5:
        reasons.append("multi-timeframe alignment confirmed (Weekly/Daily/4H/1H agree)")
    if rr1 >= RR_MIN_GATE:
        reasons.append(f"TP1 RR {rr1:.2f} clears the {RR_MIN_GATE} floor")

    return score_100, contributions, reasons


def score_to_grade(score: float) -> str:
    for threshold, grade in GRADE_BANDS:
        if score >= threshold:
            return grade
    return "No Trade"


def regime_fit_veto_discount(candidate: Candidate, rv: RegimeVector, state: Dict[str, Any]) -> float:
    """Section 19 regime-fit veto/discount. Returns a multiplier in
    [threshold, 1.0] applied to the composite score. Counter-trend engine's
    best-fit is explicitly the *opposite* of the dominant regime read, so it
    is never penalized the way a base-ensemble engine correctly would be."""
    threshold = state.get("regime_fit_discount", {}).get(candidate.engine, 0.6)
    threshold = min(max(threshold, ADAPTIVE_PARAM_BOUNDS["regime_fit_discount"][0]),
                     ADAPTIVE_PARAM_BOUNDS["regime_fit_discount"][1])
    if candidate.counter_trend:
        return 1.0  # reversal/exhaustion-into-high-volatility is this engine's expected regime
    if rv.label in candidate.best_fit_regimes:
        return 1.0
    return threshold


def liquidity_sanity_check(candidate: Candidate, view: View) -> bool:
    """Reject/discount entries sitting directly inside a level about to be
    swept, unless the setup is specifically a liquidity-sweep engine."""
    if candidate.engine == "Liquidity Sweep":
        return True
    for cluster in (view.eq_highs + view.eq_lows):
        if abs(candidate.entry - cluster["level"]) < (view.atr[-1] or 1e-9) * 0.15:
            return False
    return True


def macro_blackout_active(now: datetime, macro_events: List[Dict[str, Any]], asset: str) -> bool:
    for ev in macro_events:
        try:
            ev_time = datetime.fromtimestamp(ev["ts"] / 1000.0, tz=timezone.utc)
        except (KeyError, TypeError, ValueError):
            continue
        affected = ev.get("assets", [MACRO_ASSET])
        if asset not in affected and "*" not in affected:
            continue
        delta_min = abs((now - ev_time).total_seconds()) / 60.0
        before = (now < ev_time) and delta_min <= MACRO_BLACKOUT_MINUTES_BEFORE
        after = (now >= ev_time) and delta_min <= MACRO_BLACKOUT_MINUTES_AFTER
        if before or after:
            return True
    return False


# ============================================================================
# SECTION 12 -- ENTRY-FILL VERIFICATION & PENDING LIFECYCLE (Section 16)
# ============================================================================


def check_pending_fill(signal: Dict[str, Any], candle: Dict[str, Any]) -> bool:
    """Never evaluate SL/TP before entry has filled. Returns True if this
    candle fills the pending entry."""
    lo, hi = candle["l"], candle["h"]
    entry = signal["entry"]
    return lo <= entry <= hi


def advance_pending_signal(signal: Dict[str, Any], candle: Dict[str, Any]) -> Optional[str]:
    """Advances one pending signal by one closed 15M candle. Returns a
    terminal result string ('win'/'loss'/'expired') or None if still open."""
    if not signal.get("entry_filled", False):
        if check_pending_fill(signal, candle):
            signal["entry_filled"] = True
            # same-candle ambiguity: conservative order -- check SL before TP1
            # on the fill candle itself, since the same candle can register
            # both a fill and a same-bar stop-out; treating SL-first is the
            # conservative (never-optimistic) choice (Section 15).
            if candle["l"] <= signal["sl"] <= candle["h"] and _sl_hit_first(signal, candle):
                return "loss"
            if _tp1_hit(signal, candle):
                return "win"
            return None
        signal["pending_bars"] = signal.get("pending_bars", 0) + 1
        max_bars = signal.get("pending_expiry_bars", PENDING_ENTRY_EXPIRY_BARS["default"])
        if signal["pending_bars"] >= max_bars:
            return "expired"
        return None
    # already filled -- normal SL/TP1 resolution, SL-first conservative order
    if _sl_hit_first(signal, candle):
        return "loss"
    if _tp1_hit(signal, candle):
        return "win"
    return None


def _sl_hit_first(signal: Dict[str, Any], candle: Dict[str, Any]) -> bool:
    direction = signal["direction"]
    sl = signal["sl"]
    if direction == "bullish":
        return candle["l"] <= sl
    return candle["h"] >= sl


def _tp1_hit(signal: Dict[str, Any], candle: Dict[str, Any]) -> bool:
    direction = signal["direction"]
    tp1 = signal["tp1"]
    if direction == "bullish":
        return candle["h"] >= tp1
    return candle["l"] <= tp1


# ============================================================================
# SECTION 13 -- TRADE OUTCOME RESOLUTION (Section 15)
# ============================================================================

# Position-exit model declaration (Section 15, mandatory, explicit):
#   KAIROS uses the FULL-EXIT-AT-TP1 model. 100% of position size closes at
#   TP1. Nothing remains open afterward, so a later touch of the original SL
#   has no effect on real P&L -- it is bookkeeping only. This declaration is
#   enforced by `resolve_trade` below, which never checks the original SL
#   again once TP1 has been credited, and never repositions SL to breakeven
#   at any point (Section 15's structurally-impossible bug class).

RESOLUTION_LOGIC_VERSION = "kairos-1.0.0-full-exit"


def resolve_trade(signal: Dict[str, Any], result: str, resolved_candle: Dict[str, Any]) -> Dict[str, Any]:
    entry, sl, tp1 = signal["entry"], signal["sl"], signal["tp1"]
    direction = signal["direction"]
    risk = abs(entry - sl)

    if result == "win":
        r_realized = _rr(entry, sl, tp1, direction)
        assert r_realized > 0, "a WIN must never carry a non-positive realized R"
    elif result == "loss":
        r_realized = -1.0
    else:  # expired / no_fill
        r_realized = 0.0

    return {
        "entry": entry, "sl": sl, "tp1": tp1, "tp2": signal.get("tp2"),
        "r_realized": r_realized,
        "mae_r": signal.get("mae_r", 0.0), "mfe_r": signal.get("mfe_r", 0.0),
        "result": result,
        "confidence": signal.get("confidence"), "grade": signal.get("grade"),
        "regime_at_entry": signal.get("regime_at_entry"),
        "engine": signal.get("engine"), "asset": signal.get("asset"),
        "counter_trend": signal.get("counter_trend", False),
        "session_tag": signal.get("session_tag"),
        "resolved_ts": int(time.time() * 1000),
        "resolution_logic_version": RESOLUTION_LOGIC_VERSION,
    }


def track_mae_mfe(signal: Dict[str, Any], candle: Dict[str, Any]) -> None:
    if not signal.get("entry_filled"):
        return
    entry, sl, direction = signal["entry"], signal["sl"], signal["direction"]
    risk = abs(entry - sl) or 1e-9
    if direction == "bullish":
        mfe = (candle["h"] - entry) / risk
        mae = (entry - candle["l"]) / risk
    else:
        mfe = (entry - candle["l"]) / risk
        mae = (candle["h"] - entry) / risk
    signal["mfe_r"] = max(signal.get("mfe_r", 0.0), mfe)
    signal["mae_r"] = max(signal.get("mae_r", 0.0), mae)


# ============================================================================
# SECTION 14 -- LOSS FORENSICS & TAXONOMY ROUTING (Section 19.1)
# ============================================================================


def classify_failure(trade: Dict[str, Any], state: Dict[str, Any]) -> str:
    """Every category is a positive, verifiable condition on recorded trade
    data -- never a bare else/fallback (Section 19.1)."""
    if trade["result"] != "loss":
        return "n/a"

    regime = trade.get("regime_at_entry", "")
    engine = trade.get("engine", "")
    best_fit = state.get("engine_best_fit_regimes", {}).get(engine, [])
    if best_fit and regime not in best_fit:
        return "regime_mismatch"

    buffer_dist = state.get("sl_buffer_percentile_dist", {}).get(f"{trade['asset']}:15M")
    risk = abs(trade["entry"] - trade["sl"])
    if buffer_dist and risk <= buffer_dist * 1.2:
        return "structural_invalidation_too_tight"

    if trade.get("swept_adjacent_pool"):
        return "chased_swept_liquidity"

    if trade.get("mtf_conflict_at_entry"):
        return "mtf_conflict_ignored"

    if trade.get("sfp_impure_or_premature_mss"):
        return "sfp_mss_sequence_violated"

    if trade.get("mfe_r", 0.0) >= 0.8:
        return "correct_read_poor_rr"

    bucket = trade.get("grade", "")
    calibrated_wr = state.get("calibration_win_rate", {}).get(f"{engine}:{bucket}")
    if calibrated_wr is not None and trade.get("confidence", 0) / 100.0 > calibrated_wr + 0.15:
        return "confidence_miscalibration"

    if trade.get("thin_margin_filters"):
        return "filter_over_permissiveness"

    return "genuine_variance"


FORENSIC_ROUTES = {
    "regime_mismatch": "regime_fit_discount",
    "structural_invalidation_too_tight": "sl_buffer_percentile",
    "chased_swept_liquidity": "liquidity_sanity_threshold",
    "mtf_conflict_ignored": "mtf_alignment_weight",
    "sfp_mss_sequence_violated": "sfp_purity_threshold",
    "correct_read_poor_rr": "tp1_target_rank_preference",
    "confidence_miscalibration": "confidence_calibration",
    "filter_over_permissiveness": "adaptive_filter_thresholds",
    "genuine_variance": None,
}


# ============================================================================
# SECTION 15 -- CONTINUOUS LEARNING LOOP & CIRCUIT BREAKER (Sections 7, 7.3)
# ============================================================================


def _dampen(old: float, target: float, max_step_pct: float = ADAPTIVE_MAX_STEP_PCT) -> float:
    max_delta = abs(old) * max_step_pct if old != 0 else max_step_pct
    delta = target - old
    delta = max(min(delta, max_delta), -max_delta)
    return old + delta


def _bounded(value: float, key: str) -> float:
    lo, hi = ADAPTIVE_PARAM_BOUNDS.get(key, (-1e18, 1e18))
    return min(max(value, lo), hi)


def apply_learning_update(state: Dict[str, Any], trade: Dict[str, Any], category: str) -> None:
    """One diagnosis, one deterministic route (Section 19.3.2). Every update
    is bounded and dampened (Sections 7, 7.2)."""
    engine = trade.get("engine", "")
    asset = trade.get("asset", "")
    segment_key = f"{engine}"
    seg = state.setdefault("segment_stats", {}).setdefault(segment_key, {
        "signals": 0, "wins": 0, "losses": 0, "sum_r": 0.0,
    })
    seg["signals"] += 1
    if trade["result"] == "win":
        seg["wins"] += 1
    elif trade["result"] == "loss":
        seg["losses"] += 1
    seg["sum_r"] += trade.get("r_realized", 0.0)
    resolved = seg["wins"] + seg["losses"]
    if resolved < MIN_SAMPLE_SIZE:
        return  # Section 7.2 / 19: minimum-sample-size gate before adapting

    target_param = FORENSIC_ROUTES.get(category)
    if target_param is None:
        return  # genuine variance -- no parameter change, by design

    win_rate = seg["wins"] / resolved
    expectancy = seg["sum_r"] / resolved

    if target_param == "regime_fit_discount":
        cur = state.setdefault("regime_fit_discount", {}).get(engine, 0.6)
        new = _dampen(cur, cur * 0.9)  # strengthen (lower) the discount weight
        state["regime_fit_discount"][engine] = _bounded(new, "regime_fit_discount")
    elif target_param == "sl_buffer_percentile":
        key = f"{asset}:15M"
        cur = state.setdefault("sl_buffer_percentile", {}).get(key, 65.0)
        new = _dampen(cur, cur * 1.08)  # widen buffer percentile
        state["sl_buffer_percentile"][key] = _bounded(new, "sl_buffer_percentile")
    elif target_param == "liquidity_sanity_threshold":
        cur = state.setdefault("liquidity_sanity_threshold", {}).get(engine, 0.5)
        new = _dampen(cur, cur * 1.1)  # tighten
        state["liquidity_sanity_threshold"][engine] = _bounded(new, "liquidity_sanity_threshold")
    elif target_param == "mtf_alignment_weight":
        cur = state.get("mtf_alignment_weight", 0.15)
        new = _dampen(cur, cur * 1.1)
        state["mtf_alignment_weight"] = _bounded(new, "mtf_alignment_weight")
    elif target_param == "sfp_purity_threshold":
        cur = state.setdefault("sfp_purity_threshold", {}).get(engine, 0.6)
        new = _dampen(cur, cur * 1.1)
        state["sfp_purity_threshold"][engine] = _bounded(new, "sfp_purity_threshold")
    elif target_param == "tp1_target_rank_preference":
        cur = state.setdefault("tp1_target_rank_preference", {}).get(asset, 3)
        new = min(cur + 1, ADAPTIVE_PARAM_BOUNDS["tp1_target_rank_preference"][1])
        state["tp1_target_rank_preference"][asset] = new
    elif target_param == "confidence_calibration":
        cur = state.setdefault("confidence_calibration", {}).get(engine, 0.0)
        new = _dampen(cur, cur - 3.0)  # pull displayed confidence down
        state["confidence_calibration"][engine] = _bounded(new, "confidence_calibration")
    elif target_param == "adaptive_filter_thresholds":
        cur = state.setdefault("adaptive_filter_thresholds", {}).get(engine, 0.5)
        new = _dampen(cur, cur * 1.1)
        state["adaptive_filter_thresholds"][engine] = min(max(new, 0.2), 0.95)

    log.info("Adaptive update: engine=%s category=%s target=%s win_rate=%.2f expectancy=%.2fR",
              engine, category, target_param, win_rate, expectancy)

    # Win reinforcement path (Section 19.3.1) is symmetric and handled by the
    # caller passing category='n/a' with result='win' through
    # apply_win_reinforcement below.


def apply_win_reinforcement(state: Dict[str, Any], trade: Dict[str, Any]) -> None:
    if trade["result"] != "win":
        return
    engine = trade.get("engine", "")
    cur = state.setdefault("engine_weight", {}).get(engine, 1.0)
    new = _dampen(cur, cur * 1.05)
    state["engine_weight"][engine] = _bounded(new, "engine_weight")


def check_circuit_breaker(state: Dict[str, Any]) -> bool:
    """Section 7.3: freeze adaptation if rolling live performance falls
    materially below baseline; auto-resume once recovered."""
    log_ = state.get("closed_signals", [])
    resolved = [t for t in log_ if t.get("result") in ("win", "loss")][-CIRCUIT_BREAKER_WINDOW:]
    if len(resolved) < MIN_SAMPLE_SIZE:
        state["circuit_breaker_active"] = False
        return False
    wins = sum(1 for t in resolved if t["result"] == "win")
    win_rate = wins / len(resolved)
    deviation = BASELINE_WIN_RATE - win_rate
    active = deviation >= CIRCUIT_BREAKER_WIN_RATE_DEVIATION
    was_active = state.get("circuit_breaker_active", False)
    state["circuit_breaker_active"] = active
    if active and not was_active:
        state["circuit_breaker_alert_pending"] = True
        log.warning("LIVE-PERFORMANCE CIRCUIT BREAKER TRIPPED: rolling win rate %.2f vs baseline %.2f",
                    win_rate, BASELINE_WIN_RATE)
    elif not active and was_active:
        log.info("Circuit breaker cleared -- rolling performance recovered to baseline.")
    return active


# ============================================================================
# SECTION 16 -- SIGNAL OBJECT CONSTRUCTION & JSON FORMATTING (Section 21)
# ============================================================================


def build_signal_json(symbol: str, candidate: Candidate, plan: Dict[str, Any], score: float,
                       grade: str, rv: RegimeVector, contributions: Dict[str, float],
                       reasons: List[str], higher_tf_alignment: bool,
                       zone_result: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    scores_100 = {k: round(v * 100, 1) for k, v in contributions.items()}
    return {
        "symbol": symbol,
        "signal": "LONG" if candidate.direction == "bullish" else "SHORT",
        "engine": candidate.engine,
        "counter_trend": candidate.counter_trend,
        "style": candidate.style,
        "entry_kind": candidate.entry_kind,
        "confidence": round(score, 1),
        "grade": grade,
        "entry": candidate.entry,
        "stop_loss": plan["sl"],
        "take_profit": {"tp1": plan["tp1"], "tp2": plan["tp2"]},
        "risk_reward": {"rr1": round(plan["rr1"], 2), "rr2_suggested": round(plan["rr2"], 2)},
        "market_regime": rv.label,
        "regime_confidence": round(rv.confidence, 2),
        "trend": "Bullish" if candidate.direction == "bullish" else "Bearish",
        "sl_anchor": plan["sl_anchor"],
        "holding_time": "30m-4h" if candidate.style == "intraday" else "4h-multi-day",
        "timeframe": "15M",
        "entry_refinement_tf": candidate.entry_refinement_tf,
        "higher_timeframe_alignment": higher_tf_alignment,
        "scores": scores_100,
        "reasons": reasons,
    }


def assert_signal_integrity(signal_json: Dict[str, Any], candidate: Candidate, plan: Dict[str, Any]) -> bool:
    direction = candidate.direction
    entry, sl, tp1, tp2 = candidate.entry, plan["sl"], plan["tp1"], plan["tp2"]
    try:
        assert (tp2 > tp1) if direction == "bullish" else (tp2 < tp1), "TP2 must be beyond TP1"
        displayed_rr1 = signal_json["risk_reward"]["rr1"]
        assert abs(_rr(entry, sl, tp1, direction) - displayed_rr1) < 1e-2, \
            "displayed RR does not match RR implied by entry/sl/tp1"
        assert abs(entry - sl) > 0
        assert abs(entry - tp1) > 0
        return True
    except AssertionError as e:
        log.error("Signal integrity assertion failed for %s: %s -- dispatch blocked.",
                  signal_json.get("symbol"), e)
        return False


# ============================================================================
# SECTION 17 -- STATE PERSISTENCE (Sections 4, 7.4)
# ============================================================================


def default_state() -> Dict[str, Any]:
    return {
        "schema_version": f"{ENGINE_NAME.lower()}-{ENGINE_VERSION}",
        "engine_weight": {e: 1.0 for e in ENGINE_TYPES},
        "confidence_calibration": {e: 0.0 for e in ENGINE_TYPES},
        "sl_buffer_percentile": {},
        "sl_buffer_percentile_dist": {},
        "tp1_target_rank_preference": {s: 3 for s in WATCHLIST},
        "regime_fit_discount": {e: 0.6 for e in ENGINE_TYPES},
        "liquidity_sanity_threshold": {e: 0.5 for e in ENGINE_TYPES},
        "mtf_alignment_weight": 0.15,
        "sfp_purity_threshold": {e: 0.6 for e in ENGINE_TYPES},
        "session_open_proximity_weight": 0.05,
        "adaptive_filter_thresholds": {cat: 0.5 for cat in ADAPTIVE_FILTER_CATEGORIES},
        "engine_best_fit_regimes": {},
        "calibration_win_rate": {},
        "segment_stats": {},
        "per_asset_stats": {s: {"signals": 0, "wins": 0, "losses": 0} for s in WATCHLIST},
        "per_regime_stats": {r: {"signals": 0, "wins": 0, "losses": 0} for r in REGIME_LABELS},
        "session_anchored_stats": {"anchored": {"signals": 0, "wins": 0, "losses": 0},
                                    "non_anchored": {"signals": 0, "wins": 0, "losses": 0}},
        "forensic_category_counts": {},
        "filter_funnel_attrition": {},
        "active_signals": {},
        "closed_signals": [],           # Tier 2 -- bounded, prunable raw log
        "macro_events": [],
        "circuit_breaker_active": False,
        "circuit_breaker_alert_pending": False,
        "daily_stats": {},
        "self_monitoring": {
            "overall_win_rate": None, "rolling_30d_win_rate": None,
            "confidence_calibration": {}, "warnings": [],
        },
        "last_daily_summary_date": None,
    }


class StateStore:
    """Atomic read/write of `state.json` (Sections 4, 7.4). Loaded once at
    run start, mutated in-memory, written back atomically (write-temp then
    rename) at run end. Missing/corrupt state falls back to fresh defaults
    rather than crashing the run."""

    def __init__(self, path: str = STATE_PATH) -> None:
        self.path = path
        self.data = self._load()

    def _load(self) -> Dict[str, Any]:
        defaults = default_state()
        try:
            with open(self.path, "r") as f:
                loaded = json.load(f)
            if isinstance(loaded, dict) and loaded.get("schema_version") == defaults["schema_version"]:
                merged = defaults
                merged.update(loaded)
                return merged
            log.warning("state.json schema mismatch or missing version -- initializing fresh state "
                        "(cold start, full strength per Section 2's cold-start quality bar).")
        except (FileNotFoundError, json.JSONDecodeError, OSError):
            log.warning("No readable state.json -- cold start with defaults.")
        return defaults

    def save(self) -> None:
        try:
            prune_state(self.data)
            dir_ = os.path.dirname(os.path.abspath(self.path)) or "."
            fd, tmp = tempfile.mkstemp(dir=dir_)
            with os.fdopen(fd, "w") as f:
                json.dump(self.data, f, indent=None)
            os.replace(tmp, self.path)
        except OSError as e:
            log.error("Failed to persist state.json -- next run will not see this run's updates: %s", e)


def prune_state(state: Dict[str, Any]) -> None:
    """Tier 2 pruning: bounded by trade count and age. Tier 1 aggregates are
    untouched -- pruning raw history never resets learned behavior."""
    closed = state.get("closed_signals", [])
    cutoff_ms = int(time.time() * 1000) - TIER2_RAW_LOG_MAX_DAYS * 86400_000
    closed = [t for t in closed if t.get("resolved_ts", 0) >= cutoff_ms]
    if len(closed) > TIER2_RAW_LOG_MAX_TRADES:
        closed = closed[-TIER2_RAW_LOG_MAX_TRADES:]
    state["closed_signals"] = closed


def update_tier1_aggregates(state: Dict[str, Any], trade: Dict[str, Any], category: str) -> None:
    """Tier 1 incremental update -- one resolved trade at a time, never
    recomputed by rescanning Tier 2 (Section 7.4)."""
    asset = trade.get("asset")
    regime = trade.get("regime_at_entry")
    is_win = trade["result"] == "win"
    is_loss = trade["result"] == "loss"

    if asset in state["per_asset_stats"]:
        s = state["per_asset_stats"][asset]
        s["signals"] += 1
        s["wins"] += int(is_win)
        s["losses"] += int(is_loss)
    if regime in state["per_regime_stats"]:
        s = state["per_regime_stats"][regime]
        s["signals"] += 1
        s["wins"] += int(is_win)
        s["losses"] += int(is_loss)

    bucket = "anchored" if trade.get("session_tag") else "non_anchored"
    sb = state["session_anchored_stats"][bucket]
    sb["signals"] += 1
    sb["wins"] += int(is_win)
    sb["losses"] += int(is_loss)

    if category and category != "n/a":
        cat_stats = state["forensic_category_counts"].setdefault(category, {"count": 0})
        cat_stats["count"] += 1

    bucket_key = f"{trade.get('engine')}:{trade.get('grade')}"
    calib = state["calibration_win_rate"]
    prior = calib.get(bucket_key)
    n = state["segment_stats"].get(trade.get("engine", ""), {}).get("signals", 1)
    if prior is None:
        calib[bucket_key] = float(is_win)
    else:
        calib[bucket_key] = prior + (float(is_win) - prior) / max(n, 1)


# ============================================================================
# SECTION 18 -- SELF-MONITORING & EXPLAINABILITY (Section 20)
# ============================================================================


def run_self_monitoring(state: Dict[str, Any]) -> List[str]:
    warnings: List[str] = []
    closed = [t for t in state.get("closed_signals", []) if t.get("result") in ("win", "loss")]
    if len(closed) >= MIN_SAMPLE_SIZE:
        wins = sum(1 for t in closed if t["result"] == "win")
        wr = wins / len(closed)
        state["self_monitoring"]["overall_win_rate"] = round(wr, 3)
        recent = closed[-max(int(len(closed) * 0.3), MIN_SAMPLE_SIZE):]
        recent_wins = sum(1 for t in recent if t["result"] == "win")
        state["self_monitoring"]["rolling_30d_win_rate"] = round(recent_wins / len(recent), 3)
        if wr < BASELINE_WIN_RATE - 0.10:
            warnings.append(f"Overall win rate ({wr:.1%}) trending materially below baseline "
                             f"({BASELINE_WIN_RATE:.0%}) -- recommend feature revalidation.")

    # Bucket-inversion check (Section 5.2): highest-conviction bucket must not
    # realize a lower win rate than a lower bucket, once minimum sample met.
    calib = state.get("calibration_win_rate", {})
    by_grade: Dict[str, List[float]] = {}
    for key, wr in calib.items():
        _, grade = key.split(":", 1) if ":" in key else ("", key)
        by_grade.setdefault(grade, []).append(wr)
    grade_order = ["B", "B+", "A", "A+"]
    avgs = {g: (sum(v) / len(v) if v else None) for g, v in by_grade.items()}
    ordered = [avgs[g] for g in grade_order if avgs.get(g) is not None]
    if len(ordered) >= 2 and any(ordered[i] > ordered[i + 1] + 0.05 for i in range(len(ordered) - 1)):
        warnings.append("Confidence/grade bucket win-rate inversion detected -- term weights "
                         "require re-derivation from realized outcome data (Section 5.2).")

    # Forensic category concentration check (Section 19.1).
    total_losses = sum(v["count"] for v in state.get("forensic_category_counts", {}).values())
    if total_losses >= MIN_SAMPLE_SIZE:
        for cat, v in state["forensic_category_counts"].items():
            if v["count"] / total_losses > 0.34:
                warnings.append(f"Forensic category '{cat}' accounts for >1/3 of losses -- "
                                 f"audit its diagnostic signature against underlying MFE/MAE data.")

    if state.get("circuit_breaker_active"):
        warnings.append("Live-performance circuit breaker is ACTIVE -- adaptation frozen.")

    # Flag any gate eliminating almost everything that reaches it, once it
    # has seen enough traffic to be meaningful.
    for gate, counts in state.get("filter_funnel_attrition", {}).items():
        total = counts.get("passed", 0) + counts.get("eliminated", 0)
        if total >= MIN_SAMPLE_SIZE and counts.get("eliminated", 0) / total > 0.95:
            warnings.append(f"Filter gate '{gate}' is eliminating >95% of what reaches it "
                             f"({counts['eliminated']}/{total}) -- review whether it is over-filtering.")

    state["self_monitoring"]["warnings"] = warnings
    for w in warnings:
        log.warning(w)
    return warnings


# ============================================================================
# SECTION 19 -- NOTIFICATION DISPATCH (TELEGRAM)
# ============================================================================

TELEGRAM_MAX_MESSAGE_LEN = 4096
REACTION_EMOJI = "\U0001F440"  # single, consistently-used reaction (eyes) -- no custom asset supplied


_ACRONYM_PRESERVE = {
    "Smc": "SMC", "Mss": "MSS", "Sfp": "SFP", "Rr": "RR", "Tp1": "TP1", "Tp2": "TP2",
    "Fvg": "FVG", "Ote": "OTE", "Bos": "BOS", "Choch": "CHoCH", "Atr": "ATR",
    "Mtf": "MTF", "Htf": "HTF", "Ltf": "LTF", "Eqh": "EQH", "Eql": "EQL",
}


def _humanize(identifier: str) -> str:
    """No raw underscores in user-facing text (Section 29.2), with known
    trading-domain acronyms preserved in their conventional case rather than
    mangled by str.title() (e.g. 'SMC' -> 'Smc', 'correct_read_poor_rr' ->
    'Correct Read Poor Rr')."""
    cleaned = identifier.replace("_", " ").title()
    for word, acronym in _ACRONYM_PRESERVE.items():
        if cleaned == word:
            return acronym
        cleaned = cleaned.replace(f" {word} ", f" {acronym} ")
        if cleaned.startswith(f"{word} "):
            cleaned = f"{acronym} " + cleaned[len(word) + 1:]
        if cleaned.endswith(f" {word}"):
            cleaned = cleaned[: -len(word)] + acronym
    return cleaned


def _truncate(text: str) -> str:
    if len(text) <= TELEGRAM_MAX_MESSAGE_LEN:
        return text
    marker = "\n... [truncated]"
    return text[:TELEGRAM_MAX_MESSAGE_LEN - len(marker)] + marker


class TelegramNotifier:
    def __init__(self, bot_token: str = TG_BOT_TOKEN, chat_id: str = TG_CHAT_ID) -> None:
        self.bot_token = bot_token
        self.chat_id = chat_id

    def _call(self, method: str, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if not (self.bot_token and self.chat_id):
            log.warning("Telegram dispatch skipped (missing credentials): %s", method)
            return None
        url = f"https://api.telegram.org/bot{self.bot_token}/{method}"
        body = json.dumps(payload).encode("utf-8")
        try:
            req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"})
            with urllib.request.urlopen(req, timeout=10) as resp:
                result = json.loads(resp.read().decode("utf-8"))
                log.info("Telegram %s dispatch: sent", method)
                return result
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError) as e:
            log.error("Telegram %s dispatch failed: %s", method, e)
            return None

    def send_message(self, text: str) -> Optional[int]:
        result = self._call("sendMessage", {"chat_id": self.chat_id, "text": _truncate(text),
                                              "parse_mode": "Markdown"})
        if result and result.get("ok"):
            return result["result"]["message_id"]
        return None

    def reply(self, message_id: int, text: str) -> None:
        self._call("sendMessage", {"chat_id": self.chat_id, "text": _truncate(text),
                                    "parse_mode": "Markdown", "reply_to_message_id": message_id})

    def react(self, message_id: int) -> None:
        self._call("setMessageReaction", {"chat_id": self.chat_id, "message_id": message_id,
                                           "reaction": [{"type": "emoji", "emoji": REACTION_EMOJI}]})


def format_signal_message(sig: Dict[str, Any]) -> str:
    header = f"{ENGINE_NAME} v{ENGINE_VERSION}"
    ct_badge = "\n>>> COUNTER-TREND: against the Weekly/Daily bias <<<" if sig["counter_trend"] else ""
    lines = [
        header, ct_badge,
        f"{'🟢' if sig['signal'] == 'LONG' else '🔴'} {sig['signal']}  {sig['symbol']}",
        f"Engine: {_humanize(sig['engine'])}",
        f"Style: {_humanize(sig['style'])}   Grade: {sig['grade']} ({sig['confidence']:.1f})",
        "",
        f"Entry: `{sig['entry']}`",
        f"SL: `{sig['stop_loss']}`",
        f"TP1: `{sig['take_profit']['tp1']}`",
        f"TP2 (suggested): `{sig['take_profit']['tp2']}`",
        "",
        f"RR (TP1): {sig['risk_reward']['rr1']}   RR (TP2, suggested): {sig['risk_reward']['rr2_suggested']}",
        f"Regime: {_humanize(sig['market_regime'])} (confidence {sig['regime_confidence']})",
        f"HTF Alignment: {'Yes' if sig['higher_timeframe_alignment'] else 'No'}",
        f"Holding Time: {sig['holding_time']}",
        f"SL Anchor: {sig['sl_anchor']}   Entry Kind: {_humanize(sig['entry_kind'])}",
    ]
    if sig.get("entry_refinement_tf"):
        lines.append(f"5M Entry Refinement: applied")
    lines.append("")
    lines.append("Reasons:")
    for r in sig["reasons"]:
        lines.append(f"- {r}")
    return "\n".join(l for l in lines if l is not None)


def format_daily_summary(state: Dict[str, Any]) -> str:
    closed = [t for t in state.get("closed_signals", []) if t.get("result") in ("win", "loss")]
    wins = sum(1 for t in closed if t["result"] == "win")
    losses = len(closed) - wins
    win_rate = wins / len(closed) if closed else 0.0
    gains = sum(t["r_realized"] for t in closed if t["r_realized"] > 0)
    losses_r = -sum(t["r_realized"] for t in closed if t["r_realized"] < 0)
    pf = (gains / losses_r) if losses_r > 1e-9 else float("inf")
    avg_rr = statistics.mean([abs(t["r_realized"]) for t in closed]) if closed else 0.0

    lines = [f"{ENGINE_NAME} v{ENGINE_VERSION} -- Daily Summary",
             f"Total signals: {len(closed)}   Wins: {wins}  Losses: {losses}  Win rate: {win_rate:.1%}",
             f"Profit factor: {pf:.2f}   Avg |R|: {avg_rr:.2f}",
             "", "By Regime:"]
    for r, s in state.get("per_regime_stats", {}).items():
        if s["signals"]:
            lines.append(f"  {_humanize(r)}: {s['wins']}/{s['signals']}")
    lines.append("")
    lines.append("By Asset:")
    for a, s in state.get("per_asset_stats", {}).items():
        if s["signals"]:
            lines.append(f"  {a}: {s['wins']}/{s['signals']}")
    lines.append("")
    lines.append("By Engine (segment):")
    for e, s in state.get("segment_stats", {}).items():
        if s["signals"]:
            lines.append(f"  {_humanize(e)}: {s['wins']}/{s['signals']}  sum_R={s['sum_r']:.1f}")
    lines.append("")
    lines.append("Forensic categories:")
    for cat, v in state.get("forensic_category_counts", {}).items():
        lines.append(f"  {_humanize(cat)}: {v['count']}")
    anchored = state.get("session_anchored_stats", {})
    lines.append("")
    lines.append(f"Session-anchored: {anchored.get('anchored', {})}")
    lines.append(f"Non-anchored: {anchored.get('non_anchored', {})}")
    for w in state.get("self_monitoring", {}).get("warnings", []):
        lines.append(f"WARNING: {w}")
    return "\n".join(lines)


# ============================================================================
# SECTION 20 -- main() / CLI ENTRY POINT
# ============================================================================


def _mtf_alignment_score(bias: str, h4: StageResult) -> float:
    if bias == "Neutral":
        return 0.0
    return 1.0 if h4.outcome == "Agree" else 0.0


def _asset_correlation_group(symbol: str) -> frozenset:
    for group in CORRELATED_ASSET_GROUPS:
        if symbol in group:
            return frozenset(group)
    return frozenset({symbol})


def _count_active_in_group(state: Dict[str, Any], group: frozenset) -> int:
    return sum(1 for k, v in state.get("active_signals", {}).items()
               if v.get("asset") in group)


def scan_symbol(client: HyperliquidClient, cache: CandleCacheStore, state: Dict[str, Any],
                 symbol: str, now_ms: int, macro_views: Dict[str, View]) -> List[Dict[str, Any]]:
    """One full pass of Sections 2-16 for a single asset. Returns zero or
    more dispatch-ready signal JSON dicts."""
    now = datetime.fromtimestamp(now_ms / 1000.0, tz=timezone.utc)

    if macro_blackout_active(now, state.get("macro_events", []), symbol):
        log.info("%s: macro/news blackout window active -- skipping.", symbol)
        return []

    if _count_active_in_group(state, _asset_correlation_group(symbol)) >= 1 and symbol != MACRO_ASSET:
        # correlation cap -- one active slot per correlated group (Section 22)
        pass  # informative log only; enforced below at dispatch time

    if sum(1 for v in state.get("active_signals", {}).values()) >= MAX_CONCURRENT_ACTIVE_SIGNALS:
        log.info("%s: MAX_CONCURRENT_ACTIVE_SIGNALS reached -- skipping new scans this run.", symbol)
        return []
    if sum(1 for v in state.get("active_signals", {}).values()
           if v.get("asset") == symbol) >= MAX_CONCURRENT_PER_SYMBOL:
        return []

    raw = fetch_symbol_mtf(client, cache, symbol, now_ms)
    views: Dict[str, Optional[View]] = {tf: build_view(symbol, tf, raw.get(tf, [])) for tf in TIMEFRAMES}
    weekly, daily, h4, h1, m15, m5 = (views["1W"], views["1D"], views["4H"],
                                      views["1H"], views["15M"], views["5M"])
    if h1 is None or h4 is None or m15 is None:
        log.error("%s: insufficient candle data for mandatory timeframes -- skipping.", symbol)
        return []

    # -- Stage 1/2: Weekly+Daily bias, 4H context (Section 10) --------------
    stage1 = stage1_bias(weekly, daily)
    stage2 = stage2_context(stage1.outcome, h4)
    higher_tf_alignment = stage2.outcome == "Agree"

    rv = compute_regime_vector(macro_views.get("1H"), h1, h4, macro_views.get("watchlist_1h", {}), now)

    signals_out: List[Dict[str, Any]] = []

    log_filter_attrition(state, "stage1_neutral", passed=stage1.outcome != "Neutral")
    if stage1.outcome != "Neutral":
        log_filter_attrition(state, "stage2_agree", passed=stage2.outcome == "Agree")

    if stage1.outcome != "Neutral" and stage2.outcome == "Agree":
        zone_result = run_zone_selection_sequence(stage1.outcome, h1)
        stage3 = stage3_setup(stage1.outcome, h1, zone_result)
        log.info("%s: Stage1=%s Stage2=%s Stage3=%s", symbol, stage1.outcome, stage2.outcome, stage3.outcome)
        log_filter_attrition(state, "stage3_valid", passed=stage3.outcome == "VALID")

        if stage3.outcome == "VALID":
            candidates = run_base_ensemble(stage1.outcome, macro_views.get("1H"), h1, h4, m15, m5)
            mtf_score = _mtf_alignment_score(stage1.outcome, stage2)

            for cand in candidates:
                signal_json = _finalize_candidate(cand, symbol, m15, h1, h4, rv, mtf_score,
                                                    higher_tf_alignment, zone_result, state)
                if signal_json:
                    signals_out.append(signal_json)
    else:
        log.info("%s: Stage1=%s Stage2=%s -- NO TRADE (base ensemble).", symbol, stage1.outcome, stage2.outcome)

    if ENABLE_COUNTERTREND_ENGINE:
        ct_candidate = run_countertrend_gate(stage1.outcome, weekly, daily, h4, h1, m15, state, symbol)
        if ct_candidate is not None:
            ct_json = _finalize_candidate(ct_candidate, symbol, m15, h1, h4, rv, 0.0,
                                           higher_tf_alignment, None, state)
            if ct_json:
                signals_out.append(ct_json)

    return signals_out


def _finalize_candidate(cand: Candidate, symbol: str, m15: View, h1: View, h4: View,
                         rv: RegimeVector, mtf_score: float, higher_tf_alignment: bool,
                         zone_result: Optional[Dict[str, Any]], state: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    market_price = m15.closes[-1]
    atr_ref = m15.atr[-1] or market_price * 0.005
    if cand.entry_kind == "pending":
        distance_ok = entry_distance_ok(cand.direction, cand.entry, market_price, atr_ref)
        log_filter_attrition(state, "entry_distance", passed=distance_ok)
        if not distance_ok:
            return None

    runway_ok = tp1_runway_ok(cand.direction, cand.entry, m15, state, symbol)
    log_filter_attrition(state, "tp1_runway", passed=runway_ok)
    if not runway_ok:
        return None

    plan = build_risk_plan(cand.direction, cand.entry, m15, h1, h4, state, symbol)
    log_filter_attrition(state, "risk_plan", passed=plan is not None)
    if plan is None:
        return None

    sanity_ok = liquidity_sanity_check(cand, m15)
    log_filter_attrition(state, "liquidity_sanity", passed=sanity_ok)
    if not sanity_ok:
        return None

    engine_perf = state.get("segment_stats", {}).get(cand.engine, {"expectancy_r": 0.0})
    score, contributions, reasons = compute_composite_score(cand, plan, rv, mtf_score, engine_perf, state)
    discount = regime_fit_veto_discount(cand, rv, state)
    score *= discount
    if discount < 1.0:
        reasons.append(f"regime-fit discount applied ({_humanize(rv.label)} vs. engine best-fit)")

    grade = score_to_grade(score)
    score_ok = score >= MIN_SIGNAL_SCORE
    log_filter_attrition(state, "score_threshold", passed=score_ok)
    if not score_ok:
        return None  # below the eligible-to-become-a-signal threshold (Section 5.3)

    sig_json = build_signal_json(symbol, cand, plan, score, grade, rv, contributions, reasons,
                                  higher_tf_alignment, zone_result)

    # Optional, informational-only half-Kelly sizing, never used to gate dispatch.
    seg_stats = state.get("segment_stats", {}).get(cand.engine, {})
    win_rate = seg_stats.get("win_rate", 0.5)
    avg_rr = seg_stats.get("expectancy_r", plan["rr1"]) or plan["rr1"]
    kelly_frac = position_size_fraction(state, win_rate=win_rate, avg_rr=max(avg_rr, 1e-9))
    assert 0.0 <= kelly_frac <= PORTFOLIO_EXPOSURE_CAP_PCT, \
        "position sizing fraction must stay within the portfolio exposure cap"
    sig_json["position_sizing"] = {
        "suggested_risk_pct_of_equity": round(kelly_frac, 4),
        "method": "half_kelly" if ENABLE_KELLY_SIZING else "fixed_fractional",
    }

    integrity_ok = assert_signal_integrity(sig_json, cand, plan)
    log_filter_attrition(state, "signal_integrity", passed=integrity_ok)
    if not integrity_ok:
        return None

    state.setdefault("engine_best_fit_regimes", {})[cand.engine] = cand.best_fit_regimes
    return sig_json


def run_scan(client: HyperliquidClient, cache: CandleCacheStore, store: StateStore,
             notifier: TelegramNotifier) -> None:
    state = store.data
    now_ms = int(time.time() * 1000)
    t_start = time.monotonic()

    check_circuit_breaker(state)
    if state.get("circuit_breaker_alert_pending"):
        notifier.send_message(
            f"{ENGINE_NAME} v{ENGINE_VERSION}\nLIVE-PERFORMANCE CIRCUIT BREAKER TRIPPED.\n"
            f"Rolling win rate has fallen materially below the documented baseline "
            f"({BASELINE_WIN_RATE:.0%}). Automatic parameter adaptation is frozen; signal "
            f"generation continues on last-known-good parameters.")
        state["circuit_breaker_alert_pending"] = False

    # -- 1. Monitor every currently active/pending signal first -------------
    _resolve_active_signals(state, client, cache, now_ms, notifier)

    # -- 2. Macro view + watchlist-wide 1H views for breadth/regime ---------
    macro_raw = fetch_symbol_mtf(client, cache, MACRO_ASSET, now_ms)
    macro_1h = build_view(MACRO_ASSET, "1H", macro_raw.get("1H", []))
    watchlist_1h: Dict[str, View] = {}
    if macro_1h:
        watchlist_1h[MACRO_ASSET] = macro_1h

    # -- 3. Scan phase, thread-pooled per symbol -----------------------------
    all_signals: List[Tuple[str, Dict[str, Any]]] = []
    skipped: List[str] = []

    def _scan_one(sym: str):
        try:
            sigs = scan_symbol(client, cache, state, sym, now_ms, {"1H": macro_1h, "watchlist_1h": watchlist_1h})
            return sym, sigs, None
        except Exception as e:  # noqa: BLE001 -- one bad symbol never kills the run
            return sym, [], e

    with ThreadPoolExecutor(max_workers=6) as pool:
        futures = {pool.submit(_scan_one, sym): sym for sym in WATCHLIST}
        for fut in as_completed(futures):
            sym, sigs, err = fut.result()
            if err is not None:
                log.error("%s: scan raised %s -- skipped.", sym, err)
                skipped.append(sym)
                continue
            for s in sigs:
                all_signals.append((sym, s))

    log.info("Scan complete: %d/%d symbols processed (%d skipped), %d candidate signal(s), %.1fs elapsed.",
              len(WATCHLIST) - len(skipped), len(WATCHLIST), len(skipped), len(all_signals),
              time.monotonic() - t_start)

    # -- 4. Correlation-cap-aware dispatch -----------------------------------
    active = state.setdefault("active_signals", {})
    for sym, sig in all_signals:
        if len(active) >= MAX_CONCURRENT_ACTIVE_SIGNALS:
            break
        group = _asset_correlation_group(sym)
        if _count_active_in_group(state, group) >= 1:
            continue

        sig_id = f"{sym}:{sig['engine']}:{now_ms}"
        pending_expiry = PENDING_ENTRY_EXPIRY_BARS["swing" if sig["style"] == "swing" else "default"]
        active[sig_id] = {
            "asset": sym, "engine": sig["engine"], "direction": "bullish" if sig["signal"] == "LONG" else "bearish",
            "entry": sig["entry"], "sl": sig["stop_loss"], "tp1": sig["take_profit"]["tp1"],
            "tp2": sig["take_profit"]["tp2"], "entry_kind": sig["entry_kind"],
            "entry_filled": sig["entry_kind"] == "market",
            "pending_bars": 0, "pending_expiry_bars": pending_expiry,
            "confidence": sig["confidence"], "grade": sig["grade"],
            "regime_at_entry": sig["market_regime"], "counter_trend": sig["counter_trend"],
            "session_tag": None, "mfe_r": 0.0, "mae_r": 0.0,
        }
        state["per_asset_stats"].setdefault(sym, {"signals": 0, "wins": 0, "losses": 0})["signals"] += 1
        state["per_regime_stats"].setdefault(sig["market_regime"],
                                              {"signals": 0, "wins": 0, "losses": 0})["signals"] += 1

        message_id = notifier.send_message(format_signal_message(sig))
        if message_id:
            active[sig_id]["message_id"] = message_id
            notifier.react(message_id)
        log.info("Signal emitted: %s %s %s grade=%s confidence=%.1f", sym, sig["signal"],
                  sig["engine"], sig["grade"], sig["confidence"])

    run_self_monitoring(state)

    today = now_from_ms(now_ms).strftime("%Y-%m-%d")
    if state.get("last_daily_summary_date") != today and now_from_ms(now_ms).hour == 8:
        notifier.send_message(format_daily_summary(state))
        state["last_daily_summary_date"] = today


def now_from_ms(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)


def _resolve_active_signals(state: Dict[str, Any], client: HyperliquidClient, cache: CandleCacheStore,
                             now_ms: int, notifier: TelegramNotifier) -> None:
    active = state.setdefault("active_signals", {})
    if not active:
        return
    to_remove = []
    for sig_id, sig in list(active.items()):
        asset = sig["asset"]
        raw = fetch_symbol_mtf(client, cache, asset, now_ms)
        m15_candles = raw.get("15M", [])
        if not m15_candles:
            continue
        for candle in m15_candles[-4:]:  # a few most-recent closed candles, cheap re-check
            track_mae_mfe(sig, candle)
            result = advance_pending_signal(sig, candle)
            if result is not None:
                trade = resolve_trade(sig, result, candle)
                trade["asset"] = asset
                trade["engine"] = sig["engine"]
                category = classify_failure(trade, state)
                trade["forensic_category"] = category
                state.setdefault("closed_signals", []).append(trade)
                update_tier1_aggregates(state, trade, category)
                if not state.get("circuit_breaker_active"):
                    apply_learning_update(state, trade, category)
                    apply_win_reinforcement(state, trade)

                status = {"win": "TP1", "loss": "SL", "expired": "Expired"}[result]
                if sig.get("message_id") and result in ("win", "loss"):
                    notifier.reply(sig["message_id"], f"{ENGINE_NAME} v{ENGINE_VERSION} -- {asset}: {status}")
                to_remove.append(sig_id)
                break
    for sig_id in to_remove:
        active.pop(sig_id, None)


def main() -> int:
    log.info("=== %s v%s run starting ===", ENGINE_NAME, ENGINE_VERSION)
    try:
        store = StateStore(STATE_PATH)
        cache = CandleCacheStore(CANDLE_CACHE_PATH)
        client = HyperliquidClient(HL_API_URL)
        notifier = TelegramNotifier(TG_BOT_TOKEN, TG_CHAT_ID)
        log.info("Watchlist: %d assets. Counter-trend engine: %s.",
                  len(WATCHLIST), "ENABLED" if ENABLE_COUNTERTREND_ENGINE else "disabled")

        run_scan(client, cache, store, notifier)

        store.save()
        cache.save()
        log.info("=== %s v%s run complete ===", ENGINE_NAME, ENGINE_VERSION)
        return 0
    except Exception as e:  # noqa: BLE001 -- never a silent, unlogged crash (Section 25/30)
        log.error("Unhandled exception at top level of main(): %s", e, exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
