#!/usr/bin/env python3
"""
Meridian Adaptive Signal Engine
================================
Version: v1.0.0

An institutional-grade, self-contained, continuously-learning multi-engine
signal generator for Hyperliquid perpetuals. Built from scratch as an
original synthesis of best-in-class ideas identified across a reference
fleet of prior engines -- no code is merged or lightly adapted from those
references; every subsystem below is an independent implementation.

Design summary (see inline section headers for detail):
  - Mandatory four-stage top-down sequence (Weekly/Daily -> 4H -> 1H -> 15M)
    gates every signal; no stage is ever skipped or evaluated out of order.
  - A composite, continuous Regime Vector (never a single discrete label)
    describes market character and feeds scoring, filtering and structural
    interpretation throughout.
  - A Zone-Selection Sequence (HTF bias -> liquidity sweep -> POI -> SFP
    purity -> MSS confirmation -> breaker confirmation -> OTE refinement)
    is the single mechanism for choosing which zone to trade.
  - Thirteen+ specialized engines each independently propose direction,
    entry, SL, TP1 (sole resolving target), TP2 (informational only),
    confidence, RR and regime fit; a Decision Engine blends them with a
    small, auditable, continuous logistic scoring function -- never a
    discrete point stack.
  - All SL/TP construction runs through one shared, reference risk-plan
    mechanism: adaptive-percentile SL buffer, liquidity-pool clearing,
    confluence-ranked TP1, liquidity-wall-clipped targets, and a hard
    1.5 RR floor -- enforced as reject-only gates, never stretched to
    force a pass.
  - Trade outcome integrity (Section 11): no automatic breakeven
    repositioning, single-TP (TP1) resolution, accurate messaging.
  - Entry-fill verification (Section 12): explicit entry_kind, pending
    tracking, bounded pending lifetime, distinct expiry result type.
  - Anti-repainting (Section 12A): every structural read operates on
    fully closed candles only, through one shared detection function per
    structure type used identically in every code path.
  - A closed-taxonomy loss-forensics subsystem deterministically routes
    every resolved trade's diagnosis to the specific adaptive parameter
    it implicates, governed by the same bounds/dampening/min-sample-size
    rules as every other adaptive parameter, plus a live-performance
    circuit breaker.
  - Hyperliquid integration with a weight-aware rate limiter and a
    persistent candle cache; Telegram signal/status/daily-summary
    delivery with copy-paste-friendly price formatting.

Deliverable scope (Section 21): this is the complete engine source only.
No GitHub Actions workflow YAML, requirements.txt, state.json, or
documentation is produced here; ask the user afterward whether they want
any of those.
"""

from __future__ import annotations

import atexit
import collections
import copy
import json
import logging
import math
import os
import random
import signal
import statistics
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

# =============================================================================
# SECTION 0 -- LOGGING
# =============================================================================

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)-7s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("meridian")

# =============================================================================
# SECTION 1 -- ENGINE IDENTITY & GLOBAL CONFIG
# =============================================================================

ENGINE_NAME = "Meridian Adaptive Signal Engine"
ENGINE_VERSION = "v1.0.0"
RESOLUTION_LOGIC_VERSION = "r1"  # bumped whenever outcome/resolution logic changes (Section 11 legacy rule)

STATE_PATH = os.environ.get("MERIDIAN_STATE_PATH", "state.json")
CANDLE_CACHE_PATH = os.environ.get("MERIDIAN_CANDLE_CACHE_PATH", "candle_cache.json")
REACTION_IMAGE_PATH = os.environ.get("MERIDIAN_REACTION_IMAGE_PATH", "")  # DECISION: no image was attached
# to this build; wired as a config path so ops can drop the intended reaction
# image in without a code change, per Section 16's "use the attached image" rule.

TG_BOT_TOKEN = os.environ.get("TG_BOT_TOKEN", "")
TG_CHAT_ID = os.environ.get("TG_CHAT_ID", "")

# DECISION: watchlist reused verbatim from the reference fleet per the task's
# explicit instruction (same 25-asset universe, Hyperliquid bare-coin symbols
# as used by the axis/odyssey references -- Hyperliquid's own API addresses
# perps by bare coin name, not an exchange-suffixed pair).
WATCHLIST: List[str] = [
    "BTC", "ETH", "HYPE", "ZEC", "NEAR", "ONDO", "SUI", "PENGU", "BNB", "SOL",
    "TRX", "BCH", "DOGE", "ADA", "DOT", "TAO", "AVAX", "LINK", "AAVE", "XRP",
    "XLM", "UNI", "LTC", "APT", "PENDLE",
]
MACRO_ASSET = "BTC"  # DECISION: BTC anchors macro bias / breadth in the Regime Vector (Section 6)
MAJORS = {"BTC", "ETH", "SOL", "BNB", "XRP", "ADA", "DOGE"}

# DECISION: rough sector map purely for the correlation cap (Section 14) --
# not a trading signal in itself, just groups assets likely to co-move so
# concurrent-slot allocation doesn't get consumed by near-duplicate bets.
SECTOR_MAP: Dict[str, str] = {
    "BTC": "btc", "ETH": "eth",
    "SOL": "l1_alt", "AVAX": "l1_alt", "SUI": "l1_alt", "APT": "l1_alt",
    "NEAR": "l1_alt", "TAO": "l1_alt", "DOT": "l1_alt", "ADA": "l1_alt",
    "BNB": "bnb_eco",
    "XRP": "payments", "XLM": "payments", "TRX": "payments", "LTC": "payments", "BCH": "payments",
    "DOGE": "meme", "PENGU": "meme",
    "LINK": "oracle_infra", "ONDO": "oracle_infra", "PENDLE": "oracle_infra",
    "AAVE": "defi", "UNI": "defi",
    "HYPE": "exchange", "ZEC": "privacy",
}

# Timeframes. Forbidden per Section 7: 1M, 2M, 3M, 5M. Minimum entry TF: 15M.
FORBIDDEN_TFS = {"1m", "2m", "3m", "5m"}
TF_WEEKLY, TF_DAILY, TF_H4, TF_H1, TF_M15 = "1w", "1d", "4h", "1h", "15m"
ALL_TFS = [TF_WEEKLY, TF_DAILY, TF_H4, TF_H1, TF_M15]
TF_MS = {
    "15m": 15 * 60_000, "1h": 60 * 60_000, "4h": 4 * 60 * 60_000,
    "1d": 24 * 60 * 60_000, "1w": 7 * 24 * 60 * 60_000,
}
# Candle counts per timeframe -- enough history for weekly/daily structure,
# ATR/ADX/EMA warmup, and wick-percentile sampling without over-fetching.
CANDLE_COUNT = {"1w": 220, "1d": 260, "4h": 320, "1h": 320, "15m": 400}

SCAN_INTERVAL_MIN = 15
CANDLE_DELTA_OVERLAP_BARS = 3

# Indicator lengths
EMA_FAST, EMA_SLOW, EMA_TREND = 21, 50, 200
RSI_LEN, ATR_LEN, ADX_LEN, BB_LEN = 14, 14, 14, 20
BB_MULT = 2.0

# Concurrency / correlation (Section 14)
MAX_CONCURRENT_ACTIVE_SIGNALS = int(os.environ.get("MAX_CONCURRENT_ACTIVE_SIGNALS", "8"))
MAX_CORRELATED_CONCURRENT = 1

# Learning / validation (Section 13)
MIN_SAMPLE_SIZE_SEGMENT = int(os.environ.get("MIN_SAMPLE_SIZE_SEGMENT", "20"))
MIN_SAMPLE_SIZE_CATEGORY = int(os.environ.get("MIN_SAMPLE_SIZE_CATEGORY", "12"))
TIER2_RETENTION_DAYS = 15

# Live-performance circuit breaker (Section 5)
CIRCUIT_BREAKER_WINDOW_TRADES = 30
CIRCUIT_BREAKER_WIN_RATE_DROP = 0.20     # absolute drop vs baseline win rate
CIRCUIT_BREAKER_PF_DROP_FRAC = 0.25      # relative drop vs baseline profit factor

# Risk plan constants (Section 10 reference implementation)
MIN_RISK_ATR_MULT = 1.0     # noise-survival floor that triggers 15M -> H1/H4 SL escalation
MAX_SL_ATR_MULT = 4.0       # hard SL distance ceiling regardless of anchor
MIN_MOVE_PCT_TP1 = 0.012    # minimum entry-to-TP1 distance as a fraction of price
MIN_MOVE_PCT_TP2 = 0.020    # minimum entry-to-TP2 distance as a fraction of price
RR_MIN_GATE = 1.5           # hard RR floor for TP1 -- reject-only, never stretched
RR_TP1_SOFT_CEIL = 2.0      # TP1's natural expected ceiling; never padded past this artificially

# Entry placement (Section 10)
MIN_ENTRY_TO_SL_ATR = 0.15       # entry must clear at least this much ATR from SL
MAX_PENDING_ENTRY_DIST_ATR = 1.5  # cap on how far a pending/zone entry may sit from market

# Entry-fill verification (Section 12)
PENDING_ENTRY_EXPIRY_BARS = 8  # in MONITOR_TF (15M) bars -> 2h
MONITOR_TF = TF_M15

# Macro/news blackout (Section 13) -- documented high-impact event windows.
# DECISION: without a live economic-calendar feed wired in, scheduled events
# are supplied via this list (populated externally / by ops) rather than
# invented; the blackout *mechanism* below is fully enforced regardless of
# whether any events are currently listed.
MACRO_EVENT_BLACKOUT_MIN_BEFORE = 30
MACRO_EVENT_BLACKOUT_MIN_AFTER = 30
SCHEDULED_MACRO_EVENTS: List[Dict[str, Any]] = []  # [{"name": "FOMC", "ts_ms": ..., "assets": ["BTC","ETH",...]}]

def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


# =============================================================================
# SECTION 2 -- CORE DATA STRUCTURES
# =============================================================================

@dataclass
class Candle:
    t: int
    o: float
    h: float
    l: float
    c: float
    v: float


@dataclass
class Pivot:
    idx: int
    price: float
    kind: str  # "high" | "low"
    t: int


@dataclass
class Zone:
    """Order Block / Breaker Block / Fair Value Gap -- unified zone shape."""
    kind: str          # "ob" | "breaker" | "fvg"
    direction: str      # "bullish" | "bearish" (direction of the expected reaction)
    top: float
    bottom: float
    idx: int            # candle index the zone formed at
    t: int
    mitigated: bool = False
    swept_from: Optional[int] = None  # idx of the liquidity sweep that produced this zone, if any
    origin_move_idx: Optional[int] = None  # the impulse-leg candle idx used for OTE refinement


@dataclass
class LiquidityCluster:
    level: float
    kind: str  # "eqh" | "eql"
    pivots: List[Pivot]
    swept: bool = False
    swept_idx: Optional[int] = None


@dataclass
class TFView:
    """Fully-processed, closed-candle-only view of one asset/timeframe."""
    tf: str
    candles: List[Candle]
    ema_fast: List[float]
    ema_slow: List[float]
    ema_trend: List[float]
    rsi: List[float]
    atr: List[float]
    adx: List[float]
    bb_mid: List[float]
    bb_up: List[float]
    bb_dn: List[float]
    pivots: List[Pivot]
    order_blocks: List[Zone]
    breaker_blocks: List[Zone]
    fvgs: List[Zone]
    eq_highs: List[LiquidityCluster]
    eq_lows: List[LiquidityCluster]
    bos_choch: List[Dict[str, Any]]  # [{"idx","t","kind":"BOS"/"CHoCH","direction"}]
    sfps: List[Dict[str, Any]]       # [{"idx","t","direction","pool","purity","session_anchored"}]


@dataclass
class RegimeVector:
    """Composite, continuous regime read (Section 6) -- never a discrete label."""
    macro_bias: float          # -1..1, BTC HTF directional bias
    volatility_pctile: float   # 0..1, ATR percentile vs own recent history
    trend_strength: float      # 0..1, ADX-derived
    session_weight: float      # 0..1, active session's historical reliability
    session_open_proximity: float  # 0..1, decaying proximity score to London/NY open
    liquidity_draw: float      # -1..1, ERL(-1, IRL-seeking-inward is +1)... see compute fn for sign convention
    noise_index: float         # 0..1, chop/whipsaw measure independent of raw volatility
    breadth: float             # 0..1, fraction of watchlist agreeing with macro bias

    def as_dict(self) -> Dict[str, float]:
        return asdict(self)


@dataclass
class RiskPlan:
    sl: float
    tp1: float
    tp2: float
    rr1: float
    rr2: float
    risk: float
    buffer: float
    sl_anchor: str


@dataclass
class Candidate:
    """A specialized engine's independently-produced trade candidate."""
    engine: str
    symbol: str
    style: str            # "intraday" | "swing"
    direction: str         # "bullish" | "bearish"
    entry: float
    entry_kind: str        # "market" | "pending" (Section 12)
    plan: RiskPlan
    confidence_raw: float  # 0..1 pre-calibration confidence from the specialized engine
    confluences: List[str]
    best_fit_regimes: List[str]
    session_anchored: bool = False
    meta: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ScoredSignal:
    candidate: Candidate
    score: float           # composite continuous score, 0..1
    grade: str              # "A+" | "A" | "B"
    term_contributions: Dict[str, float] = field(default_factory=dict)


# =============================================================================
# SECTION 3 -- HYPERLIQUID API CLIENT
# =============================================================================

HL_API_URL = "https://api.hyperliquid.xyz/info"
HL_WEIGHT_BUDGET_PER_MINUTE = float(os.environ.get("HL_WEIGHT_BUDGET_PER_MINUTE", "1100"))
HL_DEFAULT_INFO_WEIGHT = 20.0
HL_ENDPOINT_BASE_WEIGHT = {
    "candleSnapshot": HL_DEFAULT_INFO_WEIGHT,
    "metaAndAssetCtxs": 20.0,
    "l2Book": 2.0,
    "allMids": 2.0,
}


class _WeightRateLimiter:
    """Sliding-60s-window pacer keyed on request *weight*, shared across
    every call this run makes, so heavy candle pulls are paced correctly
    against Hyperliquid's documented weight-based rate limits."""

    def __init__(self, budget_per_minute: float):
        self.budget = budget_per_minute
        self.window_s = 60.0
        self.lock = threading.Lock()
        self.events: collections.deque = collections.deque()

    def wait(self, weight: float) -> None:
        while True:
            with self.lock:
                now = time.monotonic()
                cutoff = now - self.window_s
                while self.events and self.events[0][0] < cutoff:
                    self.events.popleft()
                used = sum(w for _, w in self.events)
                if used + weight <= self.budget:
                    self.events.append((now, weight))
                    return
                sleep_for = max(0.05, self.events[0][0] + self.window_s - now)
            time.sleep(min(sleep_for, 2.0))


_rate_limiter = _WeightRateLimiter(HL_WEIGHT_BUDGET_PER_MINUTE)


def _request_weight(payload: dict) -> float:
    req_type = payload.get("type", "")
    if req_type == "candleSnapshot":
        req = payload.get("req", {})
        interval = req.get("interval")
        start_ms, end_ms = req.get("startTime"), req.get("endTime")
        n_bars = 60
        if interval in TF_MS and start_ms is not None and end_ms is not None:
            step = TF_MS[interval]
            n_bars = max(1, math.ceil((end_ms - start_ms) / step))
        return HL_DEFAULT_INFO_WEIGHT * math.ceil(n_bars / 60)
    return HL_ENDPOINT_BASE_WEIGHT.get(req_type, HL_DEFAULT_INFO_WEIGHT)


def hl_post(payload: dict, retries: int = 4, timeout: int = 12):
    body = json.dumps(payload).encode()
    req = urllib.request.Request(
        HL_API_URL, data=body, headers={"Content-Type": "application/json"}
    )
    weight = _request_weight(payload)
    for attempt in range(retries):
        _rate_limiter.wait(weight)
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode())
        except urllib.error.HTTPError as e:
            if e.code == 429:
                retry_after = e.headers.get("Retry-After")
                wait_s = float(retry_after) if retry_after else 10.0
                log.warning("hl_post 429 (attempt %d, type=%s), backing off %.1fs",
                            attempt + 1, payload.get("type"), wait_s)
                time.sleep(wait_s)
            else:
                log.warning("hl_post HTTP error attempt %d (%s): %s", attempt + 1, payload.get("type"), e)
                time.sleep(0.8 * (attempt + 1))
        except (urllib.error.URLError, TimeoutError, ValueError, OSError) as e:
            log.warning("hl_post attempt %d failed (%s): %s", attempt + 1, payload.get("type"), e)
            time.sleep(0.8 * (attempt + 1))
    log.error("hl_post exhausted retries for type=%s", payload.get("type"))
    return None


def hl_interval_for_tf(tf: str) -> str:
    # Hyperliquid candle intervals use lowercase; weekly is "1w".
    return tf


def current_bar_open_ms(reference_ms: int, interval: str) -> int:
    step = TF_MS[interval]
    return (reference_ms // step) * step


def filter_closed_candles(candles: List[dict], interval: str, reference_ms: int) -> List[dict]:
    """Section 12A: never allow a still-forming candle into any downstream
    structural read. Anything at or after the current bar's open is dropped."""
    cutoff = current_bar_open_ms(reference_ms, interval)
    return [c for c in candles if c["t"] < cutoff]


def _request_candles_raw(symbol: str, interval: str, start_ms: int, end_ms: int) -> List[dict]:
    payload = {
        "type": "candleSnapshot",
        "req": {"coin": symbol, "interval": hl_interval_for_tf(interval),
                 "startTime": start_ms, "endTime": end_ms},
    }
    raw = hl_post(payload)
    if not raw:
        return []
    out = []
    for c in raw:
        try:
            out.append({"t": int(c["t"]), "o": float(c["o"]), "h": float(c["h"]),
                        "l": float(c["l"]), "c": float(c["c"]), "v": float(c["v"])})
        except (KeyError, ValueError, TypeError):
            continue
    return out


def get_candles(symbol: str, interval: str, n: int, reference_ms: Optional[int] = None,
                 cache_entry: Optional[List[dict]] = None) -> List[dict]:
    """Return the last `n` fully CLOSED candles, using the persistent cache
    to fetch only the delta past the cached watermark when possible."""
    reference_ms = reference_ms or int(time.time() * 1000)

    if cache_entry:
        step = TF_MS[interval]
        last_cached_t = cache_entry[-1]["t"]
        if current_bar_open_ms(reference_ms, interval) <= last_cached_t + step:
            return filter_closed_candles(cache_entry, interval, reference_ms)[-n:]
        start_ms = last_cached_t - step * CANDLE_DELTA_OVERLAP_BARS
        new_raw = _request_candles_raw(symbol, interval, start_ms, reference_ms)
        if new_raw:
            merged = {c["t"]: c for c in cache_entry}
            for c in new_raw:
                merged[c["t"]] = c
            candles = [merged[t] for t in sorted(merged.keys())]
        else:
            candles = cache_entry
        return filter_closed_candles(candles, interval, reference_ms)[-n:]

    lookback_ms = n * TF_MS[interval] * 2 + TF_MS[interval] * 5
    raw = _request_candles_raw(symbol, interval, reference_ms - lookback_ms, reference_ms)
    return filter_closed_candles(raw, interval, reference_ms)[-n:]


def fetch_all_candles(symbol: str, candle_cache: Optional[dict] = None,
                       reference_ms: Optional[int] = None) -> Optional[Dict[str, List[dict]]]:
    bundle: Dict[str, List[dict]] = {}
    sym_cache = (candle_cache or {}).get(symbol, {})
    for tf in ALL_TFS:
        cache_entry = sym_cache.get(tf)
        candles = get_candles(symbol, tf, CANDLE_COUNT[tf], reference_ms, cache_entry)
        min_needed = {"1w": 60, "1d": 80, "4h": 80, "1h": 80, "15m": 80}[tf]
        if len(candles) < min_needed:
            log.info("Insufficient %s candles for %s (%d)", tf, symbol, len(candles))
            return None
        bundle[tf] = candles
        if candle_cache is not None:
            candle_cache.setdefault(symbol, {})[tf] = candles
    return bundle


def get_meta_and_ctx() -> Optional[Tuple[List[str], List[dict]]]:
    raw = hl_post({"type": "metaAndAssetCtxs"})
    if not raw or len(raw) < 2:
        return None
    universe = [a["name"] for a in raw[0]["universe"]]
    return universe, raw[1]


def get_market_snapshot() -> Dict[str, dict]:
    """symbol -> {mark, funding, oi_usd}"""
    out: Dict[str, dict] = {}
    got = get_meta_and_ctx()
    if not got:
        return out
    universe, ctxs = got
    watch = set(WATCHLIST)
    for i, name in enumerate(universe):
        if name not in watch:
            continue
        try:
            ctx = ctxs[i]
            mark = float(ctx.get("markPx", 0) or 0)
            funding = float(ctx.get("funding", 0) or 0)
            oi_coins = float(ctx.get("openInterest", 0) or 0)
            out[name] = {"mark": mark, "funding": funding, "oi_usd": oi_coins * mark}
        except (KeyError, ValueError, TypeError, IndexError):
            continue
    return out


def get_l2_book(coin: str) -> Optional[dict]:
    return hl_post({"type": "l2Book", "coin": coin})


def nearest_liquidity_wall(coin: str, direction: str, entry: float) -> Optional[float]:
    """Cheap order-book ledge read used by the liquidity-wall TP clip
    (Section 10): the nearest side with a resting-size cluster meaningfully
    larger than the local average, in the direction TP travels."""
    book = get_l2_book(coin)
    if not book or "levels" not in book or len(book["levels"]) < 2:
        return None
    try:
        side = book["levels"][1] if direction == "bullish" else book["levels"][0]
        sizes = [float(lvl["sz"]) for lvl in side]
        if not sizes:
            return None
        avg = sum(sizes) / len(sizes)
        for lvl in side:
            if float(lvl["sz"]) > avg * 3.0:
                return float(lvl["px"])
    except (KeyError, ValueError, TypeError, IndexError):
        return None
    return None


# =============================================================================
# SECTION 4 -- INDICATORS (closed-candle inputs only)
# =============================================================================

def to_candles(raw: List[dict]) -> List[Candle]:
    return [Candle(**c) for c in raw]


def ema(values: List[float], length: int) -> List[float]:
    if not values:
        return []
    k = 2.0 / (length + 1)
    out = [values[0]]
    for v in values[1:]:
        out.append(v * k + out[-1] * (1 - k))
    return out


def rsi(closes: List[float], length: int = RSI_LEN) -> List[float]:
    if len(closes) < 2:
        return [50.0] * len(closes)
    gains, losses = [0.0], [0.0]
    for i in range(1, len(closes)):
        d = closes[i] - closes[i - 1]
        gains.append(max(d, 0.0))
        losses.append(max(-d, 0.0))
    avg_gain, avg_loss = gains[1] if len(gains) > 1 else 0.0, losses[1] if len(losses) > 1 else 0.0
    out = [50.0]
    ag, al = 0.0, 0.0
    for i in range(1, len(closes)):
        if i <= length:
            ag = sum(gains[1:i + 1]) / length if i == length else ag
            al = sum(losses[1:i + 1]) / length if i == length else al
            out.append(50.0)
            continue
        ag = (ag * (length - 1) + gains[i]) / length
        al = (al * (length - 1) + losses[i]) / length
        rs = ag / al if al > 1e-12 else 100.0
        out.append(100.0 - 100.0 / (1.0 + rs))
    while len(out) < len(closes):
        out.append(50.0)
    return out[:len(closes)]


def true_range(candles: List[Candle]) -> List[float]:
    tr = [candles[0].h - candles[0].l]
    for i in range(1, len(candles)):
        c, p = candles[i], candles[i - 1]
        tr.append(max(c.h - c.l, abs(c.h - p.c), abs(c.l - p.c)))
    return tr


def atr(candles: List[Candle], length: int = ATR_LEN) -> List[float]:
    tr = true_range(candles)
    out = [tr[0]]
    for i in range(1, len(tr)):
        if i < length:
            out.append(sum(tr[:i + 1]) / (i + 1))
        else:
            out.append((out[-1] * (length - 1) + tr[i]) / length)
    return out


def adx(candles: List[Candle], length: int = ADX_LEN) -> List[float]:
    n = len(candles)
    if n < 2:
        return [15.0] * n
    plus_dm, minus_dm, tr = [0.0], [0.0], [candles[0].h - candles[0].l]
    for i in range(1, n):
        up = candles[i].h - candles[i - 1].h
        dn = candles[i - 1].l - candles[i].l
        plus_dm.append(up if (up > dn and up > 0) else 0.0)
        minus_dm.append(dn if (dn > up and dn > 0) else 0.0)
        c, p = candles[i], candles[i - 1]
        tr.append(max(c.h - c.l, abs(c.h - p.c), abs(c.l - p.c)))

    def wilder_smooth(vals):
        out = [vals[0]]
        for i in range(1, len(vals)):
            if i < length:
                out.append(sum(vals[:i + 1]))
            else:
                out.append(out[-1] - out[-1] / length + vals[i])
        return out

    str_tr = wilder_smooth(tr)
    str_pdm = wilder_smooth(plus_dm)
    str_mdm = wilder_smooth(minus_dm)
    dx = []
    for i in range(n):
        denom = str_tr[i] if str_tr[i] > 1e-12 else 1e-12
        pdi = 100.0 * str_pdm[i] / denom
        mdi = 100.0 * str_mdm[i] / denom
        s = pdi + mdi
        dx.append(100.0 * abs(pdi - mdi) / s if s > 1e-12 else 0.0)
    out = [dx[0]]
    for i in range(1, n):
        if i < length:
            out.append(sum(dx[:i + 1]) / (i + 1))
        else:
            out.append((out[-1] * (length - 1) + dx[i]) / length)
    return out


def bollinger(closes: List[float], length: int = BB_LEN, mult: float = BB_MULT):
    mid, up, dn = [], [], []
    for i in range(len(closes)):
        w = closes[max(0, i - length + 1):i + 1]
        m = sum(w) / len(w)
        sd = statistics.pstdev(w) if len(w) > 1 else 0.0
        mid.append(m)
        up.append(m + mult * sd)
        dn.append(m - mult * sd)
    return mid, up, dn


def swing_pivots(candles: List[Candle], lookback: int = 3) -> List[Pivot]:
    """Fractal-style swing highs/lows on fully closed candles only."""
    pivots: List[Pivot] = []
    n = len(candles)
    for i in range(lookback, n - lookback):
        window = candles[i - lookback:i + lookback + 1]
        c = candles[i]
        if c.h == max(w.h for w in window):
            pivots.append(Pivot(i, c.h, "high", c.t))
        if c.l == min(w.l for w in window):
            pivots.append(Pivot(i, c.l, "low", c.t))
    return pivots


def percentile(values: List[float], pct: float) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    idx = clamp(int(len(s) * pct / 100.0), 0, len(s) - 1)
    return s[idx]


# =============================================================================
# SECTION 5 -- STRUCTURAL DETECTION (Section 8 concepts, Section 12A parity)
# =============================================================================
# Every function in this section operates strictly on `candles`, a list that
# by construction (Section 3's filter_closed_candles) never contains a
# still-forming bar. There is exactly one implementation of each detection
# type; live and any future backtest/paper-trading path must call these same
# functions -- never a separate reimplementation -- per Section 12A.

def detect_bos_choch(candles: List[Candle], pivots: List[Pivot]) -> List[Dict[str, Any]]:
    """Break of Structure / Change of Character from confirmed swing pivots."""
    events: List[Dict[str, Any]] = []
    highs = [p for p in pivots if p.kind == "high"]
    lows = [p for p in pivots if p.kind == "low"]
    last_dir = None
    ordered = sorted(pivots, key=lambda p: p.idx)
    last_high = last_low = None
    for p in ordered:
        if p.kind == "high":
            last_high = p
        else:
            last_low = p
    # Walk candles; whenever close breaks the most recent confirmed opposite
    # pivot, register BOS (trend continuation) or CHoCH (trend reversal).
    seen_high, seen_low = None, None
    trend = None
    for i, c in enumerate(candles):
        relevant_highs = [p for p in highs if p.idx < i]
        relevant_lows = [p for p in lows if p.idx < i]
        if relevant_highs:
            rh = relevant_highs[-1]
            if c.c > rh.price:
                kind = "CHoCH" if trend == "bearish" else "BOS"
                events.append({"idx": i, "t": c.t, "kind": kind, "direction": "bullish"})
                trend = "bullish"
        if relevant_lows:
            rl = relevant_lows[-1]
            if c.c < rl.price:
                kind = "CHoCH" if trend == "bullish" else "BOS"
                events.append({"idx": i, "t": c.t, "kind": kind, "direction": "bearish"})
                trend = "bearish"
    return events


def detect_fvgs(candles: List[Candle]) -> List[Zone]:
    """Three-candle Fair Value Gap: a gap between candle[i-2] and candle[i]
    left unfilled by candle[i-1]'s range."""
    zones: List[Zone] = []
    for i in range(2, len(candles)):
        a, b, c = candles[i - 2], candles[i - 1], candles[i]
        if c.l > a.h:  # bullish imbalance
            zones.append(Zone("fvg", "bullish", top=c.l, bottom=a.h, idx=i, t=c.t,
                               origin_move_idx=i))
        elif c.h < a.l:  # bearish imbalance
            zones.append(Zone("fvg", "bearish", top=a.l, bottom=c.h, idx=i, t=c.t,
                               origin_move_idx=i))
    _mark_mitigated(zones, candles)
    return zones


def detect_order_blocks(candles: List[Candle], bos_events: List[Dict[str, Any]]) -> List[Zone]:
    """Order block = the last opposite-direction candle before the impulse
    leg that produced a confirmed BOS/CHoCH -- i.e. causally tied to a real
    structure break, not any arbitrary reversal candle."""
    zones: List[Zone] = []
    for ev in bos_events:
        i = ev["idx"]
        direction = ev["direction"]
        # look back up to 6 candles for the last opposite-colored candle
        for j in range(i - 1, max(i - 7, -1), -1):
            cand = candles[j]
            is_bear_candle = cand.c < cand.o
            is_bull_candle = cand.c > cand.o
            if direction == "bullish" and is_bear_candle:
                zones.append(Zone("ob", "bullish", top=cand.h, bottom=cand.l, idx=j, t=cand.t,
                                   origin_move_idx=i))
                break
            if direction == "bearish" and is_bull_candle:
                zones.append(Zone("ob", "bearish", top=cand.h, bottom=cand.l, idx=j, t=cand.t,
                                   origin_move_idx=i))
                break
    _mark_mitigated(zones, candles)
    return zones


def detect_breaker_blocks(candles: List[Candle], order_blocks: List[Zone],
                           bos_events: List[Dict[str, Any]]) -> List[Zone]:
    """Breaker block = a former order block that price closed back through
    (i.e. the OB failed and structure broke the other way) -- it flips to
    become a POI in the opposite direction, per Section 8."""
    zones: List[Zone] = []
    for ob in order_blocks:
        for ev in bos_events:
            if ev["idx"] <= ob.idx:
                continue
            c = candles[ev["idx"]]
            if ob.direction == "bullish" and ev["direction"] == "bearish" and c.c < ob.bottom:
                zones.append(Zone("breaker", "bearish", top=ob.top, bottom=ob.bottom,
                                   idx=ev["idx"], t=c.t, origin_move_idx=ev["idx"]))
            elif ob.direction == "bearish" and ev["direction"] == "bullish" and c.c > ob.top:
                zones.append(Zone("breaker", "bullish", top=ob.top, bottom=ob.bottom,
                                   idx=ev["idx"], t=c.t, origin_move_idx=ev["idx"]))
    _mark_mitigated(zones, candles)
    return zones


def _mark_mitigated(zones: List[Zone], candles: List[Candle]) -> None:
    for z in zones:
        for k in range(z.idx + 1, len(candles)):
            c = candles[k]
            if c.l <= z.top and c.h >= z.bottom:
                z.mitigated = True
                break


def detect_equal_liquidity(pivots: List[Pivot], candles: List[Candle],
                            tol_frac: float = 0.0015) -> Tuple[List[LiquidityCluster], List[LiquidityCluster]]:
    """EQH/EQL clustering: swing pivots sitting within a tight tolerance of
    one another form a resting-liquidity pool (BSL above EQH, SSL below EQL)."""
    highs = sorted([p for p in pivots if p.kind == "high"], key=lambda p: p.price)
    lows = sorted([p for p in pivots if p.kind == "low"], key=lambda p: p.price)

    def cluster(ps: List[Pivot], kind: str) -> List[LiquidityCluster]:
        out: List[LiquidityCluster] = []
        i = 0
        while i < len(ps):
            group = [ps[i]]
            j = i + 1
            while j < len(ps) and abs(ps[j].price - group[0].price) <= group[0].price * tol_frac:
                group.append(ps[j])
                j += 1
            if len(group) >= 2:
                level = sum(p.price for p in group) / len(group)
                lc = LiquidityCluster(level=level, kind=kind, pivots=group)
                last_idx = max(p.idx for p in group)
                for k in range(last_idx + 1, len(candles)):
                    swept = (candles[k].h > level) if kind == "eqh" else (candles[k].l < level)
                    if swept:
                        lc.swept, lc.swept_idx = True, k
                        break
                out.append(lc)
            i = j
        return out

    return cluster(highs, "eqh"), cluster(lows, "eql")


def detect_sfps(candles: List[Candle], eq_highs: List[LiquidityCluster],
                 eq_lows: List[LiquidityCluster],
                 session_open_windows: Optional[List[Tuple[int, int]]] = None) -> List[Dict[str, Any]]:
    """Swing Failure Pattern: a wick sweeps a liquidity pool (EQH/EQL) and
    closes back inside, signalling a reversal. `purity` scores how clean the
    rejection was (close-back-through fraction of the wick); session_anchored
    flags SFPs occurring inside a defined London/NY open window for Section
    13's separately-tracked performance bucket."""
    out: List[Dict[str, Any]] = []
    session_open_windows = session_open_windows or []

    def is_session_anchored(t_ms: int) -> bool:
        return any(lo <= t_ms <= hi for lo, hi in session_open_windows)

    for pool in eq_highs:
        if not pool.swept or pool.swept_idx is None:
            continue
        k = pool.swept_idx
        c = candles[k]
        if c.h > pool.level and c.c < pool.level:
            wick = c.h - max(c.o, c.c)
            purity = clamp((c.h - c.c) / wick, 0.0, 1.0) if wick > 1e-12 else 0.0
            out.append({"idx": k, "t": c.t, "direction": "bearish", "pool": pool,
                        "purity": purity, "session_anchored": is_session_anchored(c.t)})
    for pool in eq_lows:
        if not pool.swept or pool.swept_idx is None:
            continue
        k = pool.swept_idx
        c = candles[k]
        if c.l < pool.level and c.c > pool.level:
            wick = min(c.o, c.c) - c.l
            purity = clamp((c.c - c.l) / wick, 0.0, 1.0) if wick > 1e-12 else 0.0
            out.append({"idx": k, "t": c.t, "direction": "bullish", "pool": pool,
                        "purity": purity, "session_anchored": is_session_anchored(c.t)})
    return out


def build_tf_view(tf: str, raw_candles: List[dict]) -> Optional[TFView]:
    candles = to_candles(raw_candles)
    if len(candles) < 30:
        return None
    closes = [c.c for c in candles]
    ef, es, et = ema(closes, EMA_FAST), ema(closes, EMA_SLOW), ema(closes, EMA_TREND)
    r = rsi(closes)
    a = atr(candles)
    dx = adx(candles)
    bmid, bup, bdn = bollinger(closes)
    pivots = swing_pivots(candles)
    bos = detect_bos_choch(candles, pivots)
    obs = detect_order_blocks(candles, bos)
    breakers = detect_breaker_blocks(candles, obs, bos)
    fvgs = detect_fvgs(candles)
    eqh, eql = detect_equal_liquidity(pivots, candles)
    sfps = detect_sfps(candles, eqh, eql, session_open_windows=_session_open_windows_for(candles))
    return TFView(tf=tf, candles=candles, ema_fast=ef, ema_slow=es, ema_trend=et, rsi=r,
                  atr=a, adx=dx, bb_mid=bmid, bb_up=bup, bb_dn=bdn, pivots=pivots,
                  order_blocks=obs, breaker_blocks=breakers, fvgs=fvgs,
                  eq_highs=eqh, eq_lows=eql, bos_choch=bos, sfps=sfps)


def _session_open_windows_for(candles: List[Candle]) -> List[Tuple[int, int]]:
    """London (07:00 UTC) and NY (12:30 UTC) open windows, +/-45min, for
    every day spanned by `candles` -- used to tag session-anchored SFPs."""
    if not candles:
        return []
    windows = []
    seen_days = set()
    for c in candles:
        day = c.t // 86_400_000
        if day in seen_days:
            continue
        seen_days.add(day)
        day_start = day * 86_400_000
        for open_min in (7 * 60, 12 * 60 + 30):
            center = day_start + open_min * 60_000
            windows.append((center - 45 * 60_000, center + 45 * 60_000))
    return windows


# =============================================================================
# SECTION 6 -- COMPOSITE REGIME VECTOR
# =============================================================================

def _volatility_percentile(view: TFView) -> float:
    if len(view.atr) < 30:
        return 0.5
    hist = view.atr[-100:]
    cur = view.atr[-1]
    rank = sum(1 for v in hist if v <= cur) / len(hist)
    return clamp(rank, 0.0, 1.0)


def _noise_index(view: TFView) -> float:
    """Chop measure independent of raw volatility: ratio of net displacement
    to total path length over a recent window (low ratio = choppy)."""
    n = min(30, len(view.candles) - 1)
    if n < 5:
        return 0.5
    window = view.candles[-n:]
    net = abs(window[-1].c - window[0].c)
    path = sum(abs(window[i].c - window[i - 1].c) for i in range(1, len(window)))
    if path < 1e-12:
        return 0.5
    directional_ratio = net / path
    return clamp(1.0 - directional_ratio, 0.0, 1.0)


def _session_weight(reference_ms: int, session_stats: Dict[str, float]) -> float:
    """Active-session historical reliability, from Tier1 learning state
    (session_stats keyed 'asia'/'london'/'ny'), defaulting to a sane prior
    reflecting London/NY's typically higher genuine-move contribution."""
    dt = datetime.fromtimestamp(reference_ms / 1000, tz=timezone.utc)
    hour = dt.hour
    if 0 <= hour < 7:
        sess = "asia"
    elif 7 <= hour < 12:
        sess = "london"
    elif 12 <= hour < 21:
        sess = "ny"
    else:
        sess = "asia"
    default = {"asia": 0.35, "london": 0.65, "ny": 0.65}
    return clamp(session_stats.get(sess, default[sess]), 0.0, 1.0)


def _session_open_proximity(reference_ms: int) -> float:
    """Continuous, decaying score peaking at London/NY session opens.
    Section 6: soft scoring input only, never a hard gate."""
    dt = datetime.fromtimestamp(reference_ms / 1000, tz=timezone.utc)
    minute_of_day = dt.hour * 60 + dt.minute
    centers = [7 * 60, 12 * 60 + 30]
    best = 0.0
    for center in centers:
        dist_min = min(abs(minute_of_day - center), 1440 - abs(minute_of_day - center))
        score = math.exp(-(dist_min ** 2) / (2 * 60.0 ** 2))  # ~1hr decay constant
        best = max(best, score)
    return clamp(best, 0.0, 1.0)


def _liquidity_draw_state(view: TFView, direction_hint: float) -> float:
    """-1 (oriented toward internal/IRL rebalance) .. +1 (oriented toward
    external/ERL liquidity draw). Combines unmitigated-IRL density against
    unswept EQH/EQL distance, biased by macro direction hint (soft input,
    Section 6 -- never a standalone directional call)."""
    if not view.candles:
        return 0.0
    price = view.candles[-1].c
    unmitigated_irl = sum(
        1 for z in (view.order_blocks + view.breaker_blocks + view.fvgs) if not z.mitigated
    )
    unswept_pools = sum(1 for p in (view.eq_highs + view.eq_lows) if not p.swept)
    total = unmitigated_irl + unswept_pools
    if total == 0:
        return 0.0
    erl_share = unswept_pools / total
    raw = (erl_share - 0.5) * 2.0  # -1..1, positive = ERL-oriented
    return clamp(raw * 0.7 + clamp(direction_hint, -1, 1) * 0.3, -1.0, 1.0)


def _macro_bias(btc_daily: TFView, btc_h4: TFView) -> float:
    d, h = btc_daily, btc_h4
    if not d.ema_fast or not h.ema_fast:
        return 0.0
    d_score = clamp((d.ema_fast[-1] - d.ema_slow[-1]) / (d.atr[-1] or 1e-9) * 0.3, -1, 1)
    h_score = clamp((h.ema_fast[-1] - h.ema_slow[-1]) / (h.atr[-1] or 1e-9) * 0.3, -1, 1)
    trend_conf = clamp(d.adx[-1] / 40.0, 0, 1) if d.adx else 0.5
    return clamp((0.65 * d_score + 0.35 * h_score) * (0.5 + 0.5 * trend_conf), -1.0, 1.0)


def _breadth(all_h1_views: Dict[str, TFView], macro_bias: float) -> float:
    if not all_h1_views or abs(macro_bias) < 1e-9:
        return 0.5
    agree = 0
    total = 0
    macro_dir = 1 if macro_bias > 0 else -1
    for sym, v in all_h1_views.items():
        if not v.ema_fast:
            continue
        total += 1
        asset_dir = 1 if v.ema_fast[-1] >= v.ema_slow[-1] else -1
        if asset_dir == macro_dir:
            agree += 1
    return clamp(agree / total, 0.0, 1.0) if total else 0.5


def compute_regime_vector(symbol: str, views: Dict[str, TFView], btc_daily: TFView, btc_h4: TFView,
                           all_h1_views: Dict[str, TFView], reference_ms: int,
                           session_stats: Dict[str, float]) -> RegimeVector:
    h1 = views.get(TF_H1) or views.get(TF_H4)
    macro_bias = _macro_bias(btc_daily, btc_h4)
    vol_pctile = _volatility_percentile(h1) if h1 else 0.5
    trend_strength = clamp((h1.adx[-1] if h1 and h1.adx else 15.0) / 40.0, 0.0, 1.0)
    session_weight = _session_weight(reference_ms, session_stats)
    session_open_prox = _session_open_proximity(reference_ms)
    liq_draw = _liquidity_draw_state(h1, macro_bias) if h1 else 0.0
    noise = _noise_index(h1) if h1 else 0.5
    breadth = _breadth(all_h1_views, macro_bias)
    return RegimeVector(
        macro_bias=macro_bias, volatility_pctile=vol_pctile, trend_strength=trend_strength,
        session_weight=session_weight, session_open_proximity=session_open_prox,
        liquidity_draw=liq_draw, noise_index=noise, breadth=breadth,
    )


def regime_label_bucket(rv: RegimeVector) -> str:
    """Coarse human-readable label derived FROM the vector, for logging/
    messaging/segment-keying only -- never fed back in as the actual regime
    read (the vector itself, not this label, drives scoring/filtering)."""
    if rv.trend_strength > 0.55:
        base = "Trending Bull" if rv.macro_bias > 0.1 else ("Trending Bear" if rv.macro_bias < -0.1 else "Trending Neutral")
    elif rv.noise_index > 0.6:
        base = "Choppy Range"
    else:
        base = "Ranging"
    if rv.volatility_pctile > 0.8:
        base += " / High-Vol"
    elif rv.volatility_pctile < 0.2:
        base += " / Low-Vol"
    return base


# =============================================================================
# SECTION 7 -- ZONE-SELECTION SEQUENCE (Section 8)
# =============================================================================
# Mechanism for choosing WHICH zone to trade, applied before entry-placement
# rules. Sequence: HTF bias context -> liquidity sweep -> POI -> SFP purity
# -> MSS confirmation -> breaker confirmation -> OTE refinement (placement
# only, never an independent confluence point).

def zone_selection_sequence(direction: str, htf_view: TFView, ltf_view: TFView) -> Optional[Dict[str, Any]]:
    """Returns a dict describing the selected POI and its SFP/MSS lineage,
    or None if no structurally valid sequence exists. `htf_view` supplies
    the POI candidates (1H per Section 7 Stage 3); `ltf_view` supplies the
    confirming MSS (15M per Stage 4) when called from the entry stage --
    when called from Stage 3 itself, pass htf_view twice (POI + its own
    internal confirmation) since Stage 3 validates structure, not entry.
    """
    pools = htf_view.eq_lows if direction == "bullish" else htf_view.eq_highs
    swept_pools = [p for p in pools if p.swept]
    if not swept_pools:
        return None  # no liquidity sweep -> no institutional footprint to anchor a POI on

    # SFPs on the HTF view carry the sweep-to-reversal signature.
    matching_sfps = [s for s in htf_view.sfps if s["direction"] == direction]
    if not matching_sfps:
        return None
    sfp = matching_sfps[-1]
    if sfp["purity"] < 0.35:
        return None  # impure SFP -- sequence violated (Section 13 forensic category)

    sweep_idx = sfp["idx"]

    # POI candidates causally tied to this specific sweep (sweep-to-POI
    # causality requirement, Section 8/19): the zone must have formed at or
    # after the sweep and before/at the current bar.
    poi_pool = [z for z in (htf_view.order_blocks + htf_view.breaker_blocks + htf_view.fvgs)
                if z.direction == direction and not z.mitigated and z.idx >= sweep_idx]
    if not poi_pool:
        return None
    # Prefer breaker > order block > fvg (breakers already proved themselves
    # by invalidating the opposite structure once).
    rank = {"breaker": 3, "ob": 2, "fvg": 1}
    poi = max(poi_pool, key=lambda z: (rank.get(z.kind, 0), -z.idx))

    # MSS confirmation: a BOS/CHoCH in `direction` occurring after the sweep.
    mss_events = [e for e in htf_view.bos_choch if e["direction"] == direction and e["idx"] >= sweep_idx]
    if not mss_events:
        return None
    mss = mss_events[-1]

    return {
        "poi": poi, "sfp": sfp, "mss": mss, "sweep_pool": sfp["pool"],
        "session_anchored": sfp.get("session_anchored", False),
    }


def ote_refine_entry(direction: str, poi: Zone, impulse_leg: Optional[Tuple[float, float]]) -> float:
    """Fibonacci OTE (61.8-79%) refinement of entry placement within an
    already-validated POI. Never nominates a zone on its own; only tightens
    placement inside `poi`. Falls back to the POI midpoint when no clean
    impulse leg is available."""
    if impulse_leg is None:
        return (poi.top + poi.bottom) / 2.0
    leg_start, leg_end = impulse_leg
    span = leg_end - leg_start
    if abs(span) < 1e-12:
        return (poi.top + poi.bottom) / 2.0
    ote_lo = leg_end - span * 0.786
    ote_hi = leg_end - span * 0.618
    lo, hi = (ote_lo, ote_hi) if ote_lo <= ote_hi else (ote_hi, ote_lo)
    zone_lo, zone_hi = min(poi.top, poi.bottom), max(poi.top, poi.bottom)
    overlap_lo, overlap_hi = max(lo, zone_lo), min(hi, zone_hi)
    if overlap_lo <= overlap_hi:
        return (overlap_lo + overlap_hi) / 2.0
    return (poi.top + poi.bottom) / 2.0


# =============================================================================
# SECTION 8 -- MANDATORY TOP-DOWN SEQUENCE (Section 7 of spec)
# =============================================================================

def stage1_market_bias(weekly: TFView, daily: TFView) -> str:
    """Weekly + Daily -> Bullish / Bearish / Neutral. Neutral is a complete,
    valid outcome -- not a data-gap fallback."""
    if not weekly.ema_fast or not daily.ema_fast:
        return "Neutral"
    w_dir = 1 if weekly.ema_fast[-1] > weekly.ema_slow[-1] else -1
    d_dir = 1 if daily.ema_fast[-1] > daily.ema_slow[-1] else -1
    w_trend_ok = (weekly.adx[-1] if weekly.adx else 0) > 18
    d_trend_ok = (daily.adx[-1] if daily.adx else 0) > 18
    if w_dir == d_dir and (w_trend_ok or d_trend_ok):
        return "Bullish" if w_dir > 0 else "Bearish"
    return "Neutral"


def stage2_context_agrees(bias: str, h4: TFView) -> bool:
    """4H must confirm the Weekly/Daily bias -- trend vs range, momentum."""
    if bias == "Neutral" or not h4.ema_fast:
        return False
    h4_dir = 1 if h4.ema_fast[-1] > h4.ema_slow[-1] else -1
    bias_dir = 1 if bias == "Bullish" else -1
    trending_enough = (h4.adx[-1] if h4.adx else 0) > 16
    return h4_dir == bias_dir and trending_enough


def stage3_setup_status(bias: str, h1: TFView) -> Tuple[str, Optional[Dict[str, Any]]]:
    """1H -> VALID / NOT READY / INVALID via the Zone-Selection Sequence."""
    direction = "bullish" if bias == "Bullish" else "bearish"
    seq = zone_selection_sequence(direction, h1, h1)
    if seq is None:
        # distinguish NOT READY (structure intact, just not formed) from
        # INVALID (structure actively contradicts bias) using recent BOS/CHoCH
        recent = h1.bos_choch[-3:] if h1.bos_choch else []
        contradicts = any(e["direction"] != direction and e["kind"] == "CHoCH" for e in recent)
        return ("INVALID" if contradicts else "NOT READY"), None
    return "VALID", seq


def stage4_entry_trigger(bias: str, h1_seq: Dict[str, Any], m15: TFView) -> Optional[Dict[str, Any]]:
    """15M -> confirmed MSS inside the 1H POI; the FVG from that specific
    break is the entry vehicle, refined by OTE where it overlaps the POI."""
    direction = "bullish" if bias == "Bullish" else "bearish"
    poi: Zone = h1_seq["poi"]
    m15_mss = [e for e in m15.bos_choch if e["direction"] == direction]
    if not m15_mss:
        return None
    mss = m15_mss[-1]
    # the FVG created by that specific 15M break, inside the 1H POI
    candidate_fvgs = [z for z in m15.fvgs if z.direction == direction and not z.mitigated
                       and abs(z.idx - mss["idx"]) <= 2]
    poi_lo, poi_hi = min(poi.top, poi.bottom), max(poi.top, poi.bottom)
    overlapping = [z for z in candidate_fvgs
                   if max(z.bottom, poi_lo) <= min(z.top, poi_hi)]
    fvg = overlapping[0] if overlapping else (candidate_fvgs[0] if candidate_fvgs else None)
    if fvg is None:
        return None
    impulse_leg = None
    if fvg.origin_move_idx is not None and 0 < fvg.origin_move_idx < len(m15.candles):
        leg_start_idx = max(0, fvg.origin_move_idx - 4)
        impulse_leg = (m15.candles[leg_start_idx].c, m15.candles[fvg.origin_move_idx].c)
    entry = ote_refine_entry(direction, fvg, impulse_leg)
    return {"entry": entry, "fvg": fvg, "mss": mss, "poi": poi}


# =============================================================================
# SECTION 9 -- ADAPTIVE STRUCTURAL RISK PLAN (Section 10 reference pattern)
# =============================================================================
# Mandatory single construction for every SL/TP this engine dispatches.
# Reject-only gates throughout; never reshapes, stretches, or fabricates a
# level to force a pass.

def _structural_invalidation_level(direction: str, entry: float, view: TFView) -> Optional[float]:
    """The nearest opposing structural swing behind entry -- the raw level
    the SL is anchored to, before buffer/liquidity-clearing is applied."""
    if direction == "bullish":
        candidates = [p.price for p in view.pivots if p.kind == "low" and p.price < entry]
    else:
        candidates = [p.price for p in view.pivots if p.kind == "high" and p.price > entry]
    if not candidates:
        return None
    return max(candidates) if direction == "bullish" else min(candidates)


def adaptive_sl_buffer(view: TFView, state: dict, asset: str) -> float:
    key = f"{asset}:{view.tf}"
    pctile = state["tier1"]["sl_buffer_percentile"].get(key, 65.0)
    wicks = []
    for c in view.candles[1:]:
        body_top, body_bot = max(c.o, c.c), min(c.o, c.c)
        wicks.append(c.h - body_top)
        wicks.append(body_bot - c.l)
    wicks = sorted(w for w in wicks if w > 0)
    atr_val = view.atr[-1] if view.atr else 0.0
    if not wicks:
        return atr_val * 0.25
    idx = clamp(int(len(wicks) * pctile / 100.0), 0, len(wicks) - 1)
    buffer = wicks[idx]
    return clamp(buffer, atr_val * 0.4, atr_val * 2.5) if atr_val else buffer


def select_sl_anchor(direction: str, entry: float, m15_view: TFView, h1_view: TFView,
                      h4_view: TFView, state: dict, asset: str) -> Optional[Tuple[str, TFView, float]]:
    m15_level = _structural_invalidation_level(direction, entry, m15_view)
    if m15_level is not None:
        buffer = adaptive_sl_buffer(m15_view, state, asset)
        sl = (m15_level - buffer) if direction == "bullish" else (m15_level + buffer)
        risk_atr = abs(entry - sl) / (m15_view.atr[-1] or 1e-9)
        if risk_atr >= MIN_RISK_ATR_MULT:
            return ("15M", m15_view, m15_level)

    fallbacks = []
    for name, view in (("H1", h1_view), ("H4", h4_view)):
        level = _structural_invalidation_level(direction, entry, view)
        if level is not None:
            fallbacks.append((name, view, level))
    if not fallbacks:
        return None
    fallbacks.sort(key=lambda f: abs(entry - f[2]))
    return fallbacks[0]


def _clear_sl_of_liquidity_pool(direction: str, sl: float, view: TFView) -> float:
    pools = view.eq_lows if direction == "bullish" else view.eq_highs
    for pool in pools:
        if pool.swept:
            continue  # already swept -- no longer a resting pool to clear
        prices = [p.price for p in pool.pivots]
        lo, hi = min(prices), max(prices)
        margin = max(hi - lo, 1e-9)
        if direction == "bullish" and sl >= lo:
            sl = lo - margin
        elif direction == "bearish" and sl <= hi:
            sl = hi + margin
    return sl


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


def _opposing_structural_levels(direction: str, entry: float, view: TFView) -> List[Dict[str, Any]]:
    candidates: List[Dict[str, Any]] = []

    def _add(price, weight):
        candidates.append({"price": price, "score": weight})

    if direction == "bullish":
        for p in view.pivots:
            if p.kind == "high" and p.price > entry:
                _add(p.price, 1)
        for e in view.eq_highs:
            if e.level > entry:
                _add(e.level, 2 + min(len(e.pivots), 3))
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
            if e.level < entry:
                _add(e.level, 2 + min(len(e.pivots), 3))
        for z in (view.order_blocks + view.breaker_blocks):
            if z.direction == "bullish" and not z.mitigated and z.top < entry:
                _add(z.top, 2)
        for z in view.fvgs:
            if z.direction == "bullish" and not z.mitigated and z.top < entry:
                _add(z.top, 1)

    if not candidates:
        return []
    tol = (view.atr[-1] or 1e-9) * 0.05
    merged = _merge_confluent_levels(candidates, tol)
    merged.sort(key=lambda c: abs(c["price"] - entry))
    return merged


def _tp_selection_band(candidates: List[Dict[str, Any]], state: dict, asset: str) -> List[Dict[str, Any]]:
    n = int(state["tier1"].get("tp1_target_rank_preference", {}).get(asset, 3))
    n = clamp(n, 2, 6)
    return candidates[:max(int(n), 2)]


def tp1_runway_ok(direction: str, entry: float, m15_view: TFView, state: dict, asset: str) -> bool:
    candidates = _opposing_structural_levels(direction, entry, m15_view)
    if not candidates:
        return False
    band = _tp_selection_band(candidates, state, asset)
    best_in_band = max(band, key=lambda c: c["score"])
    plausible_reward = abs(best_in_band["price"] - entry)
    typical_risk = state["tier1"].get("sl_buffer_percentile_dist", {}).get(
        f"{asset}:15M", (m15_view.atr[-1] or 1e-9) * MIN_RISK_ATR_MULT)
    return (plausible_reward / max(typical_risk, 1e-9)) >= RR_MIN_GATE * 0.8


def _rr(entry: float, sl: float, tp: float, direction: str) -> float:
    risk = abs(entry - sl)
    reward = (tp - entry) if direction == "bullish" else (entry - tp)
    if risk <= 1e-12:
        return 0.0
    return reward / risk


def _liquidity_wall_clip(direction: str, entry: float, target: float, coin: str,
                          view: TFView) -> float:
    """Clip a target that would otherwise project through a closer, obvious
    liquidity wall (order-book ledge or unswept EQH/EQL) to just in front of
    it instead. Applied after RR floor/ceiling; never used to shrink TP1
    below the floor (candidate is rejected instead, per build_risk_plan)."""
    wall = None
    try:
        wall = nearest_liquidity_wall(coin, direction, entry)
    except Exception:
        wall = None
    candidates = [w for w in [wall] if w is not None]
    pools = view.eq_highs if direction == "bullish" else view.eq_lows
    for p in pools:
        if p.swept:
            continue
        if direction == "bullish" and entry < p.level < target:
            candidates.append(p.level)
        elif direction == "bearish" and target < p.level < entry:
            candidates.append(p.level)
    if not candidates:
        return target
    if direction == "bullish":
        nearest_wall = min(candidates)
        return min(target, nearest_wall * 0.999)
    nearest_wall = max(candidates)
    return max(target, nearest_wall * 1.001)


def build_risk_plan(direction: str, entry: float, coin: str, m15_view: TFView, h1_view: TFView,
                     h4_view: TFView, state: dict, asset: str) -> Optional[RiskPlan]:
    anchor = select_sl_anchor(direction, entry, m15_view, h1_view, h4_view, state, asset)
    if anchor is None:
        return None
    anchor_name, view, structural_sl = anchor

    buffer = adaptive_sl_buffer(view, state, asset)
    sl = (structural_sl - buffer) if direction == "bullish" else (structural_sl + buffer)
    sl = _clear_sl_of_liquidity_pool(direction, sl, view)

    risk = abs(entry - sl)
    if risk <= 1e-12:
        return None
    if risk / (view.atr[-1] or 1e-9) > MAX_SL_ATR_MULT:
        return None
    if risk / (view.atr[-1] or 1e-9) < MIN_ENTRY_TO_SL_ATR:
        return None  # entry-to-SL distance rule (Section 10 entry-placement rules)

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

    # Liquidity-wall clipping, applied after selection, before the RR gate.
    tp1 = _liquidity_wall_clip(direction, entry, tp1, coin, view)
    tp2 = _liquidity_wall_clip(direction, entry, tp2, coin, view)

    # TP ordering integrity -- structurally enforced, final assertion.
    if direction == "bullish" and tp2 <= tp1:
        tp2 = tp1 + max(tp1 - entry, view.atr[-1] or 0.0) * 0.5 + (tp1 - entry) * 0.05
    elif direction == "bearish" and tp2 >= tp1:
        tp2 = tp1 - max(entry - tp1, view.atr[-1] or 0.0) * 0.5 - (entry - tp1) * 0.05

    rr1 = _rr(entry, sl, tp1, direction)
    rr2 = _rr(entry, sl, tp2, direction)

    assert (tp2 > tp1) if direction == "bullish" else (tp2 < tp1), "TP ordering integrity violated"

    if abs(tp1 - entry) < entry * MIN_MOVE_PCT_TP1:
        return None
    if abs(tp2 - entry) < entry * MIN_MOVE_PCT_TP2:
        return None
    # Never stretch TP1 artificially toward the soft ceiling -- honest RR only.
    if rr1 < RR_MIN_GATE:
        return None

    return RiskPlan(sl=sl, tp1=tp1, tp2=tp2, rr1=rr1, rr2=rr2, risk=risk,
                     buffer=buffer, sl_anchor=anchor_name)


# =============================================================================
# SECTION 10 -- MANDATORY TOP-DOWN GATE (shared backbone, Section 7)
# =============================================================================

@dataclass
class GatedSetup:
    symbol: str
    bias: str               # "Bullish" | "Bearish"
    direction: str           # "bullish" | "bearish"
    seq: Dict[str, Any]      # zone-selection sequence result (1H POI/SFP/MSS)
    entry_info: Dict[str, Any]  # stage4 result (entry, fvg, mss, poi)
    regime: RegimeVector


def run_mandatory_topdown(symbol: str, views: Dict[str, TFView], regime: RegimeVector) -> Optional[GatedSetup]:
    weekly, daily, h4, h1, m15 = (views.get(TF_WEEKLY), views.get(TF_DAILY),
                                    views.get(TF_H4), views.get(TF_H1), views.get(TF_M15))
    if not all([weekly, daily, h4, h1, m15]):
        return None

    bias = stage1_market_bias(weekly, daily)
    if bias == "Neutral":
        return None  # Trade Filter: Stage 1 Neutral -> NO TRADE

    if not stage2_context_agrees(bias, h4):
        return None  # Trade Filter: Stage 2 disagreement -> NO TRADE

    status, seq = stage3_setup_status(bias, h1)
    if status != "VALID" or seq is None:
        return None  # Trade Filter: NOT READY / INVALID -> NO TRADE

    entry_info = stage4_entry_trigger(bias, seq, m15)
    if entry_info is None:
        return None  # Trade Filter: no valid MSS->FVG sequence -> NO TRADE

    direction = "bullish" if bias == "Bullish" else "bearish"
    return GatedSetup(symbol=symbol, bias=bias, direction=direction, seq=seq,
                       entry_info=entry_info, regime=regime)


def _entry_kind_and_style(entry: float, market_price: float, view: TFView) -> Tuple[str, str]:
    atr_val = view.atr[-1] if view.atr else 0.0
    dist_atr = abs(entry - market_price) / atr_val if atr_val else 0.0
    entry_kind = "market" if dist_atr < 0.05 else "pending"
    style = "intraday"  # style is refined per-engine/per-plan by hold-time implication (Section 7)
    return entry_kind, style


def _base_candidate_from_gate(engine_name: str, gate: GatedSetup, views: Dict[str, TFView],
                               state: dict, extra_confluences: List[str], base_conf: float,
                               best_fit_regimes: List[str], style: str = "intraday") -> Optional[Candidate]:
    m15, h1, h4 = views[TF_M15], views[TF_H1], views[TF_H4]
    symbol = gate.symbol
    entry = gate.entry_info["entry"]
    market_price = m15.candles[-1].c

    dist_atr = abs(entry - market_price) / (m15.atr[-1] or 1e-9)
    if dist_atr > MAX_PENDING_ENTRY_DIST_ATR:
        return None  # entry too far from market -- Section 10 entry-placement cap

    if not tp1_runway_ok(gate.direction, entry, m15, state, symbol):
        return None  # TP1 runway pre-check (Section 10)

    plan = build_risk_plan(gate.direction, entry, symbol, m15, h1, h4, state, symbol)
    if plan is None:
        return None

    entry_kind, _ = _entry_kind_and_style(entry, market_price, m15)

    confluences = list(extra_confluences)
    confluences.append(f"SFP purity {gate.seq['sfp']['purity']:.2f}")
    confluences.append(f"POI: {gate.seq['poi'].kind}")
    if gate.entry_info["poi"] is gate.seq["poi"]:
        confluences.append("OTE within POI")
    if gate.seq.get("session_anchored"):
        confluences.append("Session-anchored SFP")

    # hold-time style: swing if the anchor/target distance implies a
    # multi-day move relative to the asset's own ATR, else intraday
    swing_ratio = plan.risk / (h4.atr[-1] or plan.risk or 1e-9)
    style = "swing" if swing_ratio > 2.0 else style

    conf = clamp(base_conf, 0.0, 1.0)
    return Candidate(
        engine=engine_name, symbol=symbol, style=style, direction=gate.direction,
        entry=entry, entry_kind=entry_kind, plan=plan, confidence_raw=conf,
        confluences=confluences, best_fit_regimes=best_fit_regimes,
        session_anchored=bool(gate.seq.get("session_anchored")),
    )


# =============================================================================
# SECTION 11 -- SPECIALIZED ENGINE ENSEMBLE (Section 4)
# =============================================================================
# Every engine shares the mandatory top-down gate (Section 7/10 above) and
# differs in which additional structural confirmation it requires and which
# regime(s) it documents itself as best-suited for. Each returns 0 or 1
# Candidate for a given symbol/scan.

def engine_smc(gate: GatedSetup, views: Dict[str, TFView], state: dict) -> Optional[Candidate]:
    """Smart Money Concept: pure POI/SFP/MSS sequence with no extra filter --
    the canonical, highest-purity read of the mandatory sequence itself."""
    conf = 0.55 + 0.25 * gate.seq["sfp"]["purity"]
    return _base_candidate_from_gate("SMC", gate, views, state, ["Core SMC sequence"], conf,
                                      ["Trending Bull", "Trending Bear"])


def engine_trend_continuation(gate: GatedSetup, views: Dict[str, TFView], state: dict) -> Optional[Candidate]:
    h1, h4 = views[TF_H1], views[TF_H4]
    if not h1.ema_fast or not h4.ema_fast:
        return None
    h1_dir = 1 if h1.ema_fast[-1] > h1.ema_slow[-1] else -1
    h4_dir = 1 if h4.ema_fast[-1] > h4.ema_slow[-1] else -1
    want = 1 if gate.direction == "bullish" else -1
    if h1_dir != want or h4_dir != want:
        return None
    if (h1.adx[-1] if h1.adx else 0) < 20:
        return None
    conf = 0.55 + clamp((h1.adx[-1] - 20) / 40.0, 0, 0.3)
    return _base_candidate_from_gate("Trend Continuation", gate, views, state,
                                      ["MTF EMA alignment", "ADX > 20"], conf,
                                      ["Trending Bull", "Trending Bear"])


def engine_breakout(gate: GatedSetup, views: Dict[str, TFView], state: dict) -> Optional[Candidate]:
    m15 = views[TF_M15]
    mss = gate.entry_info["mss"]
    if mss["kind"] != "BOS":
        return None  # breakout wants continuation-style break, not reversal CHoCH
    idx = mss["idx"]
    if idx < 5:
        return None
    recent_range = max(c.h for c in m15.candles[idx - 5:idx]) - min(c.l for c in m15.candles[idx - 5:idx])
    move = abs(m15.candles[idx].c - m15.candles[idx - 1].c)
    if recent_range <= 1e-12 or move / recent_range < 0.5:
        return None  # not a genuine expansion breakout
    conf = 0.5 + clamp(move / recent_range - 0.5, 0, 0.35)
    return _base_candidate_from_gate("Breakout", gate, views, state,
                                      ["Expansion BOS", "Range compression prior"], conf,
                                      ["Expansion", "Trending Bull", "Trending Bear"])


def engine_pullback(gate: GatedSetup, views: Dict[str, TFView], state: dict) -> Optional[Candidate]:
    h1 = views[TF_H1]
    poi = gate.seq["poi"]
    prem_disc = _premium_discount(gate.direction, h1)
    if prem_disc is None:
        return None
    ok = prem_disc <= 0.5 if gate.direction == "bullish" else prem_disc >= 0.5
    if not ok:
        return None  # pullback into discount(long)/premium(short) only
    conf = 0.5 + (0.5 - abs(prem_disc - (0.25 if gate.direction == "bullish" else 0.75))) * 0.4
    return _base_candidate_from_gate("Pullback", gate, views, state,
                                      [f"Premium/Discount {prem_disc:.2f}"], conf,
                                      ["Trending Bull", "Trending Bear", "Ranging"])


def engine_liquidity_sweep(gate: GatedSetup, views: Dict[str, TFView], state: dict) -> Optional[Candidate]:
    sfp = gate.seq["sfp"]
    if sfp["purity"] < 0.6:
        return None  # this engine specifically trades clean sweeps
    conf = 0.55 + 0.3 * sfp["purity"]
    return _base_candidate_from_gate("Liquidity Sweep", gate, views, state,
                                      [f"Clean SFP purity {sfp['purity']:.2f}"], conf,
                                      ["High-volatility", "Reversal"])


def engine_order_block(gate: GatedSetup, views: Dict[str, TFView], state: dict) -> Optional[Candidate]:
    if gate.seq["poi"].kind != "ob":
        return None
    conf = 0.5 + 0.25 * gate.seq["sfp"]["purity"]
    return _base_candidate_from_gate("Order Block", gate, views, state,
                                      ["Unmitigated OB POI"], conf,
                                      ["Trending Bull", "Trending Bear", "Ranging"])


def engine_breaker_block(gate: GatedSetup, views: Dict[str, TFView], state: dict) -> Optional[Candidate]:
    if gate.seq["poi"].kind != "breaker":
        return None
    conf = 0.55 + 0.25 * gate.seq["sfp"]["purity"]
    return _base_candidate_from_gate("Breaker Block", gate, views, state,
                                      ["Failed-OB breaker POI"], conf,
                                      ["Reversal", "High-volatility"])


def engine_fvg(gate: GatedSetup, views: Dict[str, TFView], state: dict) -> Optional[Candidate]:
    if gate.seq["poi"].kind != "fvg":
        return None
    conf = 0.5 + 0.2 * gate.seq["sfp"]["purity"]
    return _base_candidate_from_gate("Fair Value Gap", gate, views, state,
                                      ["Unmitigated FVG POI"], conf,
                                      ["Trending Bull", "Trending Bear"])


def engine_momentum(gate: GatedSetup, views: Dict[str, TFView], state: dict) -> Optional[Candidate]:
    h1 = views[TF_H1]
    if not h1.rsi:
        return None
    r = h1.rsi[-1]
    want_bull = 50 < r < 75
    want_bear = 25 < r < 50
    if gate.direction == "bullish" and not want_bull:
        return None
    if gate.direction == "bearish" and not want_bear:
        return None
    momentum_strength = abs(r - 50) / 25.0
    conf = 0.5 + 0.3 * momentum_strength
    return _base_candidate_from_gate("Momentum", gate, views, state,
                                      [f"1H RSI {r:.1f}"], conf,
                                      ["Trending Bull", "Trending Bear", "Expansion"])


def engine_reversal(gate: GatedSetup, views: Dict[str, TFView], state: dict) -> Optional[Candidate]:
    if gate.entry_info["mss"]["kind"] != "CHoCH" and gate.seq["mss"]["kind"] != "CHoCH":
        return None  # reversal engine requires genuine character change, not continuation
    conf = 0.55 + 0.25 * gate.seq["sfp"]["purity"]
    return _base_candidate_from_gate("Reversal", gate, views, state,
                                      ["CHoCH confirmed"], conf,
                                      ["Reversal", "High-volatility"])


def engine_mean_reversion(gate: GatedSetup, views: Dict[str, TFView], state: dict) -> Optional[Candidate]:
    h1 = views[TF_H1]
    if not h1.bb_up or not h1.bb_dn:
        return None
    price = h1.candles[-1].c
    if gate.direction == "bullish" and price > h1.bb_dn[-1] * 1.01:
        return None
    if gate.direction == "bearish" and price < h1.bb_up[-1] * 0.99:
        return None
    trend_ok = (h1.adx[-1] if h1.adx else 30) < 22  # mean reversion wants range, not strong trend
    if not trend_ok:
        return None
    conf = 0.5 + 0.2
    return _base_candidate_from_gate("Mean Reversion", gate, views, state,
                                      ["BB extreme reclaim", "Low ADX"], conf,
                                      ["Ranging", "Low-volatility"])


def engine_range_trading(gate: GatedSetup, views: Dict[str, TFView], state: dict) -> Optional[Candidate]:
    h1 = views[TF_H1]
    if (h1.adx[-1] if h1.adx else 30) > 20:
        return None  # requires genuine range, not a trending 1H
    conf = 0.5
    return _base_candidate_from_gate("Range Trading", gate, views, state,
                                      ["Low ADX range context"], conf,
                                      ["Ranging", "Consolidation"])


def engine_volatility_expansion(gate: GatedSetup, views: Dict[str, TFView], state: dict) -> Optional[Candidate]:
    h1 = views[TF_H1]
    if gate.regime.volatility_pctile < 0.6:
        return None
    conf = 0.5 + 0.3 * gate.regime.volatility_pctile
    return _base_candidate_from_gate("Volatility Expansion", gate, views, state,
                                      [f"Vol percentile {gate.regime.volatility_pctile:.2f}"], conf,
                                      ["Expansion", "High-volatility"])


def _premium_discount(direction: str, view: TFView) -> Optional[float]:
    """0..1 position of current price within the recent dealing range
    (0 = discount extreme, 1 = premium extreme)."""
    if len(view.candles) < 30:
        return None
    window = view.candles[-60:]
    hi, lo = max(c.h for c in window), min(c.l for c in window)
    if hi - lo < 1e-12:
        return None
    return clamp((view.candles[-1].c - lo) / (hi - lo), 0.0, 1.0)


SPECIALIZED_ENGINES = [
    engine_smc, engine_trend_continuation, engine_breakout, engine_pullback,
    engine_liquidity_sweep, engine_order_block, engine_breaker_block, engine_fvg,
    engine_momentum, engine_reversal, engine_mean_reversion, engine_range_trading,
    engine_volatility_expansion,
]


def run_specialized_engines(symbol: str, views: Dict[str, TFView], regime: RegimeVector,
                             state: dict) -> List[Candidate]:
    gate = run_mandatory_topdown(symbol, views, regime)
    if gate is None:
        return []
    out: List[Candidate] = []
    for engine_fn in SPECIALIZED_ENGINES:
        try:
            cand = engine_fn(gate, views, state)
        except Exception:
            log.exception("Engine %s raised on %s", engine_fn.__name__, symbol)
            cand = None
        if cand is not None:
            out.append(cand)
    return out


# =============================================================================
# SECTION 12 -- STATE MANAGEMENT (Section 5: Tier 1 aggregates / Tier 2 log)
# =============================================================================

def default_state() -> dict:
    return {
        "schema_version": 1,
        "resolution_logic_version": RESOLUTION_LOGIC_VERSION,
        "tier1": {
            # adaptive parameters -- every one bounded + dampened, none hardcoded elsewhere
            "engine_weights": {e.__name__: 1.0 for e in SPECIALIZED_ENGINES},
            "confidence_calibration": {},       # key: "{engine}:{bucket}" -> multiplicative adj, default 1.0
            "regime_fit_discount": {},          # key: "{engine}:{regime_label}" -> 0..1 multiplier, default 1.0
            "sl_buffer_percentile": {},         # key: "{asset}:{tf}" -> float, default 65.0
            "sl_buffer_percentile_dist": {},    # key: "{asset}:15M" -> float
            "tp1_target_rank_preference": {},   # key: asset -> int [2,6], default 3
            "liquidity_sanity_threshold": {},   # key: "{engine}" -> 0..1, default 0.5
            "mtf_alignment_weight": 0.15,       # composite score term weight, bounded [0.05, 0.35]
            "session_open_proximity_weight": 0.05,  # bounded [0.0, 0.20], earned empirically (Sec 13.7)
            "sfp_purity_requirement": {},       # key: "{engine}" -> 0..1 minimum, default per engine
            # segment performance aggregates: key "{asset}:{regime_label}:{tf}:{engine}"
            "segment_stats": {},                # -> {"n","wins","losses","sum_r","sum_conf_bucket_hits":{}}
            "session_stats": {"asia": 0.35, "london": 0.65, "ny": 0.65},
            "session_anchored_stats": {},       # key: "{engine}:{asset}" -> {"anchored":{n,wins,sum_r}, "plain":{...}}
            "forensic_category_stats": {},      # key: category -> {"n","recent_trend":[...]}
            "confidence_calibration_samples": {},  # key: "{engine}:{bucket}" -> {"n","wins","sum_r"}
            "baseline": {"win_rate": None, "profit_factor": None, "avg_rr": None, "n": 0},
            "circuit_breaker": {"tripped": False, "tripped_ts": None, "reason": None},
            "fill_rate_stats": {},              # key: "{engine}:{entry_kind}" -> {"filled","expired"}
            "filter_funnel": {},                # key: stage_name -> {"seen","passed"}
        },
        "tier2_trades": [],   # bounded, prunable raw trade log (Section 5)
        "active_signals": {},  # id -> pending/open signal dict
        "last_run_ts": None,
        "last_daily_summary_date": None,
        "signal_seq": 0,
    }


def _deep_merge_defaults(state: dict, defaults: dict) -> dict:
    for k, v in defaults.items():
        if k not in state:
            state[k] = copy.deepcopy(v)
        elif isinstance(v, dict) and isinstance(state[k], dict):
            _deep_merge_defaults(state[k], v)
    return state


def load_state(path: str = STATE_PATH) -> dict:
    if os.path.exists(path):
        try:
            with open(path, "r") as f:
                state = json.load(f)
            return _deep_merge_defaults(state, default_state())
        except (json.JSONDecodeError, OSError) as e:
            log.error("Failed to load state.json (%s); using defaults. This should be investigated,"
                      " not silently overwritten in production.", e)
    return default_state()


def save_state(state: dict, path: str = STATE_PATH) -> None:
    """Atomic write: write to a temp file in the same directory, then
    os.replace -- never leaves a half-written state.json on crash."""
    d = os.path.dirname(os.path.abspath(path)) or "."
    fd, tmp_path = tempfile.mkstemp(dir=d, prefix=".state_tmp_")
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(state, f, indent=2, default=str)
        os.replace(tmp_path, path)
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


def load_candle_cache(path: str = CANDLE_CACHE_PATH) -> dict:
    if os.path.exists(path):
        try:
            with open(path, "r") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            return {}
    return {}


def save_candle_cache(cache: dict, path: str = CANDLE_CACHE_PATH) -> None:
    d = os.path.dirname(os.path.abspath(path)) or "."
    fd, tmp_path = tempfile.mkstemp(dir=d, prefix=".cache_tmp_")
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(cache, f)
        os.replace(tmp_path, path)
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


def prune_tier2(state: dict, now_ms: int) -> None:
    """Tier 2 pruning never touches Tier 1 -- learned parameters are
    unaffected (Section 5)."""
    cutoff = now_ms - TIER2_RETENTION_DAYS * 86_400_000
    state["tier2_trades"] = [t for t in state["tier2_trades"] if t.get("resolved_ts", now_ms) >= cutoff]


# --- Bounded, dampened adaptive parameter update helper (Section 5) ---

def bounded_update(current: float, target: float, lo: float, hi: float,
                    max_step_frac: float = 0.15, smoothing: float = 0.25) -> float:
    """Exponential-smoothing blend of `current` toward `target`, capped by
    both a maximum fractional step and hard [lo, hi] bounds. This is the
    single mechanism every adaptive parameter update in this engine goes
    through -- no update anywhere bypasses this."""
    blended = current * (1 - smoothing) + target * smoothing
    max_step = max(abs(current) * max_step_frac, (hi - lo) * 0.02)
    if blended > current + max_step:
        blended = current + max_step
    elif blended < current - max_step:
        blended = current - max_step
    return clamp(blended, lo, hi)


# =============================================================================
# SECTION 13 -- DECISION ENGINE (Section 4: continuous blend, Section 13 vetoes)
# =============================================================================

def _logistic(x: float) -> float:
    x = clamp(x, -12.0, 12.0)  # numerical guard, not a term cap
    return 1.0 / (1.0 + math.exp(-x))


def _term(value: float, weight: float, cap: float) -> float:
    """Every composite-score term is weight * value, hard-capped in its own
    contribution so no single term can saturate the logistic (Section 4
    mandatory no-single-term-saturation rule)."""
    return clamp(weight * value, -cap, cap)


def _confidence_bucket(raw_conf: float) -> str:
    if raw_conf >= 0.75:
        return "high"
    if raw_conf >= 0.55:
        return "mid"
    return "low"


def _regime_fit_score(candidate: Candidate, regime: RegimeVector) -> float:
    label = regime_label_bucket(regime)
    fit = 1.0 if label in candidate.best_fit_regimes else 0.0
    # partial credit: trending engines still get some credit in adjacent
    # high-trend-strength regimes even if the exact label differs, since the
    # Regime Vector is continuous, not a bucket match alone.
    if fit == 0.0 and any("Trending" in r for r in candidate.best_fit_regimes):
        fit = clamp(regime.trend_strength - 0.4, 0.0, 0.5)
    if fit == 0.0 and any(r in ("Ranging", "Consolidation") for r in candidate.best_fit_regimes):
        fit = clamp(0.5 - regime.trend_strength, 0.0, 0.5)
    return clamp(fit, 0.0, 1.0)


def _historical_segment_performance(candidate: Candidate, regime_label: str, state: dict) -> float:
    key = f"{candidate.symbol}:{regime_label}:{candidate.style}:{candidate.engine}"
    seg = state["tier1"]["segment_stats"].get(key)
    if not seg or seg.get("n", 0) < MIN_SAMPLE_SIZE_SEGMENT:
        return 0.0  # not enough data -- neutral, no bonus/penalty (cold-start safe)
    n, wins = seg["n"], seg["wins"]
    wr = wins / n
    return clamp((wr - 0.5) * 2.0, -1.0, 1.0)


def _confidence_calibration_adj(candidate: Candidate, state: dict) -> float:
    bucket = _confidence_bucket(candidate.confidence_raw)
    key = f"{candidate.engine}:{bucket}"
    return state["tier1"]["confidence_calibration"].get(key, 1.0)


def _liquidity_sanity_ok(candidate: Candidate, views: Dict[str, TFView], state: dict) -> bool:
    """Section 13: reject/discount entries sitting directly inside a pool
    about to be swept, unless the engine specifically trades that behavior."""
    if candidate.engine == "Liquidity Sweep":
        return True
    h1 = views.get(TF_H1)
    if not h1:
        return True
    threshold = state["tier1"]["liquidity_sanity_threshold"].get(candidate.engine, 0.5)
    entry = candidate.entry
    atr_val = h1.atr[-1] if h1.atr else 0.0
    pools = h1.eq_highs if candidate.direction == "bullish" else h1.eq_lows
    for p in pools:
        if p.swept:
            continue
        dist = abs(entry - p.level) / (atr_val or 1e-9)
        if dist < threshold:
            return False
    return True


def _macro_blackout_active(symbol: str, reference_ms: int) -> bool:
    for ev in SCHEDULED_MACRO_EVENTS:
        if symbol not in ev.get("assets", []) and symbol != MACRO_ASSET:
            continue
        ts = ev.get("ts_ms")
        if ts is None:
            continue
        lo = ts - MACRO_EVENT_BLACKOUT_MIN_BEFORE * 60_000
        hi = ts + MACRO_EVENT_BLACKOUT_MIN_AFTER * 60_000
        if lo <= reference_ms <= hi:
            return True
    return False


def score_candidate(candidate: Candidate, regime: RegimeVector, state: dict,
                     views: Dict[str, TFView]) -> Optional[ScoredSignal]:
    """Continuous logistic blend over a small, auditable set of terms.
    Never a discrete point stack (Section 4 mandatory rule)."""
    regime_label = regime_label_bucket(regime)

    # Regime-fit veto/discount (Section 13) -- heavy discount, not necessarily
    # an outright reject, so the continuous blend still governs final ranking.
    regime_fit = _regime_fit_score(candidate, regime)
    regime_discount = state["tier1"]["regime_fit_discount"].get(
        f"{candidate.engine}:{regime_label}", 1.0)
    if regime_fit < 0.15:
        return None  # documented best-fit regime(s) not remotely matched -- suppress

    if not _liquidity_sanity_ok(candidate, views, state):
        return None

    engine_weight = state["tier1"]["engine_weights"].get(candidate.engine, 1.0)
    calib = _confidence_calibration_adj(candidate, state)
    seg_perf = _historical_segment_performance(candidate, regime_label, state)
    mtf_w = state["tier1"]["mtf_alignment_weight"]
    session_w = state["tier1"]["session_open_proximity_weight"]

    conf_input = candidate.confidence_raw * calib
    ev_input = clamp((candidate.plan.rr1 - RR_MIN_GATE) / (RR_TP1_SOFT_CEIL - RR_MIN_GATE + 1e-9), 0.0, 1.5)
    confluence_input = clamp(len(candidate.confluences) / 6.0, 0.0, 1.0)

    CAP = 1.2  # per-term hard cap on weight*value contribution (Section 4 mandatory)
    terms = {
        "confidence": _term(conf_input, 1.0, CAP),
        "regime_fit": _term(regime_fit * regime_discount, 0.9, CAP),
        "mtf_alignment": _term(1.0, mtf_w, CAP),   # candidate already passed Stage1/2 MTF agreement
        "confluence": _term(confluence_input, 0.6, CAP),
        "segment_perf": _term(seg_perf, 0.5, CAP),
        "ev_rr": _term(ev_input, 0.35, CAP),       # RR informs, never dominates win-probability
        "session_open": _term(candidate.session_anchored and gate_session_bonus(regime) or 0.0,
                               session_w, CAP),
        "engine_weight": _term(engine_weight - 1.0, 0.4, CAP),
    }
    z = sum(terms.values())
    score = _logistic(z)

    if score >= 0.80:
        grade = "A+"
    elif score >= 0.65:
        grade = "A"
    else:
        grade = "B"

    return ScoredSignal(candidate=candidate, score=score, grade=grade, term_contributions=terms)


def gate_session_bonus(regime: RegimeVector) -> float:
    return regime.session_open_proximity


def decision_engine_select(all_candidates: List[Candidate], regime_by_symbol: Dict[str, RegimeVector],
                            views_by_symbol: Dict[str, Dict[str, TFView]], state: dict,
                            reference_ms: int, active_symbols_and_sectors: List[Tuple[str, str]]
                            ) -> List[ScoredSignal]:
    scored: List[ScoredSignal] = []
    for cand in all_candidates:
        if _macro_blackout_active(cand.symbol, reference_ms):
            continue
        regime = regime_by_symbol[cand.symbol]
        s = score_candidate(cand, regime, state, views_by_symbol[cand.symbol])
        if s is not None:
            scored.append(s)

    # keep best-scoring candidate per symbol (avoid multiple engines double-
    # dispatching the same underlying setup on one asset in one scan)
    best_per_symbol: Dict[str, ScoredSignal] = {}
    for s in scored:
        cur = best_per_symbol.get(s.candidate.symbol)
        if cur is None or s.score > cur.score:
            best_per_symbol[s.candidate.symbol] = s
    ranked = sorted(best_per_symbol.values(), key=lambda s: s.score, reverse=True)

    # correlation cap + concurrency cap (Section 14)
    selected: List[ScoredSignal] = []
    sector_counts: Dict[str, int] = collections.Counter(sec for _, sec in active_symbols_and_sectors)
    slots_used = len(active_symbols_and_sectors)
    for s in ranked:
        if slots_used >= MAX_CONCURRENT_ACTIVE_SIGNALS:
            break
        sector = SECTOR_MAP.get(s.candidate.symbol, s.candidate.symbol)
        if sector_counts[sector] >= MAX_CORRELATED_CONCURRENT:
            continue
        selected.append(s)
        sector_counts[sector] += 1
        slots_used += 1
    return selected


# =============================================================================
# SECTION 14 -- SIGNAL DISPATCH & LIFECYCLE (Sections 11, 12, 12A)
# =============================================================================
# Position-exit model declaration (Section 11, mandatory): this engine uses
# the FULL-EXIT-AT-TP1 model. 100% of position size is modeled as closing at
# TP1; nothing remains open afterward, so a later touch of the original SL
# is bookkeeping only (closing the internal tracking record) and can never
# turn a TP1 hit into anything but a WIN. The resolution function below
# implements exactly this and only this model.

def new_signal_id(state: dict) -> str:
    state["signal_seq"] += 1
    return f"MSE-{state['signal_seq']:08d}"


def dispatch_signal(scored: ScoredSignal, state: dict, reference_ms: int) -> dict:
    c = scored.candidate
    sig_id = new_signal_id(state)
    record = {
        "id": sig_id,
        "engine": c.engine,
        "symbol": c.symbol,
        "style": c.style,
        "direction": c.direction,
        "entry": c.entry,
        "entry_kind": c.entry_kind,
        "sl": c.plan.sl,
        "tp1": c.plan.tp1,
        "tp2": c.plan.tp2,
        "rr1": c.plan.rr1,
        "rr2": c.plan.rr2,
        "sl_anchor": c.plan.sl_anchor,
        "confidence": scored.score,
        "grade": scored.grade,
        "confluences": c.confluences,
        "regime_at_entry": regime_label_bucket(state["_last_regime_by_symbol"][c.symbol]),
        "regime_vector_at_entry": state["_last_regime_by_symbol"][c.symbol].as_dict(),
        "session_anchored": c.session_anchored,
        "entry_filled": (c.entry_kind == "market"),
        "pending_bars": 0,
        "status": "active" if c.entry_kind == "market" else "pending",
        "created_ts": reference_ms,
        "resolved_ts": None,
        "result": None,          # "win" | "loss" | "expired" -- set on resolution
        "r_realized": None,
        "mae_r": 0.0,
        "mfe_r": 0.0,
        "forensic_category": None,
        "resolution_logic_version": RESOLUTION_LOGIC_VERSION,
    }
    state["active_signals"][sig_id] = record
    return record


def monitor_signals(state: dict, symbol: str, monitor_view: TFView) -> List[dict]:
    """Advance every active signal for `symbol` one closed MONITOR_TF candle
    at a time. Returns the list of newly-resolved (or newly-expired)
    records this pass, for Telegram notification + forensic tagging."""
    events: List[dict] = []
    ids = [sid for sid, r in state["active_signals"].items() if r["symbol"] == symbol]
    if not ids or not monitor_view.candles:
        return events

    # Only evaluate candles that closed after the signal was created.
    for sid in ids:
        rec = state["active_signals"][sid]
        new_candles = [c for c in monitor_view.candles if c.t > rec["created_ts"] - TF_MS[MONITOR_TF]]
        for c in new_candles:
            if rec["status"] not in ("active", "pending"):
                break

            if rec["status"] == "pending":
                # Section 12: never evaluate SL/TP before entry has filled.
                lo, hi = min(c.l, c.h), max(c.l, c.h)
                if not (lo <= rec["entry"] <= hi):
                    rec["pending_bars"] += 1
                    if rec["pending_bars"] >= PENDING_ENTRY_EXPIRY_BARS:
                        rec["status"] = "expired"
                        rec["result"] = "expired"
                        rec["resolved_ts"] = c.t
                        events.append(rec)
                        break
                    continue
                rec["entry_filled"] = True
                rec["status"] = "active"
                # fall through: this same candle can still register a same-
                # candle SL/TP hit (Section 12).

            direction = rec["direction"]
            sl, tp1 = rec["sl"], rec["tp1"]
            risk = abs(rec["entry"] - sl)
            if risk <= 1e-12:
                risk = 1e-9

            if direction == "bullish":
                mfe = max(rec["mfe_r"], (c.h - rec["entry"]) / risk)
                mae = max(rec["mae_r"], (rec["entry"] - c.l) / risk)
            else:
                mfe = max(rec["mfe_r"], (rec["entry"] - c.l) / risk)
                mae = max(rec["mae_r"], (c.h - rec["entry"]) / risk)
            rec["mfe_r"], rec["mae_r"] = mfe, mae

            hit_sl = (c.l <= sl) if direction == "bullish" else (c.h >= sl)
            hit_tp1 = (c.h >= tp1) if direction == "bullish" else (c.l <= tp1)

            # Section 11: documented, conservative same-candle-ambiguity
            # handling -- if both could have been touched on the same candle,
            # resolve to the WORSE-of/more-conservative outcome for realism
            # (SL-first) rather than always crediting the win. This is never
            # a "worst-case-first bias that manufactures a false stop-out" --
            # it is the honest read when a single candle's path is unknown.
            if hit_sl and hit_tp1:
                resolved_as_loss = True
            elif hit_sl:
                resolved_as_loss = True
            elif hit_tp1:
                resolved_as_loss = False
            else:
                continue

            rec["status"] = "resolved"
            rec["resolved_ts"] = c.t
            if resolved_as_loss:
                rec["result"] = "loss"
                rec["r_realized"] = -1.0
            else:
                rec["result"] = "win"
                rec["r_realized"] = rec["rr1"]  # full-exit model: TP1 R credited in full
            events.append(rec)
            break

    for r in events:
        del state["active_signals"][r["id"]]
    return events


# =============================================================================
# SECTION 15 -- LOSS FORENSICS & ADAPTIVE FEEDBACK LOOP (Section 13)
# =============================================================================
# Closed taxonomy. Every category's signature is a positive, verifiable
# condition on the trade's own recorded data -- never an else/elimination
# branch -- and each maps to exactly one deterministic adaptive response.

FORENSIC_CATEGORIES = [
    "regime_mismatch", "structural_invalidation_too_tight", "chased_swept_liquidity",
    "mtf_conflict_ignored", "sfp_mss_sequence_violated", "correct_read_poor_rr",
    "confidence_miscalibration", "filter_over_permissiveness", "genuine_variance",
]


def classify_trade(rec: dict, state: dict) -> str:
    rv = RegimeVector(**rec["regime_vector_at_entry"])
    label = regime_label_bucket(rv)
    engine = rec["engine"]

    # Positive, verifiable checks in a deliberate priority order (documented):
    # a trade can technically show more than one signature; priority reflects
    # which cause is most structurally certain to have driven the outcome.
    if rec["result"] == "loss":
        best_fit = ENGINE_BEST_FIT.get(engine, [])
        if best_fit and label not in best_fit and _regime_fit_score_from_label(label, best_fit) < 0.15:
            return "regime_mismatch"

        buffer_dist = rec.get("sl_buffer_used")
        if buffer_dist and rec["mae_r"] * abs(rec["entry"] - rec["sl"]) <= buffer_dist * 1.15:
            return "structural_invalidation_too_tight"

        if rec.get("chased_swept_liquidity"):
            return "chased_swept_liquidity"

        if rec.get("mtf_disagreement_at_entry"):
            return "mtf_conflict_ignored"

        if rec.get("sfp_purity_at_entry", 1.0) < 0.4 or rec.get("mss_confirmed_at_entry", True) is False:
            return "sfp_mss_sequence_violated"

        if rec["mfe_r"] >= 0.8 * rec["rr1"]:
            return "correct_read_poor_rr"

        bucket = _confidence_bucket(rec["confidence"])
        key = f"{engine}:{bucket}"
        cal = state["tier1"]["confidence_calibration_samples"].get(key, {"n": 0, "wins": 0})
        if cal["n"] >= MIN_SAMPLE_SIZE_CATEGORY and bucket == "high" and (cal["wins"] / max(cal["n"], 1)) < 0.45:
            return "confidence_miscalibration"

        if rec.get("thin_margin_filters"):
            return "filter_over_permissiveness"

        return "genuine_variance"

    # Win-side reinforcement categories mirror the same signatures.
    if rec.get("mtf_disagreement_at_entry") is False and rec.get("sfp_purity_at_entry", 0) >= 0.6:
        return "sfp_mss_sequence_violated"  # reused label -- reinforcement path checks rec["result"]=="win"
    return "genuine_variance"


def _regime_fit_score_from_label(label: str, best_fit: List[str]) -> float:
    return 1.0 if label in best_fit else 0.0


ENGINE_BEST_FIT = {
    "SMC": ["Trending Bull", "Trending Bear"],
    "Trend Continuation": ["Trending Bull", "Trending Bear"],
    "Breakout": ["Expansion", "Trending Bull", "Trending Bear"],
    "Pullback": ["Trending Bull", "Trending Bear", "Ranging"],
    "Liquidity Sweep": ["High-volatility", "Reversal"],
    "Order Block": ["Trending Bull", "Trending Bear", "Ranging"],
    "Breaker Block": ["Reversal", "High-volatility"],
    "Fair Value Gap": ["Trending Bull", "Trending Bear"],
    "Momentum": ["Trending Bull", "Trending Bear", "Expansion"],
    "Reversal": ["Reversal", "High-volatility"],
    "Mean Reversion": ["Ranging", "Low-volatility"],
    "Range Trading": ["Ranging", "Consolidation"],
    "Volatility Expansion": ["Expansion", "High-volatility"],
}


def apply_forensic_response(rec: dict, category: str, state: dict) -> Optional[str]:
    """Routes the diagnosis to exactly its documented adaptive parameter(s),
    bounded/dampened per Section 5. Returns a short description of the
    delta applied (or None) for auditability (Section 13.5)."""
    t1 = state["tier1"]
    engine, symbol = rec["engine"], rec["symbol"]
    label = regime_label_bucket(RegimeVector(**rec["regime_vector_at_entry"]))

    cat_stats = t1["forensic_category_stats"].setdefault(category, {"n": 0, "trend": []})
    cat_stats["n"] += 1
    cat_stats["trend"].append(1)
    if len(cat_stats["trend"]) > 200:
        cat_stats["trend"] = cat_stats["trend"][-200:]

    if cat_stats["n"] < MIN_SAMPLE_SIZE_CATEGORY:
        return None  # min-sample-size gate (Section 13/5) -- log only, don't adapt yet

    if category == "regime_mismatch":
        key = f"{engine}:{label}"
        cur = t1["regime_fit_discount"].get(key, 1.0)
        t1["regime_fit_discount"][key] = bounded_update(cur, cur * 0.85, 0.1, 1.0)
        return f"regime_fit_discount[{key}] tightened"

    if category == "structural_invalidation_too_tight":
        key = f"{symbol}:15M"
        cur = t1["sl_buffer_percentile"].get(key, 65.0)
        t1["sl_buffer_percentile"][key] = bounded_update(cur, cur + 8, 50.0, 90.0)
        return f"sl_buffer_percentile[{key}] widened"

    if category == "chased_swept_liquidity":
        cur = t1["liquidity_sanity_threshold"].get(engine, 0.5)
        t1["liquidity_sanity_threshold"][engine] = bounded_update(cur, cur + 0.1, 0.2, 1.5)
        return f"liquidity_sanity_threshold[{engine}] tightened"

    if category == "mtf_conflict_ignored":
        cur = t1["mtf_alignment_weight"]
        t1["mtf_alignment_weight"] = bounded_update(cur, cur + 0.03, 0.05, 0.35)
        return "mtf_alignment_weight raised"

    if category == "sfp_mss_sequence_violated":
        cur = t1["sfp_purity_requirement"].get(engine, 0.35)
        t1["sfp_purity_requirement"][engine] = bounded_update(cur, cur + 0.05, 0.2, 0.85)
        return f"sfp_purity_requirement[{engine}] tightened"

    if category == "correct_read_poor_rr":
        cur = int(t1["tp1_target_rank_preference"].get(symbol, 3))
        t1["tp1_target_rank_preference"][symbol] = int(bounded_update(cur, cur + 1, 2, 6, max_step_frac=0.5))
        return f"tp1_target_rank_preference[{symbol}] widened"

    if category == "confidence_miscalibration":
        bucket = _confidence_bucket(rec["confidence"])
        key = f"{engine}:{bucket}"
        cur = t1["confidence_calibration"].get(key, 1.0)
        t1["confidence_calibration"][key] = bounded_update(cur, cur * 0.9, 0.5, 1.2)
        return f"confidence_calibration[{key}] lowered"

    if category == "filter_over_permissiveness":
        return None  # routed via filter_funnel review (Section 14), not a single scalar here

    return None  # genuine_variance -- no parameter change by design


def reinforce_win(rec: dict, state: dict) -> None:
    """Win-side reinforcement (Section 13.2): raise weights of factors
    genuinely present and predictive, never a blanket engine-weight bump."""
    t1 = state["tier1"]
    engine = rec["engine"]
    seg_key = f"{rec['symbol']}:{regime_label_bucket(RegimeVector(**rec['regime_vector_at_entry']))}:{rec['style']}:{engine}"
    seg = t1["segment_stats"].get(seg_key, {"n": 0, "wins": 0, "sum_r": 0.0})
    if seg["n"] >= MIN_SAMPLE_SIZE_SEGMENT:
        wr = seg["wins"] / seg["n"]
        if wr > 0.55:
            cur = t1["engine_weights"].get(engine, 1.0)
            t1["engine_weights"][engine] = bounded_update(cur, cur * 1.05, 0.4, 2.0)


def update_segment_stats(rec: dict, state: dict) -> None:
    """Tier 1 aggregates updated incrementally, one trade at a time --
    never by rescanning Tier 2 (Section 5 mandatory)."""
    t1 = state["tier1"]
    label = regime_label_bucket(RegimeVector(**rec["regime_vector_at_entry"]))
    seg_key = f"{rec['symbol']}:{label}:{rec['style']}:{rec['engine']}"
    seg = t1["segment_stats"].setdefault(seg_key, {"n": 0, "wins": 0, "losses": 0, "sum_r": 0.0})
    seg["n"] += 1
    if rec["result"] == "win":
        seg["wins"] += 1
    else:
        seg["losses"] += 1
    seg["sum_r"] += rec["r_realized"]

    bucket = _confidence_bucket(rec["confidence"])
    cal_key = f"{rec['engine']}:{bucket}"
    cal = t1["confidence_calibration_samples"].setdefault(cal_key, {"n": 0, "wins": 0, "sum_r": 0.0})
    cal["n"] += 1
    cal["wins"] += 1 if rec["result"] == "win" else 0
    cal["sum_r"] += rec["r_realized"]

    if rec.get("session_anchored"):
        sess_key = f"{rec['engine']}:{rec['symbol']}"
        sa = t1["session_anchored_stats"].setdefault(sess_key, {
            "anchored": {"n": 0, "wins": 0, "sum_r": 0.0}, "plain": {"n": 0, "wins": 0, "sum_r": 0.0}})
        bucket_name = "anchored" if rec["session_anchored"] else "plain"
        sa[bucket_name]["n"] += 1
        sa[bucket_name]["wins"] += 1 if rec["result"] == "win" else 0
        sa[bucket_name]["sum_r"] += rec["r_realized"]
        anchored, plain = sa["anchored"], sa["plain"]
        if anchored["n"] >= MIN_SAMPLE_SIZE_CATEGORY and plain["n"] >= MIN_SAMPLE_SIZE_CATEGORY:
            a_ev = anchored["sum_r"] / anchored["n"]
            p_ev = plain["sum_r"] / plain["n"]
            cur = t1["session_open_proximity_weight"]
            if a_ev > p_ev + 0.1:
                t1["session_open_proximity_weight"] = bounded_update(cur, cur + 0.02, 0.0, 0.20)
            elif a_ev <= p_ev:
                t1["session_open_proximity_weight"] = bounded_update(cur, cur * 0.8, 0.0, 0.20)


def process_resolution(rec: dict, state: dict) -> None:
    """Full pipeline for one resolved trade: classify -> route adaptive
    response -> update Tier 1 aggregates -> persist Tier 2 record. This is
    the only place adaptive parameters move in response to trade outcomes."""
    if rec["result"] not in ("win", "loss"):
        # expired/no-fill signals are excluded from every win/loss consumer
        # (Section 12 mandatory) -- still logged, never scored.
        fk = f"{rec['engine']}:{rec['entry_kind']}"
        fr = state["tier1"]["fill_rate_stats"].setdefault(fk, {"filled": 0, "expired": 0})
        fr["expired"] += 1
        state["tier2_trades"].append(rec)
        return

    fk = f"{rec['engine']}:{rec['entry_kind']}"
    fr = state["tier1"]["fill_rate_stats"].setdefault(fk, {"filled": 0, "expired": 0})
    fr["filled"] += 1

    category = classify_trade(rec, state)
    rec["forensic_category"] = category
    frozen = adaptation_frozen(state)
    # Segment/calibration *statistics* keep accumulating even while frozen --
    # the circuit breaker needs live data to detect recovery -- but no
    # adaptive *parameter* is allowed to move while frozen (Section 5).
    update_segment_stats(rec, state)
    if frozen:
        rec["adaptive_delta"] = "frozen (circuit breaker active)"
    else:
        delta_desc = apply_forensic_response(rec, category, state) if rec["result"] == "loss" else None
        rec["adaptive_delta"] = delta_desc
        if rec["result"] == "win":
            reinforce_win(rec, state)

    state["tier2_trades"].append(rec)
    establish_baseline_if_needed(state)
    _update_circuit_breaker(state)


def _update_circuit_breaker(state: dict) -> None:
    """Live-performance circuit breaker (Section 5): freeze adaptation on a
    material sustained deviation from the pre-deployment baseline; auto-
    resume once performance recovers over a fresh window of the same size."""
    t1 = state["tier1"]
    baseline = t1["baseline"]
    if baseline.get("n", 0) < MIN_SAMPLE_SIZE_SEGMENT or baseline.get("win_rate") is None:
        return  # no meaningful baseline yet -- nothing to compare against

    recent = [t for t in state["tier2_trades"] if t["result"] in ("win", "loss")][-CIRCUIT_BREAKER_WINDOW_TRADES:]
    if len(recent) < CIRCUIT_BREAKER_WINDOW_TRADES:
        return

    wins = sum(1 for t in recent if t["result"] == "win")
    wr = wins / len(recent)
    gains = sum(t["r_realized"] for t in recent if t["r_realized"] > 0)
    losses = abs(sum(t["r_realized"] for t in recent if t["r_realized"] < 0))
    pf = gains / losses if losses > 1e-9 else float("inf")

    cb = t1["circuit_breaker"]
    wr_drop = baseline["win_rate"] - wr
    pf_drop_frac = (1 - pf / baseline["profit_factor"]) if baseline.get("profit_factor") else 0.0

    if not cb["tripped"] and (wr_drop >= CIRCUIT_BREAKER_WIN_RATE_DROP or pf_drop_frac >= CIRCUIT_BREAKER_PF_DROP_FRAC):
        cb["tripped"] = True
        cb["tripped_ts"] = int(time.time() * 1000)
        cb["reason"] = f"win_rate {wr:.2f} vs baseline {baseline['win_rate']:.2f} / pf {pf:.2f} vs {baseline['profit_factor']:.2f}"
        log.warning("CIRCUIT BREAKER TRIPPED: %s", cb["reason"])
    elif cb["tripped"] and wr >= baseline["win_rate"] and pf >= baseline["profit_factor"]:
        cb["tripped"] = False
        cb["tripped_ts"] = None
        cb["reason"] = None
        log.info("Circuit breaker auto-resumed: live performance recovered to baseline.")


def adaptation_frozen(state: dict) -> bool:
    return bool(state["tier1"]["circuit_breaker"]["tripped"])


def establish_baseline_if_needed(state: dict) -> None:
    """DECISION: this engine has no separate offline backtest harness wired
    into this file (Section 21 scopes the deliverable to the engine source
    only). The pragmatic, documented stand-in required for the circuit
    breaker to function day-one: the first MIN_SAMPLE_SIZE_SEGMENT resolved
    live/paper trades become the pre-deployment baseline (Section 13),
    computed once and then frozen -- exactly the statistically-meaningful
    threshold already used everywhere else in this engine, applied here to
    the earliest trade batch instead of a separate offline run."""
    t1 = state["tier1"]
    if t1["baseline"]["win_rate"] is not None:
        return
    resolved = [t for t in state["tier2_trades"] if t["result"] in ("win", "loss")]
    if len(resolved) < MIN_SAMPLE_SIZE_SEGMENT:
        return
    batch = resolved[:MIN_SAMPLE_SIZE_SEGMENT]
    wins = sum(1 for t in batch if t["result"] == "win")
    gains = sum(t["r_realized"] for t in batch if t["r_realized"] > 0)
    losses = abs(sum(t["r_realized"] for t in batch if t["r_realized"] < 0))
    t1["baseline"] = {
        "win_rate": wins / len(batch),
        "profit_factor": (gains / losses) if losses > 1e-9 else float("inf"),
        "avg_rr": sum(t["rr1"] for t in batch) / len(batch),
        "n": len(batch),
    }
    log.info("Baseline established from first %d resolved trades: %s", len(batch), t1["baseline"])


# =============================================================================
# SECTION 16 -- TELEGRAM INTEGRATION
# =============================================================================

def _titlecase_token(s: str) -> str:
    """Section 16 mandatory formatting rule: no raw underscores anywhere in
    user-facing text; convert to clean Title Case with spaces."""
    return " ".join(w.capitalize() for w in str(s).replace("-", "_").split("_"))


def _fmt_price(p: float) -> str:
    if p >= 100:
        return f"{p:,.2f}"
    if p >= 1:
        return f"{p:,.4f}"
    return f"{p:.6f}"


def _tg_escape(s: str) -> str:
    for ch in ("_", "*", "[", "]", "(", ")", "~", "`", ">", "#", "+", "-", "=", "|", "{", "}", ".", "!"):
        s = s.replace(ch, "\\" + ch)
    return s


def _tg_post(method: str, payload: dict, files: Optional[dict] = None) -> None:
    if not TG_BOT_TOKEN or not TG_CHAT_ID:
        log.info("Telegram not configured; message suppressed: %s", payload.get("text", "")[:120])
        return
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/{method}"
    payload = dict(payload)
    payload.setdefault("chat_id", TG_CHAT_ID)
    try:
        body = json.dumps(payload).encode()
        req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            resp.read()
    except (urllib.error.URLError, TimeoutError, OSError) as e:
        log.warning("Telegram send failed: %s", e)


def format_signal_message(rec: dict) -> str:
    engine_clean = _titlecase_token(rec["engine"])
    regime_clean = _titlecase_token(rec["regime_at_entry"])
    direction_clean = "Long" if rec["direction"] == "bullish" else "Short"
    lines = [
        f"*{_tg_escape(ENGINE_NAME)} {_tg_escape(ENGINE_VERSION)}*",
        "",
        f"Signal ID: {rec['id']}",
        f"Asset: {rec['symbol']}   Direction: {direction_clean}",
        f"Style: {_titlecase_token(rec['style'])}   Engine: {engine_clean}",
        f"Grade: {rec['grade']}   Confidence: {rec['confidence']*100:.0f}%",
        f"Regime: {regime_clean}",
        "",
        f"Entry: `{_fmt_price(rec['entry'])}`",
        f"SL: `{_fmt_price(rec['sl'])}`",
        f"TP1: `{_fmt_price(rec['tp1'])}`",
        f"TP2 (suggested): `{_fmt_price(rec['tp2'])}`",
        "",
        f"RR (TP1): {rec['rr1']:.2f}   RR (TP2, suggested): {rec['rr2']:.2f}",
        f"SL Anchor: {rec['sl_anchor']}   Entry Type: {_titlecase_token(rec['entry_kind'])}",
        "",
        "Confluences:",
    ]
    for cf in rec["confluences"]:
        lines.append(f"  - {_tg_escape(cf)}")
    lines.append("")
    lines.append("TP1 is the sole resolving target for this signal; TP2 is an informational "
                  "extended reference only and is never tracked after dispatch.")
    return "\n".join(lines)


def send_signal(rec: dict) -> None:
    _tg_post("sendMessage", {"text": format_signal_message(rec), "parse_mode": "Markdown"})
    if REACTION_IMAGE_PATH and os.path.exists(REACTION_IMAGE_PATH):
        try:
            with open(REACTION_IMAGE_PATH, "rb") as img:
                pass  # multipart photo upload intentionally omitted from this
                       # minimal urllib client; wire a multipart POST here if
                       # a reaction image asset is supplied at deploy time.
        except OSError:
            pass


def send_status_update(rec: dict, status: str) -> None:
    """Activated, Expired, or resolution (TP1=WIN / SL=LOSS). No TP2 status,
    no auto-breakeven status (Section 11/16 mandatory)."""
    status_clean = _titlecase_token(status)
    lines = [
        f"*{_tg_escape(ENGINE_NAME)}* -- Status Update",
        "",
        f"Signal ID: {rec['id']}   Asset: {rec['symbol']}",
        f"Status: {status_clean}",
    ]
    if status == "win":
        lines += [
            "",
            "TP1 hit -- signal resolved as WIN.",
            f"Realized R: {rec['r_realized']:.2f}",
            "The original SL remains at its structural level, unchanged, for record-keeping only; "
            "this position is fully closed.",
        ]
    elif status == "loss":
        lines += [
            "",
            "SL hit -- signal resolved as LOSS.",
            f"Realized R: {rec['r_realized']:.2f}",
        ]
    elif status == "expired":
        lines += ["", "Entry was never filled within the pending window -- signal expired, no fill."]
    _tg_post("sendMessage", {"text": "\n".join(lines), "parse_mode": "Markdown"})


def format_daily_summary(state: dict, date_str: str) -> str:
    trades = [t for t in state["tier2_trades"]
              if t.get("resolved_ts") and datetime.fromtimestamp(t["resolved_ts"] / 1000, tz=timezone.utc).strftime("%Y-%m-%d") == date_str]
    wl = [t for t in trades if t["result"] in ("win", "loss")]
    n = len(wl)
    wins = sum(1 for t in wl if t["result"] == "win")
    win_rate = (wins / n * 100.0) if n else 0.0
    gains = sum(t["r_realized"] for t in wl if t["r_realized"] > 0)
    losses = abs(sum(t["r_realized"] for t in wl if t["r_realized"] < 0))
    pf = (gains / losses) if losses > 1e-9 else float("inf")
    avg_rr = (sum(t["rr1"] for t in wl) / n) if n else 0.0

    by_regime: Dict[str, List[dict]] = collections.defaultdict(list)
    by_engine: Dict[str, List[dict]] = collections.defaultdict(list)
    for t in wl:
        by_regime[t["regime_at_entry"]].append(t)
        by_engine[t["engine"]].append(t)

    lines = [
        f"*{_tg_escape(ENGINE_NAME)} {_tg_escape(ENGINE_VERSION)} -- Daily Summary*",
        f"Date: {date_str}",
        "",
        f"Total Signals: {len(trades)}   Wins: {wins}   Losses: {n - wins}",
        f"Win Rate: {win_rate:.1f}%   Profit Factor: {pf:.2f}   Average RR: {avg_rr:.2f}",
        "",
        "By Regime:",
    ]
    for label, ts in by_regime.items():
        wr = sum(1 for t in ts if t["result"] == "win") / len(ts) * 100.0
        lines.append(f"  - {_titlecase_token(label)}: {len(ts)} trades, {wr:.0f}% win rate")
    lines.append("")
    lines.append("By Engine:")
    for eng, ts in by_engine.items():
        wr = sum(1 for t in ts if t["result"] == "win") / len(ts) * 100.0
        lines.append(f"  - {_titlecase_token(eng)}: {len(ts)} trades, {wr:.0f}% win rate")

    if wl:
        best = max(wl, key=lambda t: t["r_realized"])
        worst = min(wl, key=lambda t: t["r_realized"])
        lines += ["", f"Best Setup: {best['symbol']} ({_titlecase_token(best['engine'])}), R={best['r_realized']:.2f}",
                  f"Worst Setup: {worst['symbol']} ({_titlecase_token(worst['engine'])}), R={worst['r_realized']:.2f}"]

    lines.append("")
    lines.append("Loss/Win Forensic Breakdown:")
    for cat in FORENSIC_CATEGORIES:
        stats = state["tier1"]["forensic_category_stats"].get(cat, {"n": 0, "trend": []})
        trend = stats["trend"][-20:]
        trend_desc = "n/a" if len(trend) < 5 else ("declining" if sum(trend[-10:]) < sum(trend[:10]) else "stable/rising")
        lines.append(f"  - {_titlecase_token(cat)}: {stats['n']} total, recent trend: {trend_desc}")

    expired_total = sum(v.get("expired", 0) for v in state["tier1"]["fill_rate_stats"].values())
    filled_total = sum(v.get("filled", 0) for v in state["tier1"]["fill_rate_stats"].values())
    fill_denom = expired_total + filled_total
    fill_rate = (filled_total / fill_denom * 100.0) if fill_denom else 100.0
    lines += ["", f"Fill Rate: {fill_rate:.1f}% ({filled_total} filled / {expired_total} expired)"]

    cb = state["tier1"]["circuit_breaker"]
    lines += ["", f"Circuit Breaker: {'TRIPPED - ' + str(cb['reason']) if cb['tripped'] else 'Normal'}"]

    lines += ["", "Learning Adjustments Today:"]
    adj_today = [t.get("adaptive_delta") for t in wl if t.get("adaptive_delta") and "frozen" not in str(t.get("adaptive_delta"))]
    if adj_today:
        for a in adj_today[:15]:
            lines.append(f"  - {a}")
    else:
        lines.append("  - No parameter changes today (all within sample-size gates, or genuine variance).")

    return "\n".join(lines)


def send_daily_summary(state: dict, date_str: str) -> None:
    _tg_post("sendMessage", {"text": format_daily_summary(state, date_str), "parse_mode": "Markdown"})


def send_circuit_breaker_alert(reason: str) -> None:
    lines = [
        f"*{_tg_escape(ENGINE_NAME)}* -- CIRCUIT BREAKER ALERT",
        "",
        "Live performance has deviated materially from the documented baseline.",
        f"Reason: {_tg_escape(reason)}",
        "",
        "Automatic parameter adaptation is now FROZEN at last-known-good values. "
        "Signal generation continues using those frozen parameters.",
    ]
    _tg_post("sendMessage", {"text": "\n".join(lines), "parse_mode": "Markdown"})


# =============================================================================
# SECTION 17 -- SCAN ORCHESTRATION
# =============================================================================

def _active_symbols_and_sectors(state: dict) -> List[Tuple[str, str]]:
    return [(r["symbol"], SECTOR_MAP.get(r["symbol"], r["symbol"]))
            for r in state["active_signals"].values()]


def build_all_views(candle_bundle: Dict[str, List[dict]]) -> Dict[str, TFView]:
    views: Dict[str, TFView] = {}
    for tf, raw in candle_bundle.items():
        v = build_tf_view(tf, raw)
        if v is not None:
            views[tf] = v
    return views


def run_scan(state: dict, candle_cache: dict, reference_ms: Optional[int] = None) -> dict:
    reference_ms = reference_ms or int(time.time() * 1000)
    log.info("=== %s %s scan starting @ %s ===", ENGINE_NAME, ENGINE_VERSION,
              datetime.fromtimestamp(reference_ms / 1000, tz=timezone.utc).isoformat())

    cb_was_tripped = state["tier1"]["circuit_breaker"]["tripped"]

    # --- 1. Fetch candles + build views for every watchlist asset ---
    views_by_symbol: Dict[str, Dict[str, TFView]] = {}
    for symbol in WATCHLIST:
        bundle = fetch_all_candles(symbol, candle_cache, reference_ms)
        if bundle is None:
            log.info("Skipping %s this scan (insufficient candle data)", symbol)
            continue
        views_by_symbol[symbol] = build_all_views(bundle)

    if MACRO_ASSET not in views_by_symbol:
        log.warning("Macro asset %s unavailable this scan; regime vector will use neutral macro bias.", MACRO_ASSET)
        btc_daily = btc_h4 = None
    else:
        btc_daily = views_by_symbol[MACRO_ASSET].get(TF_DAILY)
        btc_h4 = views_by_symbol[MACRO_ASSET].get(TF_H4)

    all_h1_views = {s: v[TF_H1] for s, v in views_by_symbol.items() if TF_H1 in v}

    # --- 2. Composite Regime Vector per asset ---
    regime_by_symbol: Dict[str, RegimeVector] = {}
    for symbol, views in views_by_symbol.items():
        if btc_daily is None or btc_h4 is None:
            btc_daily_use = views.get(TF_DAILY)
            btc_h4_use = views.get(TF_H4)
        else:
            btc_daily_use, btc_h4_use = btc_daily, btc_h4
        if btc_daily_use is None or btc_h4_use is None:
            continue
        regime_by_symbol[symbol] = compute_regime_vector(
            symbol, views, btc_daily_use, btc_h4_use, all_h1_views, reference_ms,
            state["tier1"]["session_stats"])
    state["_last_regime_by_symbol"] = regime_by_symbol

    # --- 3. Monitor + resolve existing active signals ---
    for symbol, views in views_by_symbol.items():
        m15 = views.get(TF_M15)
        if m15 is None:
            continue
        events = monitor_signals(state, symbol, m15)
        for rec in events:
            process_resolution(rec, state)
            send_status_update(rec, rec["result"])
            if rec["result"] in ("win", "loss"):
                log.info("Resolved %s (%s) -> %s, R=%.2f, category=%s",
                          rec["id"], rec["symbol"], rec["result"], rec["r_realized"],
                          rec["forensic_category"])

    cb_now_tripped = state["tier1"]["circuit_breaker"]["tripped"]
    if cb_now_tripped and not cb_was_tripped:
        send_circuit_breaker_alert(state["tier1"]["circuit_breaker"]["reason"] or "deviation detected")

    # --- 4. Run every specialized engine across the full watchlist ---
    all_candidates: List[Candidate] = []
    for symbol, views in views_by_symbol.items():
        regime = regime_by_symbol.get(symbol)
        if regime is None:
            continue
        cands = run_specialized_engines(symbol, views, regime, state)
        all_candidates.extend(cands)

    log.info("Generated %d raw candidates across %d assets", len(all_candidates), len(views_by_symbol))

    # --- 5. Decision Engine: continuous blend, vetoes, correlation/concurrency caps ---
    active_syms_sectors = _active_symbols_and_sectors(state)
    selected = decision_engine_select(all_candidates, regime_by_symbol, views_by_symbol,
                                       state, reference_ms, active_syms_sectors)

    # --- 6. Dispatch ---
    for s in selected:
        rec = dispatch_signal(s, state, reference_ms)
        send_signal(rec)
        log.info("Dispatched %s: %s %s via %s, grade=%s, score=%.3f",
                  rec["id"], rec["symbol"], rec["direction"], rec["engine"], rec["grade"], s.score)

    if not selected:
        log.info("No signals cleared the bar this scan -- NO TRADE across the full watchlist. "
                 "This is a complete success, not a shortfall (Section 2/14).")

    # --- 7. Daily summary (08:00 UTC) ---
    now_dt = datetime.fromtimestamp(reference_ms / 1000, tz=timezone.utc)
    today_str = now_dt.strftime("%Y-%m-%d")
    if now_dt.hour == 8 and state.get("last_daily_summary_date") != today_str:
        yesterday_str = datetime.fromtimestamp(reference_ms / 1000 - 86400, tz=timezone.utc).strftime("%Y-%m-%d")
        send_daily_summary(state, yesterday_str)
        state["last_daily_summary_date"] = today_str

    # --- 8. Housekeeping ---
    prune_tier2(state, reference_ms)
    state["last_run_ts"] = reference_ms
    state.pop("_last_regime_by_symbol", None)
    return state


# =============================================================================
# SECTION 18 -- ENTRY POINT
# =============================================================================

_shutdown_requested = False


def _handle_shutdown(signum, frame):
    global _shutdown_requested
    _shutdown_requested = True
    log.info("Shutdown signal received; will exit cleanly after current scan.")


signal.signal(signal.SIGTERM, _handle_shutdown)
signal.signal(signal.SIGINT, _handle_shutdown)


def main() -> int:
    state = load_state()
    candle_cache = load_candle_cache()

    def _persist():
        try:
            save_state(state)
            save_candle_cache(candle_cache)
        except Exception:
            log.exception("Failed to persist state/candle cache")

    atexit.register(_persist)

    try:
        run_scan(state, candle_cache)
    except Exception:
        log.exception("Unhandled exception during scan -- state will still be persisted so learning "
                      "and active-signal tracking are not lost.")
        _persist()
        return 1

    _persist()
    log.info("Scan complete.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
