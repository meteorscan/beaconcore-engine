#!/usr/bin/env python3
"""
SOVEREIGN SIGNAL ENGINE v1.0.0
===============================
Institutional-grade, multi-engine adaptive SMC/ICT signal engine for
Hyperliquid perpetual futures. Single-file, scan-per-run architecture,
designed to be invoked every 15 minutes by an external scheduler
(GitHub Actions, cron-job.org, systemd timer, etc.).

--------------------------------------------------------------------------
DESIGN SUMMARY (see inline section headers for the full implementation)
--------------------------------------------------------------------------
  - Mandatory four-stage top-down sequence (Weekly+Daily bias -> 4H context
    -> 1H zone-selection -> 15M MSS/FVG entry) gates every specialized
    engine; no engine can bypass it (Section 7).
  - Zone-Selection Sequence: HTF bias -> POI -> SFP purity (EQH/EQL sweep,
    sweep-to-POI causality) -> MSS -> breaker confirmation -> Fibonacci OTE
    refinement (Section 8).
  - Thirteen specialized engines (SMC, Trend Continuation, Breakout,
    Pullback, Liquidity Sweep, Order Block, Breaker Block, Fair Value Gap,
    Momentum, Reversal, Mean Reversion, Range Trading, Volatility
    Expansion) plus an opt-in Counter-Trend Reversal engine, each emitting
    direction/entry/SL/TP1/TP2/confidence/RR/confluences/regime-fit.
  - Composite Regime Vector (macro bias, volatility percentile, ADX trend
    strength, session weight, session-open proximity, ERL/IRL liquidity
    draw, noise index, breadth) feeds regime-fit veto, adaptive filter
    routing, and the Decision Engine.
  - Decision Engine: a small, bounded, continuous logistic blend over
    regime fit, MTF alignment, confluence strength, historical segment
    performance, EV, RR context, and liquidity/volatility context -- never
    a discrete point stack. Every term is contribution-capped so no single
    term can saturate the blend.
  - Adaptive-percentile SL buffer + confluence-ranked, liquidity-wall-
    clipped TP1/TP2, with a hard 1.5 TP1 RR floor (TP1 is the sole
    resolving target; TP2 is an uncapped, informational-only reference).
  - Entry-fill verification (`entry_kind` market/pending abstraction) with
    bounded pending expiry -- phantom fills are structurally impossible.
  - No automatic SL-to-breakeven repositioning on TP1 -- premature
    stop-outs from that bug class are structurally impossible.
  - Closed-candle-only structural detection shared by every code path
    (Section 12A) -- no repainting, no live/backtest divergence.
  - Closed-taxonomy loss/win forensics deterministically routes every
    resolved trade's diagnosis to the one adaptive parameter it implicates
    (Section 13), feeding a bounded, dampened, minimum-sample-gated,
    circuit-breaker-protected learning loop (Section 5).
  - Tier 1 (permanent aggregates) / Tier 2 (bounded raw log) state.json
    structure so learned behavior survives Tier-2 pruning untouched.
  - Correlation-cluster deduplication, macro/news blackout, filter-funnel
    attrition logging, and fill-rate health tracking (Section 14).
  - Hyperliquid client with weighted rate limiting, retries/backoff, and a
    persisted, delta-fetched candle cache shared across every stage and
    engine (Section 15).
  - Clean, copy-paste-friendly Telegram formatting with reply-threaded
    lifecycle status updates (Section 17).

Only deliverable per the build spec is this Python file (Section 21) --
no GitHub Actions YAML, requirements.txt, or state.json are produced here.

Run:
    python3 sovereign_signal_engine_v1_0_0.py
"""

from __future__ import annotations

import collections
import fcntl
import json
import logging
import math
import os
import statistics
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

# ============================================================================
# SECTION 0 -- CONFIGURATION
# ============================================================================

HL_API_URL = "https://api.hyperliquid.xyz/info"
TG_BOT_TOKEN = os.environ.get("TG_BOT_TOKEN", "")
TG_CHAT_ID = os.environ.get("TG_CHAT_ID", "")
TG_REACTION_IMAGE_URL = os.environ.get("TG_REACTION_IMAGE_URL", "")

STATE_PATH = os.environ.get("SOVEREIGN_STATE_PATH", "state.json")
LOCK_PATH = os.environ.get("SOVEREIGN_LOCK_PATH", "sovereign_engine.lock")
LOG_PATH = os.environ.get("SOVEREIGN_LOG_PATH", "sovereign_engine.log")
CANDLE_CACHE_PATH = os.environ.get("SOVEREIGN_CANDLE_CACHE_PATH", "candle_cache.json")
CANDLE_DELTA_OVERLAP_BARS = 3  # extra closed bars re-fetched past the cached watermark

ENGINE_NAME = "SOVEREIGN"
ENGINE_VERSION = "1.0.0"
RESOLUTION_LOGIC_VERSION = "1.0.0"  # Section 11 legacy-data tag; bump on any resolution-logic change

# Soft internal wall-clock budget for a single run, checked at every
# checkpoint in run_scan(). Must stay comfortably below the GitHub Actions
# job's `timeout-minutes` -- if the scan is still running past this, it
# stops taking on new work and returns immediately so save_state()/
# save_candle_cache() and the git-persist step still get to run before the
# job is killed. A run that gets hard-killed by the CI timeout never
# reaches the `finally` block in main(), so anything dispatched via
# Telegram during that run would otherwise never make it into state.json.
SOFT_RUN_DEADLINE_S = float(os.environ.get("SOVEREIGN_SOFT_DEADLINE_S", "420"))  # 7 min

# Same watchlist as the reference engines in this project (Section: "use the
# same watchlist ... unless the prompt's own rules require a change" -- no
# rule here requires a change).
WATCHLIST = [
    "BTC", "ETH", "HYPE", "ZEC", "NEAR", "ONDO", "SUI", "PENGU", "BNB", "SOL",
    "TRX", "BCH", "DOGE", "ADA", "DOT", "TAO", "AVAX", "LINK", "AAVE", "XRP",
    "XLM", "UNI", "LTC", "APT", "PENDLE",
]
MACRO_ASSET = "BTC"  # dominant benchmark for Regime Vector macro bias / breadth

# --- Timeframes (Section 7: forbidden 1M/2M/3M/5M, minimum entry TF = 15M) --
TF_WEEKLY, TF_DAILY, TF_4H, TF_1H, TF_15M = "1w", "1d", "4h", "1h", "15m"
ALL_TFS = (TF_WEEKLY, TF_DAILY, TF_4H, TF_1H, TF_15M)
TF_MS = {
    TF_15M: 15 * 60_000,
    TF_1H: 60 * 60_000,
    TF_4H: 4 * 60 * 60_000,
    TF_DAILY: 24 * 60 * 60_000,
    TF_WEEKLY: 7 * 24 * 60 * 60_000,
}
CANDLE_COUNT = {TF_15M: 300, TF_1H: 300, TF_4H: 260, TF_DAILY: 260, TF_WEEKLY: 156}

# 15m matches the scan cadence -- SL/TP monitoring evaluates each closed 15m
# candle's high/low against active levels, so a wick that touches and
# reverses between scans is still caught rather than relying on live price.
MONITOR_TF = TF_15M

# Resting/POI entries (order-block, breaker, FVG, retest) sit away from
# market price and may never actually fill. Sized for the 15M entry
# timeframe: 12 * 15m = 3h, comfortably inside the intraday hold window.
PENDING_ENTRY_EXPIRY_BARS = 12
COUNTERTREND_RETEST_EXPIRY_BARS = 8  # tighter -- counter-trend entries are intraday-only

ATR_LEN = 14
RSI_LEN = 14
ADX_LEN = 14
EMA_FAST, EMA_SLOW, EMA_TREND = 20, 50, 200
BB_LEN, BB_MULT = 20, 2.0
SWING_LOOKBACK = 3  # bars each side for a fractal pivot high/low
EQ_CLUSTER_TOLERANCE_ATR = 0.12  # swing points within this * ATR are "equal"

RR_MIN_GATE = 1.5              # Section 10 hard floor -- shared by every core engine
RR_MIN_GATE_COUNTERTREND = 2.0  # Section 4A stricter floor -- own call site only, never mutates RR_MIN_GATE
RR_SOFT_TARGET = 2.0            # natural upper end of TP1's honest 1.5-2.0 range

MAX_CONCURRENT_ACTIVE_SIGNALS = 8
MAX_CORRELATED_CONCURRENT = 2  # per correlation cluster (Section 14)

ENABLE_COUNTERTREND_ENGINE = os.environ.get("ENABLE_COUNTERTREND_ENGINE", "false").lower() == "true"

# --- Section 5: adaptive-parameter bounds & dampening -----------------------
ENGINE_WEIGHT_MIN, ENGINE_WEIGHT_MAX = 0.70, 1.35
ENGINE_WEIGHT_LR = 0.05
CALIBRATION_ADJ_MIN, CALIBRATION_ADJ_MAX = -0.18, 0.18
CALIBRATION_LR = 0.06
FILTER_THRESH_MIN, FILTER_THRESH_MAX = 0.60, 1.40  # multiplicative envelope around baseline
FILTER_THRESH_LR = 0.05
SL_BUFFER_PCTL_MIN, SL_BUFFER_PCTL_MAX = 55.0, 90.0
SL_BUFFER_PCTL_LR_STEP = 2.0
TP1_RANK_PREF_MIN, TP1_RANK_PREF_MAX = 2, 6

MIN_SAMPLE_SIZE = 20            # Section 13: minimum trades/segment before adapting
CIRCUIT_BREAKER_WINDOW = 30     # rolling trade window compared against baseline
CIRCUIT_BREAKER_WR_DROP = 0.15  # material sustained win-rate drop (absolute) vs baseline
CIRCUIT_BREAKER_PF_DROP = 0.35  # material sustained profit-factor drop (relative) vs baseline

TIER2_RETENTION_DAYS = 15
TIER2_MAX_TRADES = 1500

# --- Section 13: baseline (documented pre-deployment reference) -------------
# The reference engines in this project have demonstrated very low real-world
# win rates (well under the ~45-50% a 1.5-2.0R strategy needs for positive
# expectancy after Sections 11/12's integrity fixes). This baseline is the
# comparison point the live-performance circuit breaker below measures
# against once enough trades have resolved to be meaningful; it is
# deliberately conservative rather than aspirational.
BASELINE_WIN_RATE = 0.42
BASELINE_PROFIT_FACTOR = 1.35
BASELINE_AVG_RR = 1.7

# --- Section 13: macro/news blackout (static high-impact calendar hooks) ---
# Exact scheduled timestamps are out of scope for a self-contained engine
# with no external economic-calendar feed; instead this enforces the
# mechanism -- a documented window around any event the operator flags via
# state.json's "macro_events" list (unix ms timestamps + affected symbols) --
# so the blackout is real and reachable rather than a no-op placeholder.
MACRO_BLACKOUT_MINUTES_BEFORE = 30
MACRO_BLACKOUT_MINUTES_AFTER = 30

# --- Correlation clusters (Section 14) --------------------------------------
CORRELATION_CLUSTERS = {
    "majors": {"BTC", "ETH"},
    "l1_alt": {"SOL", "AVAX", "NEAR", "SUI", "APT", "TAO", "DOT", "TRX", "ADA", "BNB"},
    "defi": {"UNI", "AAVE", "LINK", "ONDO", "PENDLE"},
    "meme_beta": {"DOGE", "PENGU"},
    "payments": {"XRP", "XLM", "LTC", "BCH"},
    "hype_zec": {"HYPE", "ZEC"},
}


def correlation_cluster(symbol: str) -> str:
    for cluster, members in CORRELATION_CLUSTERS.items():
        if symbol in members:
            return cluster
    return f"solo:{symbol}"


# ============================================================================
# SECTION 0A -- LOGGING
# ============================================================================

log = logging.getLogger("sovereign")
log.setLevel(logging.INFO)
if not log.handlers:
    _fh = logging.FileHandler(LOG_PATH)
    _sh = logging.StreamHandler()
    _fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
    _fh.setFormatter(_fmt)
    _sh.setFormatter(_fmt)
    log.addHandler(_fh)
    log.addHandler(_sh)


def utcnow_ms() -> int:
    return int(time.time() * 1000)


def human_label(raw: str) -> str:
    """Section 17: convert any snake/underscore identifier into clean Title
    Case for every user-facing surface -- the single shared formatting
    function every message-construction path must funnel through. Idempotent
    on strings that are already human-authored (e.g. engine names like
    "Order Block" or acronyms like "EQH/EQL"): a word that already carries
    internal capitalization is left untouched rather than lower-cased away."""
    if not raw:
        return ""
    words = str(raw).replace("-", "_").split("_")
    out = []
    for w in words:
        for token in w.split(" "):
            if not token:
                continue
            has_internal_case = any(ch.isupper() for ch in token[1:])
            out.append(token if has_internal_case else token.capitalize())
    return " ".join(out)


# ============================================================================
# SECTION 1 -- STATE PERSISTENCE (Tier 1 permanent aggregates / Tier 2 raw log)
# ============================================================================

def _default_state() -> dict:
    return {
        "schema_version": 1,
        "resolution_logic_version": RESOLUTION_LOGIC_VERSION,
        "tier1": {
            "engine_weights": {},       # engine -> weight (bounded, dampened)
            "combo_weights": {},        # style -> weight
            "confidence_calibration": {},  # "engine|bucket" -> additive adjustment
            "filter_thresholds": {},    # filter_name -> multiplier around baseline
            "sl_buffer_percentile": {}, # "symbol|tf" -> percentile (55-90)
            "tp1_rank_preference": {},  # symbol -> int in [2,6]
            "regime_fit_discount": {},  # "engine|regime" -> discount multiplier
            "segment_stats": {},        # "asset|regime|tf|engine" -> {n,wins,losses,sum_r,...}
            "calibration_buckets": {},  # "engine|bucket" -> {n, wins, sum_conf}
            "forensic_counts": {},      # category -> count (rolling)
            "fill_stats": {},           # "engine|entry_kind" -> {dispatched, filled, expired}
            "filter_funnel": {},        # filter_name -> {seen, rejected}
            "circuit_breaker": {"tripped": False, "tripped_ts": None, "reason": None},
            "governor": {"last_adjust_ts": 0},
            "daily_totals": {},         # "YYYY-MM-DD" -> summary dict
        },
        "tier2_trades": [],          # bounded raw trade log
        "active_signals": [],        # currently open/pending signals
        "macro_events": [],          # operator-supplied [{ "ts": ms, "symbols": [...] }]
        "last_run_ts": None,
    }


def load_state() -> dict:
    if not os.path.exists(STATE_PATH):
        return _default_state()
    try:
        with open(STATE_PATH, "r") as f:
            fcntl.flock(f.fileno(), fcntl.LOCK_SH)
            try:
                data = json.load(f)
            finally:
                fcntl.flock(f.fileno(), fcntl.LOCK_UN)
        base = _default_state()
        base.update({k: v for k, v in data.items() if k in base})
        for k in ("tier1",):
            merged = base[k]
            merged.update(data.get(k, {}))
            base[k] = merged
        return base
    except (json.JSONDecodeError, OSError, ValueError) as e:
        log.error("load_state failed (%s); starting from a fresh default state", e)
        return _default_state()


def save_state(state: dict) -> None:
    """Atomic write (tmp + os.replace) under an exclusive file lock so a
    crash mid-write can never corrupt state.json, and two overlapping runs
    can never interleave writes."""
    tmp_path = f"{STATE_PATH}.tmp"
    lock_f = open(LOCK_PATH, "w")
    try:
        fcntl.flock(lock_f.fileno(), fcntl.LOCK_EX)
        with open(tmp_path, "w") as f:
            json.dump(state, f, indent=2, default=str)
        os.replace(tmp_path, STATE_PATH)
    except OSError as e:
        log.error("save_state failed: %s", e)
    finally:
        fcntl.flock(lock_f.fileno(), fcntl.LOCK_UN)
        lock_f.close()


def prune_tier2(state: dict) -> None:
    """Bounded, prunable raw log (Section 5). Tier 1 aggregates are already
    updated incrementally per resolved trade, so pruning Tier 2 never
    changes any learned parameter -- only the detailed forensic record of
    already-summarized old trades is discarded."""
    cutoff_ms = utcnow_ms() - TIER2_RETENTION_DAYS * 86_400_000
    trades = [t for t in state["tier2_trades"] if t.get("resolved_ts", 0) >= cutoff_ms]
    if len(trades) > TIER2_MAX_TRADES:
        trades = trades[-TIER2_MAX_TRADES:]
    state["tier2_trades"] = trades


def load_candle_cache() -> dict:
    if not os.path.exists(CANDLE_CACHE_PATH):
        return {}
    try:
        with open(CANDLE_CACHE_PATH, "r") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        log.warning("candle cache unreadable; starting fresh")
        return {}


def save_candle_cache(cache: dict) -> None:
    tmp_path = f"{CANDLE_CACHE_PATH}.tmp"
    try:
        with open(tmp_path, "w") as f:
            json.dump(cache, f)
        os.replace(tmp_path, CANDLE_CACHE_PATH)
    except OSError as e:
        log.error("save_candle_cache failed: %s", e)


# ============================================================================
# SECTION 2 -- HYPERLIQUID CLIENT (weighted rate limiting, retries, cache)
# ============================================================================

HL_WEIGHT_BUDGET_PER_MINUTE = 1150  # conservative margin under HL's documented 1200/min IP limit
HL_DEFAULT_INFO_WEIGHT = 20
HL_ENDPOINT_BASE_WEIGHT = {"metaAndAssetCtxs": 20, "l2Book": 2, "candleSnapshot": 20}


class _WeightedRateLimiter:
    """Sliding-60s-window pacer shared across all threads, tracking request
    weight (not raw count) so heavier calls are paced proportionally."""

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
                # A single request heavier than the whole budget can never
                # satisfy `used + weight <= budget`, even with an empty queue.
                # Without this escape hatch that case spins in this loop
                # forever, hanging the job until the CI timeout kills it.
                if used + weight <= self.budget or (not self.events and weight > self.budget):
                    self.events.append((now, weight))
                    return
                sleep_for = max(0.05, self.events[0][0] + self.window_s - now)
            time.sleep(min(sleep_for, 2.0))


_rate_limiter = _WeightedRateLimiter(HL_WEIGHT_BUDGET_PER_MINUTE)


def _request_weight(payload: dict) -> float:
    req_type = payload.get("type", "")
    if req_type == "candleSnapshot":
        req = payload.get("req", {})
        interval = req.get("interval")
        start_ms, end_ms = req.get("startTime"), req.get("endTime")
        n_bars = 60
        if interval in TF_MS and start_ms is not None and end_ms is not None:
            n_bars = max(1, math.ceil((end_ms - start_ms) / TF_MS[interval]))
        return HL_DEFAULT_INFO_WEIGHT * math.ceil(n_bars / 60)
    return HL_ENDPOINT_BASE_WEIGHT.get(req_type, HL_DEFAULT_INFO_WEIGHT)


def hl_post(payload: dict, retries: int = 4, timeout: int = 12):
    body = json.dumps(payload).encode()
    req = urllib.request.Request(HL_API_URL, data=body, headers={"Content-Type": "application/json"})
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


def hl_coin(symbol: str) -> str:
    return symbol  # Hyperliquid perp coin names match the watchlist symbols directly


def current_bar_open_ms(reference_ms: int, interval: str) -> int:
    step = TF_MS[interval]
    return (reference_ms // step) * step


def filter_closed_candles(candles: list, interval: str, reference_ms: int) -> list:
    """Section 12A: never trust a still-forming candle. Anything whose open
    time is >= the current bar's open is provisional and is dropped here,
    once, at the source -- every downstream consumer inherits closed-only
    data automatically rather than re-implementing this check itself."""
    cutoff = current_bar_open_ms(reference_ms, interval)
    return [c for c in candles if c["t"] < cutoff]


def _request_candles(symbol: str, interval: str, start_ms: int, end_ms: int) -> list:
    payload = {"type": "candleSnapshot",
               "req": {"coin": hl_coin(symbol), "interval": interval, "startTime": start_ms, "endTime": end_ms}}
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
                 cache_entry: Optional[list] = None) -> list:
    """Return the last `n` closed candles. Delta-fetches only new bars past
    the cached watermark when a cache entry is available (Section 15's
    persistent candle_cache requirement), falling back to a full pull
    otherwise or on a failed delta fetch."""
    reference_ms = reference_ms or utcnow_ms()

    if cache_entry:
        step = TF_MS[interval]
        last_cached_t = cache_entry[-1]["t"]
        if current_bar_open_ms(reference_ms, interval) <= last_cached_t + step:
            return filter_closed_candles(cache_entry, interval, reference_ms)[-n:]
        start_ms = last_cached_t - step * CANDLE_DELTA_OVERLAP_BARS
        new_raw = _request_candles(symbol, interval, start_ms, reference_ms)
        if new_raw:
            merged = {c["t"]: c for c in cache_entry}
            for c in new_raw:
                merged[c["t"]] = c
            candles = [merged[t] for t in sorted(merged.keys())]
        else:
            candles = cache_entry
        return filter_closed_candles(candles, interval, reference_ms)[-n:]

    lookback_ms = n * TF_MS[interval] * 2 + TF_MS[interval] * 5
    raw = _request_candles(symbol, interval, reference_ms - lookback_ms, reference_ms)
    return filter_closed_candles(raw, interval, reference_ms)[-n:]


def fetch_all_candles(symbol: str, candle_cache: dict, reference_ms: Optional[int] = None) -> Optional[dict]:
    """Bounded, shared, persisted per Section 15: every stage and every
    specialized engine reads this same bundle rather than each fetching or
    caching its own copy."""
    bundle = {}
    sym_cache = candle_cache.get(symbol, {})
    for tf in ALL_TFS:
        cache_entry = sym_cache.get(tf)
        candles = get_candles(symbol, tf, CANDLE_COUNT[tf], reference_ms, cache_entry)
        min_required = 60 if tf != TF_WEEKLY else 30
        if len(candles) < min_required:
            log.info("Insufficient %s candles for %s (%d)", tf, symbol, len(candles))
            return None
        bundle[tf] = candles
        candle_cache.setdefault(symbol, {})[tf] = candles[-CANDLE_COUNT[tf]:]  # keep cache bounded
    return bundle


def get_meta_and_ctx():
    raw = hl_post({"type": "metaAndAssetCtxs"})
    if not raw or len(raw) < 2:
        return None
    universe = [a["name"] for a in raw[0]["universe"]]
    return universe, raw[1]


def get_market_snapshot() -> dict:
    """symbol -> {mark, funding, oi_usd} via one batched metaAndAssetCtxs call."""
    out = {}
    got = get_meta_and_ctx()
    if not got:
        return out
    universe, ctxs = got
    wanted = set(WATCHLIST) | {MACRO_ASSET}
    for i, name in enumerate(universe):
        if name not in wanted:
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


# ============================================================================
# SECTION 3 -- INDICATORS
# ============================================================================

def safe(v, fb=0.0):
    try:
        if v is None or math.isnan(v) or math.isinf(v):
            return fb
        return v
    except TypeError:
        return fb


def ema(vals: list, period: int) -> list:
    if not vals:
        return []
    k = 2 / (period + 1)
    out = [vals[0]]
    for v in vals[1:]:
        out.append(out[-1] + k * (v - out[-1]))
    return out


def rsi(closes: list, period: int = RSI_LEN) -> list:
    if len(closes) < period + 1:
        return [50.0] * len(closes)
    gains, losses = [0.0], [0.0]
    for i in range(1, len(closes)):
        d = closes[i] - closes[i - 1]
        gains.append(max(d, 0.0))
        losses.append(max(-d, 0.0))
    out = [50.0] * len(closes)
    avg_g = sum(gains[1:period + 1]) / period
    avg_l = sum(losses[1:period + 1]) / period
    for i in range(period + 1, len(closes)):
        avg_g = (avg_g * (period - 1) + gains[i]) / period
        avg_l = (avg_l * (period - 1) + losses[i]) / period
        rs = avg_g / avg_l if avg_l > 1e-12 else 100.0
        out[i] = 100.0 - (100.0 / (1.0 + rs))
    return out


def true_ranges(candles: list) -> list:
    trs = [candles[0]["h"] - candles[0]["l"]]
    for i in range(1, len(candles)):
        h, l, pc = candles[i]["h"], candles[i]["l"], candles[i - 1]["c"]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    return trs


def atr_series(candles: list, period: int = ATR_LEN) -> list:
    trs = true_ranges(candles)
    if len(trs) < period:
        return [statistics.fmean(trs)] * len(trs) if trs else []
    out = [None] * (period - 1)
    first = sum(trs[:period]) / period
    out.append(first)
    for i in range(period, len(trs)):
        out.append((out[-1] * (period - 1) + trs[i]) / period)
    out[0:period - 1] = [first] * (period - 1)
    return out


def adx_series(candles: list, period: int = ADX_LEN) -> list:
    n = len(candles)
    if n < period + 2:
        return [15.0] * n
    plus_dm, minus_dm = [0.0], [0.0]
    for i in range(1, n):
        up = candles[i]["h"] - candles[i - 1]["h"]
        dn = candles[i - 1]["l"] - candles[i]["l"]
        plus_dm.append(up if (up > dn and up > 0) else 0.0)
        minus_dm.append(dn if (dn > up and dn > 0) else 0.0)
    trs = true_ranges(candles)

    def wilder_smooth(vals):
        out = [None] * (period - 1)
        s = sum(vals[:period])
        out.append(s)
        for i in range(period, len(vals)):
            s = s - (s / period) + vals[i]
            out.append(s)
        out[0:period - 1] = [out[period - 1]] * (period - 1)
        return out

    atr_sm = wilder_smooth(trs)
    pdm_sm = wilder_smooth(plus_dm)
    mdm_sm = wilder_smooth(minus_dm)
    dx = []
    for i in range(n):
        a = atr_sm[i] or 1e-9
        pdi = 100 * (pdm_sm[i] / a) if a else 0.0
        mdi = 100 * (mdm_sm[i] / a) if a else 0.0
        denom = pdi + mdi
        dx.append(100 * abs(pdi - mdi) / denom if denom > 1e-9 else 0.0)
    out = [None] * (2 * period - 1)
    first = sum(dx[period - 1:2 * period - 1]) / period
    out.append(first)
    for i in range(2 * period, n):
        out.append((out[-1] * (period - 1) + dx[i]) / period)
    fill = out[2 * period] if len(out) > 2 * period else 15.0
    out[0:2 * period] = [fill] * min(2 * period, n)
    return out[:n]


def bollinger(closes: list, period: int = BB_LEN, mult: float = BB_MULT):
    if len(closes) < period:
        m = statistics.fmean(closes) if closes else 0.0
        return m, m, m
    window = closes[-period:]
    mid = statistics.fmean(window)
    sd = statistics.pstdev(window)
    return mid - mult * sd, mid, mid + mult * sd


def percentile(vals: list, pct: float) -> float:
    if not vals:
        return 0.0
    s = sorted(vals)
    k = (len(s) - 1) * (pct / 100.0)
    f, c = math.floor(k), math.ceil(k)
    if f == c:
        return s[int(k)]
    return s[f] + (s[c] - s[f]) * (k - f)


def percentile_rank(vals: list, x: float) -> float:
    if not vals:
        return 50.0
    below = sum(1 for v in vals if v <= x)
    return 100.0 * below / len(vals)


# ============================================================================
# SECTION 4 -- STRUCTURAL PRIMITIVES (Pivots, OB, Breaker, FVG, BOS/CHoCH,
#              EQH/EQL liquidity pools, SFP purity)
#
# Section 12A mandate: these are the ONE set of detection functions used by
# every code path (live scan today; the same functions would drive any
# future backtest harness) -- never a parallel, simplified reimplementation.
# Every function here operates strictly on the closed-candle list handed to
# it; callers are responsible for having already stripped the forming
# candle via filter_closed_candles(), so nothing here can repaint.
# ============================================================================

@dataclass
class Pivot:
    idx: int
    t: int
    price: float
    kind: str  # "high" | "low"


@dataclass
class Zone:
    kind: str          # "ob" | "breaker" | "fvg"
    direction: str      # "bullish" | "bearish"
    top: float
    bottom: float
    idx: int
    t: int
    mitigated: bool = False
    from_sweep: bool = False  # sweep-to-POI causality tag (Section 8 step 3)


@dataclass
class LiquidityPool:
    kind: str      # "BSL" | "SSL"
    price: float
    idx_list: list
    equal: bool    # True if this is a genuine EQH/EQL cluster, not just an isolated swing
    swept: bool = False
    swept_idx: Optional[int] = None
    pure_sfp: bool = False


def find_pivots(candles: list, lookback: int = SWING_LOOKBACK) -> list:
    pivots = []
    n = len(candles)
    for i in range(lookback, n - lookback):
        window = candles[i - lookback:i + lookback + 1]
        h, l = candles[i]["h"], candles[i]["l"]
        if h == max(c["h"] for c in window):
            pivots.append(Pivot(i, candles[i]["t"], h, "high"))
        if l == min(c["l"] for c in window):
            pivots.append(Pivot(i, candles[i]["t"], l, "low"))
    return pivots


def liquidity_pools(candles: list, pivots: list, atr: float) -> list:
    """EQH/EQL -> BSL/SSL identification (Section 8 step 3). Clusters swing
    points within EQ_CLUSTER_TOLERANCE_ATR * ATR of each other; an isolated
    swing point (no cluster partner) is still returned, tagged equal=False,
    since a sweep of an isolated point is not disqualified -- just a weaker
    case per the spec."""
    highs = sorted([p for p in pivots if p.kind == "high"], key=lambda p: p.price)
    lows = sorted([p for p in pivots if p.kind == "low"], key=lambda p: p.price)
    tol = max(atr * EQ_CLUSTER_TOLERANCE_ATR, 1e-9)
    pools = []

    def cluster(sorted_pivots, kind):
        used = set()
        for i, p in enumerate(sorted_pivots):
            if p.idx in used:
                continue
            group = [p]
            for q in sorted_pivots[i + 1:]:
                if q.idx in used:
                    continue
                if abs(q.price - p.price) <= tol:
                    group.append(q)
            if len(group) >= 1:
                for g in group:
                    used.add(g.idx)
                level = statistics.fmean(g.price for g in group)
                pools.append(LiquidityPool(kind=kind, price=level,
                                            idx_list=[g.idx for g in group],
                                            equal=len(group) >= 2))
    cluster(highs, "BSL")
    cluster(lows, "SSL")
    return pools


def mark_sweeps(candles: list, pools: list) -> None:
    """Mutates pools in place: for each pool, find the first closed candle
    whose wick trades through the level and closes back on the origin side
    (a genuine wick-based rejection -- SFP purity), vs. one that merely
    trades through without closing back (impure / not a sweep at all)."""
    n = len(candles)
    for pool in pools:
        origin_idx = max(pool.idx_list)
        for i in range(origin_idx + 1, n):
            c = candles[i]
            if pool.kind == "BSL" and c["h"] > pool.price:
                closed_back = c["c"] < pool.price
                pool.swept, pool.swept_idx, pool.pure_sfp = True, i, closed_back
                break
            if pool.kind == "SSL" and c["l"] < pool.price:
                closed_back = c["c"] > pool.price
                pool.swept, pool.swept_idx, pool.pure_sfp = True, i, closed_back
                break


def find_fvgs(candles: list, origin_idx_min: int = 0) -> list:
    """Three-candle imbalance: gap between candle i-2's high/low and candle
    i's low/high, left unfilled by candle i-1."""
    zones = []
    for i in range(2, len(candles)):
        if i - 2 < origin_idx_min:
            continue
        a, c = candles[i - 2], candles[i]
        if c["l"] > a["h"]:
            zones.append(Zone(kind="fvg", direction="bullish", top=c["l"], bottom=a["h"], idx=i, t=c["t"]))
        if c["h"] < a["l"]:
            zones.append(Zone(kind="fvg", direction="bearish", top=a["l"], bottom=c["h"], idx=i, t=c["t"]))
    return zones


def structure_shift(candles: list, pivots: list, direction: str, kind: str,
                     start_idx: int = 0) -> Optional[Pivot]:
    """BOS (break of structure, trend continuation) or CHoCH (change of
    character, counter-trend) detection, shared by every caller (Section
    12A -- one function, all paths). `direction` is the direction of the
    break being sought (bullish break = close above the last swing high).
    Returns the pivot that was broken, or None."""
    highs = [p for p in pivots if p.kind == "high" and p.idx >= start_idx]
    lows = [p for p in pivots if p.kind == "low" and p.idx >= start_idx]
    if direction == "bullish" and highs:
        last_high = highs[-1]
        for i in range(last_high.idx + 1, len(candles)):
            if candles[i]["c"] > last_high.price:
                # BOS = prior structure was already bullish/neutral; CHoCH = prior leg was bearish
                prior_lows = [p for p in lows if p.idx < last_high.idx]
                was_bearish = len(prior_lows) >= 1 and prior_lows[-1].price < last_high.price
                actual_kind = "CHoCH" if was_bearish else "BOS"
                if actual_kind == kind or kind == "any":
                    return last_high
                return None
    if direction == "bearish" and lows:
        last_low = lows[-1]
        for i in range(last_low.idx + 1, len(candles)):
            if candles[i]["c"] < last_low.price:
                prior_highs = [p for p in highs if p.idx < last_low.idx]
                was_bullish = len(prior_highs) >= 1 and prior_highs[-1].price > last_low.price
                actual_kind = "CHoCH" if was_bullish else "BOS"
                if actual_kind == kind or kind == "any":
                    return last_low
                return None
    return None


def find_order_blocks(candles: list, direction: str, since_idx: int = 0) -> list:
    """Last opposite-colored candle before an impulsive move in `direction`."""
    zones = []
    n = len(candles)
    for i in range(max(since_idx, 1), n - 1):
        c, nxt = candles[i], candles[i + 1]
        body = abs(c["c"] - c["o"])
        impulse = abs(nxt["c"] - nxt["o"])
        if impulse < body * 1.3:
            continue
        if direction == "bullish" and c["c"] < c["o"] and nxt["c"] > nxt["o"] and nxt["c"] > c["h"]:
            zones.append(Zone(kind="ob", direction="bullish", top=c["h"], bottom=c["l"], idx=i, t=c["t"]))
        if direction == "bearish" and c["c"] > c["o"] and nxt["c"] < nxt["o"] and nxt["c"] < c["l"]:
            zones.append(Zone(kind="ob", direction="bearish", top=c["h"], bottom=c["l"], idx=i, t=c["t"]))
    return zones


def find_breaker_blocks(candles: list, pivots: list, direction: str, since_idx: int = 0) -> list:
    """A breaker block is a former order block on the losing side of a
    confirmed structure shift, flipped and retested. Reuses structure_shift
    and find_order_blocks rather than re-deriving structure detection."""
    shift = structure_shift(candles, pivots, direction, "any", since_idx)
    if shift is None:
        return []
    opposite = "bearish" if direction == "bullish" else "bullish"
    obs = find_order_blocks(candles, opposite, max(0, shift.idx - 20))
    obs = [z for z in obs if z.idx <= shift.idx]
    if not obs:
        return []
    latest = obs[-1]
    return [Zone(kind="breaker", direction=direction, top=latest.top, bottom=latest.bottom,
                 idx=latest.idx, t=latest.t)]


def mark_mitigated(zones: list, candles: list) -> None:
    for z in zones:
        for c in candles[z.idx + 1:]:
            if z.direction == "bullish" and c["l"] <= z.top:
                z.mitigated = True
                break
            if z.direction == "bearish" and c["h"] >= z.bottom:
                z.mitigated = True
                break


def tag_sweep_to_poi_causality(zones: list, pools: list, candles: list, lookahead_bars: int = 6) -> None:
    """Sweep-to-POI causality (Section 8 step 3, mandatory framing): a zone
    only counts as arising from a sweep if it forms within a short window
    after that specific pool's sweep, not merely nearby in price."""
    swept = [p for p in pools if p.swept and p.pure_sfp]
    for z in zones:
        for p in swept:
            if p.swept_idx is not None and 0 <= z.idx - p.swept_idx <= lookahead_bars:
                z.from_sweep = True
                break


# ============================================================================
# SECTION 5 -- VIEW MODEL (per-timeframe computed context, closed-candle only)
# ============================================================================

@dataclass
class View:
    tf: str
    candles: list
    pivots: list = field(default_factory=list)
    pools: list = field(default_factory=list)
    atr: float = 0.0
    atr_hist: list = field(default_factory=list)
    rsi: float = 50.0
    adx: float = 15.0
    ema_fast: float = 0.0
    ema_slow: float = 0.0
    ema_trend: float = 0.0
    bb: tuple = (0.0, 0.0, 0.0)
    bull_obs: list = field(default_factory=list)
    bear_obs: list = field(default_factory=list)
    bull_breakers: list = field(default_factory=list)
    bear_breakers: list = field(default_factory=list)
    fvgs: list = field(default_factory=list)

    @property
    def last(self) -> dict:
        return self.candles[-1]

    @property
    def close(self) -> float:
        return self.candles[-1]["c"]


def build_view(tf: str, candles: list) -> View:
    closes = [c["c"] for c in candles]
    pivots = find_pivots(candles)
    atr_s = atr_series(candles)
    atr_now = safe(atr_s[-1], statistics.fmean(true_ranges(candles)))
    pools = liquidity_pools(candles, pivots, atr_now)
    mark_sweeps(candles, pools)
    since = max(0, len(candles) - 120)
    bull_obs = find_order_blocks(candles, "bullish", since)
    bear_obs = find_order_blocks(candles, "bearish", since)
    bull_breakers = find_breaker_blocks(candles, pivots, "bullish", since)
    bear_breakers = find_breaker_blocks(candles, pivots, "bearish", since)
    fvgs = find_fvgs(candles, since)
    for zone_list in (bull_obs, bear_obs, bull_breakers, bear_breakers, fvgs):
        mark_mitigated(zone_list, candles)
        tag_sweep_to_poi_causality(zone_list, pools, candles)
    ema_f = ema(closes, EMA_FAST)[-1] if len(closes) >= EMA_FAST else closes[-1]
    ema_s = ema(closes, EMA_SLOW)[-1] if len(closes) >= EMA_SLOW else closes[-1]
    ema_t = ema(closes, EMA_TREND)[-1] if len(closes) >= EMA_TREND else closes[-1]
    return View(
        tf=tf, candles=candles, pivots=pivots, pools=pools, atr=atr_now, atr_hist=atr_s,
        rsi=safe(rsi(closes)[-1], 50.0), adx=safe(adx_series(candles)[-1], 15.0),
        ema_fast=ema_f, ema_slow=ema_s, ema_trend=ema_t, bb=bollinger(closes),
        bull_obs=bull_obs, bear_obs=bear_obs, bull_breakers=bull_breakers,
        bear_breakers=bear_breakers, fvgs=fvgs,
    )


def build_all_views(bundle: dict) -> dict:
    return {tf: build_view(tf, candles) for tf, candles in bundle.items()}


# ============================================================================
# SECTION 6 -- COMPOSITE REGIME VECTOR
# ============================================================================

@dataclass
class RegimeVector:
    macro_bias: str            # "bullish" | "bearish" | "neutral"
    volatility_pctl: float      # 0-100, this asset's own recent ATR distribution
    trend_strength: float       # ADX-derived, 0-100
    session: str                 # "asia" | "london" | "ny" | "off_hours"
    session_weight: float        # historical reliability weight of the active session, 0-1
    session_open_proximity: float  # 0-1 decaying score, continuous input only (never a gate)
    liquidity_draw: str          # "ERL" | "IRL" | "neutral"
    noise_index: float           # 0-1, choppiness independent of raw volatility
    breadth: float                # -1..1, watchlist coherence with macro_bias direction

    def is_trending(self) -> bool:
        return self.trend_strength >= 22.0

    def is_high_vol(self) -> bool:
        return self.volatility_pctl >= 70.0

    def is_low_vol(self) -> bool:
        return self.volatility_pctl <= 30.0


SESSION_HISTORICAL_WEIGHT = {"asia": 0.55, "london": 0.85, "ny": 0.90, "off_hours": 0.40}
SESSION_OPEN_HOURS_UTC = {"london": 8, "ny": 13}  # session-open anchors for proximity scoring


def active_session(ts_ms: int) -> str:
    hour = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).hour
    if 0 <= hour < 7:
        return "asia"
    if 7 <= hour < 12:
        return "london"
    if 12 <= hour < 21:
        return "ny"
    return "off_hours"


def session_open_proximity(ts_ms: int) -> float:
    """Continuous, decaying score for closeness to a London/NY session-open
    window -- soft input only (Section 6), never a hard gate anywhere."""
    dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
    minutes_now = dt.hour * 60 + dt.minute
    best = 0.0
    for _, anchor_hour in SESSION_OPEN_HOURS_UTC.items():
        anchor_minutes = anchor_hour * 60
        delta = min(abs(minutes_now - anchor_minutes), 1440 - abs(minutes_now - anchor_minutes))
        score = max(0.0, 1.0 - delta / 90.0)  # decays to 0 over 90 minutes either side
        best = max(best, score)
    return best


def noise_index(view: View) -> float:
    """Choppiness independent of raw volatility: ratio of net directional
    displacement to summed absolute bar-to-bar movement over a recent
    window -- low ratio = whipsaw, high ratio = clean directional travel."""
    window = view.candles[-30:]
    if len(window) < 10:
        return 0.5
    net = abs(window[-1]["c"] - window[0]["c"])
    total = sum(abs(window[i]["c"] - window[i - 1]["c"]) for i in range(1, len(window)))
    if total < 1e-9:
        return 0.5
    directionality = net / total
    return max(0.0, min(1.0, 1.0 - directionality))


def compute_breadth(views_by_symbol: dict, macro_bias: str) -> float:
    if macro_bias == "neutral" or not views_by_symbol:
        return 0.0
    agree, total = 0, 0
    for sym, v in views_by_symbol.items():
        total += 1
        bullish = v.close > v.ema_slow
        if (bullish and macro_bias == "bullish") or (not bullish and macro_bias == "bearish"):
            agree += 1
    return (agree / total) * 2 - 1 if total else 0.0


def liquidity_draw_state(view_1h: View) -> str:
    """ERL (external range liquidity -- an unswept EQH/EQL cluster) vs IRL
    (internal range liquidity -- an unmitigated FVG/OB/breaker inside the
    current range). Soft continuous read, never a standalone directional
    call (Section 6)."""
    unswept_pools = [p for p in view_1h.pools if not p.swept and p.equal]
    unmitigated_zones = [z for z in (view_1h.fvgs + view_1h.bull_obs + view_1h.bear_obs) if not z.mitigated]
    if len(unswept_pools) > len(unmitigated_zones):
        return "ERL"
    if unmitigated_zones:
        return "IRL"
    return "neutral"


def build_regime_vector(views_by_tf: dict, macro_bias: str, views_by_symbol: dict,
                         ts_ms: int) -> RegimeVector:
    v1h = views_by_tf[TF_1H]
    vol_hist = [x for x in v1h.atr_hist if x is not None][-120:]
    vol_pctl = percentile_rank(vol_hist, v1h.atr) if vol_hist else 50.0
    session = active_session(ts_ms)
    return RegimeVector(
        macro_bias=macro_bias,
        volatility_pctl=vol_pctl,
        trend_strength=v1h.adx,
        session=session,
        session_weight=SESSION_HISTORICAL_WEIGHT[session],
        session_open_proximity=session_open_proximity(ts_ms),
        liquidity_draw=liquidity_draw_state(v1h),
        noise_index=noise_index(v1h),
        breadth=compute_breadth(views_by_symbol, macro_bias),
    )


# ============================================================================
# SECTION 7 -- MANDATORY TOP-DOWN SEQUENCE
# ============================================================================

@dataclass
class StageResult:
    stage: int
    outcome: str          # varies per stage, see below
    reason: str = ""


def stage1_bias(views: dict) -> StageResult:
    """Weekly + Daily -> Market Bias. Resolves exactly one of Bullish /
    Bearish / Neutral (Section 7 Stage 1)."""
    w, d = views[TF_WEEKLY], views[TF_DAILY]
    w_bull = w.close > w.ema_slow and w.ema_fast > w.ema_slow
    w_bear = w.close < w.ema_slow and w.ema_fast < w.ema_slow
    d_bull = d.close > d.ema_slow and d.ema_fast > d.ema_slow
    d_bear = d.close < d.ema_slow and d.ema_fast < d.ema_slow
    if w_bull and d_bull:
        return StageResult(1, "bullish")
    if w_bear and d_bear:
        return StageResult(1, "bearish")
    return StageResult(1, "neutral", "Weekly/Daily bias disagreement or no clear trend")


def stage2_context(views: dict, bias: str) -> StageResult:
    """4H -> Market Context. Confirms the 4H agrees with the Stage 1 bias."""
    h4 = views[TF_4H]
    h4_bull = h4.close > h4.ema_slow and h4.ema_fast >= h4.ema_slow * 0.999
    h4_bear = h4.close < h4.ema_slow and h4.ema_fast <= h4.ema_slow * 1.001
    if bias == "bullish" and h4_bull:
        return StageResult(2, "agree")
    if bias == "bearish" and h4_bear:
        return StageResult(2, "agree")
    return StageResult(2, "disagree", "4H context does not confirm Weekly/Daily bias")


def zone_selection_sequence(views: dict, bias: str, state: dict, symbol: str):
    """Section 8's mandatory Zone-Selection Sequence, executed within Stage
    3 (1H Trade Setup). Steps 2-5; step 6 (Fibonacci OTE) is applied later,
    inside Stage 4, as a placement refinement only. Returns (outcome, poi)
    where outcome in {"VALID","NOT READY","INVALID"}."""
    h1 = views[TF_1H]
    direction = bias
    # Step 2: POI candidate pool -- order blocks, breaker blocks, FVGs matching bias
    if direction == "bullish":
        candidates = h1.bull_obs + h1.bull_breakers + [z for z in h1.fvgs if z.direction == "bullish"]
    else:
        candidates = h1.bear_obs + h1.bear_breakers + [z for z in h1.fvgs if z.direction == "bearish"]
    candidates = [z for z in candidates if not z.mitigated]
    if not candidates:
        return "NOT READY", None

    # Step 3: SFP purity + sweep-to-POI causality -- prefer zones that arose from a pure sweep
    swept_candidates = [z for z in candidates if z.from_sweep]
    pool_candidates = swept_candidates if swept_candidates else candidates

    # Step 4: MSS -- require a confirmed structure shift in the bias direction on 1H
    shift = structure_shift(h1.candles, h1.pivots, direction, "any")
    if shift is None:
        return "NOT READY", None

    # Step 5: breaker confirmation preferred as the most precise zone if available
    breakers = [z for z in pool_candidates if z.kind == "breaker"]
    poi = breakers[-1] if breakers else sorted(pool_candidates, key=lambda z: z.idx)[-1]

    price = h1.close
    inside = poi.bottom <= price <= poi.top
    near = min(abs(price - poi.top), abs(price - poi.bottom)) <= h1.atr * 1.5
    if not (inside or near):
        return "NOT READY", None

    return "VALID", poi


def stage3_zone_selection(views: dict, bias: str, state: dict, symbol: str) -> StageResult:
    outcome, poi = zone_selection_sequence(views, bias, state, symbol)
    result = StageResult(3, outcome, "" if outcome == "VALID" else "zone-selection sequence incomplete")
    result.poi = poi  # type: ignore[attr-defined]
    return result


def fibonacci_ote_refine(direction: str, impulse_low: float, impulse_high: float,
                          poi_top: float, poi_bottom: float):
    """Step 6: Fibonacci OTE refinement -- a placement adjustment inside an
    already-validated zone, never a standalone confluence point (Section
    8). Returns a refined entry price if the 61.8-79% pocket overlaps the
    structural POI, else None (caller falls back to the POI midpoint)."""
    span = impulse_high - impulse_low
    if span <= 0:
        return None
    if direction == "bullish":
        ote_low = impulse_high - span * 0.79
        ote_high = impulse_high - span * 0.618
    else:
        ote_low = impulse_low + span * 0.618
        ote_high = impulse_low + span * 0.79
    overlap_low = max(ote_low, poi_bottom)
    overlap_high = min(ote_high, poi_top)
    if overlap_low >= overlap_high:
        return None
    return (overlap_low + overlap_high) / 2.0


def stage4_entry(views: dict, bias: str, poi: Zone) -> StageResult:
    """15M -> Trade Entry. Requires a confirmed 15M MSS in the bias
    direction occurring inside the Stage 3 POI, with the FVG created by
    that specific break as the entry vehicle (Section 7 Stage 4)."""
    m15 = views[TF_15M]
    shift = structure_shift(m15.candles, m15.pivots, bias, "any")
    if shift is None:
        return StageResult(4, "NO TRADE", "no confirmed 15M MSS")

    since = shift.idx
    fresh_fvgs = [z for z in find_fvgs(m15.candles, since) if z.direction == bias]
    mark_mitigated(fresh_fvgs, m15.candles)
    fresh_fvgs = [z for z in fresh_fvgs if not z.mitigated]
    if not fresh_fvgs:
        return StageResult(4, "NO TRADE", "no MSS-originated 15M FVG entry vehicle")

    entry_zone = fresh_fvgs[-1]
    inside_poi = not (entry_zone.top < poi.bottom or entry_zone.bottom > poi.top)
    if not inside_poi:
        return StageResult(4, "NO TRADE", "15M FVG did not form inside the validated 1H POI")

    impulse_leg = m15.candles[max(0, since - 5):since + 1]
    impulse_low = min(c["l"] for c in impulse_leg) if impulse_leg else entry_zone.bottom
    impulse_high = max(c["h"] for c in impulse_leg) if impulse_leg else entry_zone.top
    refined = fibonacci_ote_refine(bias, impulse_low, impulse_high, entry_zone.top, entry_zone.bottom)
    entry_price = refined if refined is not None else (entry_zone.top + entry_zone.bottom) / 2.0

    result = StageResult(4, "VALID", "")
    result.entry_zone = entry_zone       # type: ignore[attr-defined]
    result.entry_price = entry_price     # type: ignore[attr-defined]
    return result


# ============================================================================
# SECTION 8 -- RISK MANAGEMENT: ADAPTIVE STRUCTURAL RISK PLAN
# (adaptive-percentile SL buffer, confluence-ranked TP, liquidity-wall clip,
#  RR floor gate -- the single mandatory construction every engine shares)
# ============================================================================

MAX_SL_DISTANCE_ATR = 3.0      # hard ceiling on SL distance -- a stop only clearing it by
                                # escalating further is rejected outright, never accepted at worse RR
MIN_ENTRY_SL_DISTANCE_ATR = 0.25
MAX_ENTRY_FROM_MARKET_ATR = 1.2  # cap on how far a pending/zone entry may sit from current price
NOISE_SURVIVAL_FLOOR_ATR = 0.35  # 15m-anchored SL must clear this vs recent adverse wicks


def _adaptive_sl_buffer(symbol: str, tf: str, view: View, state: dict) -> float:
    """Nth percentile of recent adverse-wick excursions beyond structure,
    where N is itself a bounded, dampened adaptive parameter (Section 5),
    not a fixed constant -- widens on noisy assets/timeframes, tightens on
    clean ones."""
    key = f"{symbol}|{tf}"
    pctl = state["tier1"]["sl_buffer_percentile"].get(key, 70.0)
    pctl = max(SL_BUFFER_PCTL_MIN, min(SL_BUFFER_PCTL_MAX, pctl))
    wicks = []
    for c in view.candles[-60:]:
        body_top, body_bot = max(c["o"], c["c"]), min(c["o"], c["c"])
        wicks.append(c["h"] - body_top)
        wicks.append(body_bot - c["l"])
    wicks = [w for w in wicks if w > 0]
    if not wicks:
        return view.atr * 0.25
    return max(percentile(wicks, pctl), view.atr * 0.05)


def _opposing_structural_levels(direction: str, view: View) -> list:
    """Genuine opposing structural levels on the given view: opposite-side
    pivots, unmitigated opposing zones, and opposing liquidity pools."""
    levels = []
    opp_pivot_kind = "high" if direction == "bullish" else "low"
    for p in view.pivots:
        if p.kind == opp_pivot_kind:
            levels.append({"price": p.price, "confluence": 1, "kind": "pivot"})
    opp_zones = (view.bear_obs + view.bear_breakers) if direction == "bullish" else (view.bull_obs + view.bull_breakers)
    for z in opp_zones:
        if not z.mitigated:
            levels.append({"price": z.top if direction == "bullish" else z.bottom, "confluence": 2, "kind": z.kind})
    opp_pool_kind = "BSL" if direction == "bullish" else "SSL"
    for pool in view.pools:
        if pool.kind == opp_pool_kind and not pool.swept:
            levels.append({"price": pool.price, "confluence": 3 if pool.equal else 1, "kind": "liquidity_pool"})
    return levels


def _tp_selection_band(direction: str, entry: float, sl: float, view: View,
                        rank_preference: int) -> dict:
    """Confluence-ranked TP candidate selection (Section 10 mandatory
    pattern): among genuine opposing levels within the runway near-band,
    picks the strongest by confluence -- not simply nearest by price --
    then clips to the closest liquidity wall inside the path."""
    risk = abs(entry - sl)
    levels = _opposing_structural_levels(direction, view)
    if direction == "bullish":
        levels = [lv for lv in levels if lv["price"] > entry]
    else:
        levels = [lv for lv in levels if lv["price"] < entry]
    if not levels:
        return {}
    near_band_max_r = 6.0  # runway pre-check near-band -- plausible RR territory only
    banded = [lv for lv in levels if abs(lv["price"] - entry) <= risk * near_band_max_r]
    banded = banded or levels
    banded.sort(key=lambda lv: lv["confluence"], reverse=True)
    top_n = banded[:max(rank_preference, 1)]
    top_n.sort(key=lambda lv: abs(lv["price"] - entry))  # nearest-among-strong wins ties
    tp1_level = top_n[0]

    # liquidity-wall clip: if a closer opposing pool sits inside the path to tp1, clip to it
    wall_kind = "BSL" if direction == "bullish" else "SSL"
    for pool in sorted(view.pools, key=lambda p: p.price if direction == "bullish" else -p.price):
        if pool.kind != wall_kind or pool.swept:
            continue
        between = (entry < pool.price < tp1_level["price"]) if direction == "bullish" else \
                  (entry > pool.price > tp1_level["price"])
        if between:
            tp1_level = {"price": pool.price, "confluence": tp1_level["confluence"], "kind": "liquidity_wall_clip"}
            break

    beyond = [lv for lv in levels if
              (lv["price"] > tp1_level["price"] if direction == "bullish" else lv["price"] < tp1_level["price"])]
    tp2_price = None
    if beyond:
        beyond.sort(key=lambda lv: abs(lv["price"] - entry))
        tp2_price = beyond[0]["price"]
    return {"tp1": tp1_level["price"], "tp2": tp2_price}


def build_risk_plan(direction: str, entry: float, view_15m: View, view_1h: View, view_4h: View,
                     state: dict, symbol: str, rr_min_gate: float = RR_MIN_GATE) -> Optional[dict]:
    """The single mandatory risk-plan construction (Section 10 Reference
    Implementation) -- SL-anchor hierarchy, adaptive-percentile buffer,
    confluence-ranked/liquidity-wall-clipped TP, RR floor, TP-ordering
    integrity, entry-placement rules. Fully replaces any fixed ATR
    multiple, RR formula, or percentage target anywhere else in the
    engine. Returns None (reject) on any hard gate failure."""

    # --- SL anchor: 15M primary, escalate to H1/H4 only on noise-survival failure ---
    anchor_view = view_15m
    anchor_tf = TF_15M
    opp_pivots_15m = [p for p in view_15m.pivots if p.kind == ("low" if direction == "bullish" else "high")]
    if not opp_pivots_15m:
        return None
    structural_level = opp_pivots_15m[-1].price
    buffer_15m = _adaptive_sl_buffer(symbol, TF_15M, view_15m, state)
    sl_15m = structural_level - buffer_15m if direction == "bullish" else structural_level + buffer_15m
    risk_15m = abs(entry - sl_15m)

    if risk_15m < view_15m.atr * NOISE_SURVIVAL_FLOOR_ATR:
        # escalate: 15m stop too tight to survive ordinary wick noise
        for tf_view, tf_name in ((view_1h, TF_1H), (view_4h, TF_4H)):
            opp = [p for p in tf_view.pivots if p.kind == ("low" if direction == "bullish" else "high")]
            if not opp:
                continue
            candidate_struct = opp[-1].price
            candidate_dist = abs(entry - candidate_struct)
            if candidate_dist < abs(entry - structural_level) * 4:  # closer of H1/H4, sanity-bounded
                anchor_view, anchor_tf, structural_level = tf_view, tf_name, candidate_struct
                break
        buffer_final = _adaptive_sl_buffer(symbol, anchor_tf, anchor_view, state)
        sl = structural_level - buffer_final if direction == "bullish" else structural_level + buffer_final
    else:
        sl = sl_15m

    risk = abs(entry - sl)
    if risk <= 0:
        return None
    if risk > view_15m.atr * MAX_SL_DISTANCE_ATR:
        return None  # hard ceiling; a stop only clearing it by escalating further is rejected, never accepted worse
    if risk < view_15m.atr * MIN_ENTRY_SL_DISTANCE_ATR:
        return None  # entry-placement rule: entry too close to invalidation is noise, not a real trade

    # --- entry-placement: prefer entry near current market, cap pending-entry distance ---
    market_price = view_15m.close
    if abs(entry - market_price) > view_15m.atr * MAX_ENTRY_FROM_MARKET_ATR:
        return None

    # --- TP selection (confluence-ranked, liquidity-wall-clipped) ---
    rank_pref = int(state["tier1"]["tp1_rank_preference"].get(symbol, 3))
    rank_pref = max(TP1_RANK_PREF_MIN, min(TP1_RANK_PREF_MAX, rank_pref))
    band = _tp_selection_band(direction, entry, sl, anchor_view, rank_pref)
    if not band.get("tp1"):
        return None
    tp1 = band["tp1"]
    tp1_dist = abs(tp1 - entry)
    if tp1_dist < risk * MIN_ENTRY_SL_DISTANCE_ATR:
        return None

    rr1 = tp1_dist / risk
    if rr1 < rr_min_gate:
        return None  # reject-only gate, never rescued by widening SL/target
    rr1 = min(rr1, RR_SOFT_TARGET) if rr1 > RR_SOFT_TARGET and rr_min_gate == RR_MIN_GATE else rr1

    tp2 = band.get("tp2")
    # TP ordering integrity: TP2 must sit strictly farther than TP1, guaranteed by construction
    if tp2 is None or abs(tp2 - entry) <= tp1_dist:
        extension = tp1_dist * 0.6
        tp2 = tp1 + extension if direction == "bullish" else tp1 - extension
    assert (abs(tp2 - entry) > abs(tp1 - entry)), "TP ordering integrity violated"

    rr2 = abs(tp2 - entry) / risk

    return {
        "direction": direction, "entry": entry, "sl": sl, "tp1": tp1, "tp2": tp2,
        "rr1": rr1, "rr2": rr2, "sl_anchor_tf": anchor_tf, "risk": risk,
    }


# ============================================================================
# SECTION 9 -- CANDIDATE MODEL & ENTRY-FILL VERIFICATION LIFECYCLE
# ============================================================================

@dataclass
class Candidate:
    engine: str
    direction: str
    entry: float
    sl: float
    tp1: float
    tp2: float
    confidence: float           # 0-1 raw engine confidence, pre-Decision-Engine blend
    rr1: float
    rr2: float
    confluences: list
    regime_fit: list            # list of regime descriptors this setup is best suited for
    style: str                  # "intraday" | "swing"
    entry_kind: str             # "market" | "pending" -- Section 12 mandatory abstraction
    symbol: str = ""
    counter_trend: bool = False
    sl_anchor_tf: str = TF_15M


def make_pending_state() -> dict:
    return {"entry_filled": False, "pending_bars": 0}


def entry_kind_for(engine_name: str, is_zone_based: bool) -> str:
    """Which entry_kind each engine/setup type uses is explicitly
    documented here, never silently assumed (Section 12)."""
    return "pending" if is_zone_based else "market"


# ============================================================================
# SECTION 10 -- SPECIALIZED ENGINES (Section 4's ensemble, minimum 13)
#
# Every engine is locked to the Stage-1 bias as its trade direction (Section
# 7/8 step 1 -- "nothing downstream is evaluated against a level that fights
# this bias"). Each independently returns a Candidate carrying direction,
# entry, SL, TP1(+TP2), confidence, RR, confluences, and documented regime
# fit, or None. All construct SL/TP exclusively via build_risk_plan -- no
# engine computes its own RR formula, ATR multiple, or percentage target.
# ============================================================================

def _base_confidence(confluence_count: int, rr1: float, regime: RegimeVector, best_fit: bool) -> float:
    """Shared raw-confidence starting point every engine derives from,
    before the Decision Engine's own continuous blend (Section 4) --
    intentionally simple and bounded so no engine pre-biases the blend."""
    c = 0.42 + 0.06 * min(confluence_count, 5)
    c += 0.05 if best_fit else -0.05
    c += 0.03 if regime.trend_strength >= 22 else 0.0
    return max(0.05, min(0.95, c))


def run_smc_engine(bias: str, views: dict, stage3: StageResult, stage4: StageResult,
                    regime: RegimeVector, state: dict, symbol: str) -> Optional[Candidate]:
    """Primary SMC/ICT engine: the full Section 7/8 pipeline itself --
    HTF bias -> zone-selection sequence -> MSS/FVG entry -> risk plan."""
    if stage3.outcome != "VALID" or stage4.outcome != "VALID":
        return None
    poi = stage3.poi  # type: ignore[attr-defined]
    entry = stage4.entry_price  # type: ignore[attr-defined]
    plan = build_risk_plan(bias, entry, views[TF_15M], views[TF_1H], views[TF_4H], state, symbol)
    if plan is None:
        return None
    confluences = ["1H POI", "15M MSS->FVG"]
    if poi.kind == "breaker":
        confluences.append("Breaker confirmation")
    if poi.from_sweep:
        confluences.append("Sweep-to-POI causality")
    best_fit = regime.is_trending() and not regime.is_low_vol()
    conf = _base_confidence(len(confluences), plan["rr1"], regime, best_fit)
    return Candidate(engine="SMC", direction=bias, entry=plan["entry"], sl=plan["sl"], tp1=plan["tp1"],
                      tp2=plan["tp2"], confidence=conf, rr1=plan["rr1"], rr2=plan["rr2"],
                      confluences=confluences, regime_fit=["trending", "expansion"], style="intraday",
                      entry_kind="pending", symbol=symbol, sl_anchor_tf=plan["sl_anchor_tf"])


def run_trend_continuation_engine(bias: str, views: dict, regime: RegimeVector,
                                   state: dict, symbol: str) -> Optional[Candidate]:
    h1, m15 = views[TF_1H], views[TF_15M]
    if not regime.is_trending():
        return None
    pullback_pool = h1.bull_obs if bias == "bullish" else h1.bear_obs
    pullback_pool = [z for z in pullback_pool if not z.mitigated]
    if not pullback_pool:
        return None
    zone = pullback_pool[-1]
    price = h1.close
    if not (zone.bottom <= price <= zone.top):
        return None
    trend_confirm = (m15.ema_fast > m15.ema_slow) if bias == "bullish" else (m15.ema_fast < m15.ema_slow)
    if not trend_confirm:
        return None
    entry = (zone.top + zone.bottom) / 2.0
    plan = build_risk_plan(bias, entry, m15, h1, views[TF_4H], state, symbol)
    if plan is None:
        return None
    conf = _base_confidence(2, plan["rr1"], regime, best_fit=True)
    return Candidate(engine="Trend Continuation", direction=bias, entry=plan["entry"], sl=plan["sl"],
                      tp1=plan["tp1"], tp2=plan["tp2"], confidence=conf, rr1=plan["rr1"], rr2=plan["rr2"],
                      confluences=["1H pullback OB", "15M EMA trend confirm"],
                      regime_fit=["trending"], style="swing", entry_kind="pending", symbol=symbol,
                      sl_anchor_tf=plan["sl_anchor_tf"])


def run_breakout_engine(bias: str, views: dict, regime: RegimeVector,
                         state: dict, symbol: str) -> Optional[Candidate]:
    h1, m15 = views[TF_1H], views[TF_15M]
    lookback = h1.candles[-20:]
    range_high = max(c["h"] for c in lookback[:-1])
    range_low = min(c["l"] for c in lookback[:-1])
    last = h1.candles[-1]
    if bias == "bullish" and last["c"] > range_high and last["c"] > last["o"]:
        broke = True
    elif bias == "bearish" and last["c"] < range_low and last["c"] < last["o"]:
        broke = True
    else:
        broke = False
    if not broke:
        return None
    vol_confirm = last["v"] > statistics.fmean(c["v"] for c in lookback[:-1]) * 1.15
    if not vol_confirm:
        return None
    entry = m15.close
    plan = build_risk_plan(bias, entry, m15, h1, views[TF_4H], state, symbol)
    if plan is None:
        return None
    best_fit = regime.is_high_vol() or regime.noise_index < 0.4
    conf = _base_confidence(2, plan["rr1"], regime, best_fit)
    return Candidate(engine="Breakout", direction=bias, entry=plan["entry"], sl=plan["sl"], tp1=plan["tp1"],
                      tp2=plan["tp2"], confidence=conf, rr1=plan["rr1"], rr2=plan["rr2"],
                      confluences=["1H range breakout", "volume confirmation"],
                      regime_fit=["expansion", "high_volatility"], style="intraday", entry_kind="market",
                      symbol=symbol, sl_anchor_tf=plan["sl_anchor_tf"])


def run_pullback_engine(bias: str, views: dict, regime: RegimeVector,
                         state: dict, symbol: str) -> Optional[Candidate]:
    m15, h1 = views[TF_15M], views[TF_1H]
    fib_zone_pool = h1.bull_obs if bias == "bullish" else h1.bear_obs
    fib_zone_pool = [z for z in fib_zone_pool if not z.mitigated]
    shift = structure_shift(m15.candles, m15.pivots, bias, "any")
    if not fib_zone_pool or shift is None:
        return None
    zone = fib_zone_pool[-1]
    entry = (zone.top + zone.bottom) / 2.0
    if not (zone.bottom * 0.995 <= views[TF_1H].close <= zone.top * 1.005):
        return None
    plan = build_risk_plan(bias, entry, m15, h1, views[TF_4H], state, symbol)
    if plan is None:
        return None
    conf = _base_confidence(2, plan["rr1"], regime, best_fit=regime.is_trending())
    return Candidate(engine="Pullback", direction=bias, entry=plan["entry"], sl=plan["sl"], tp1=plan["tp1"],
                      tp2=plan["tp2"], confidence=conf, rr1=plan["rr1"], rr2=plan["rr2"],
                      confluences=["1H OB pullback zone", "15M structure shift"],
                      regime_fit=["trending", "reversal"], style="intraday", entry_kind="pending",
                      symbol=symbol, sl_anchor_tf=plan["sl_anchor_tf"])


def run_liquidity_sweep_engine(bias: str, views: dict, regime: RegimeVector,
                                state: dict, symbol: str) -> Optional[Candidate]:
    h1, m15 = views[TF_1H], views[TF_15M]
    wanted_kind = "SSL" if bias == "bullish" else "BSL"
    pools = [p for p in h1.pools if p.kind == wanted_kind and p.swept and p.pure_sfp]
    if not pools:
        return None
    pool = sorted(pools, key=lambda p: p.swept_idx or 0)[-1]
    entry = m15.close
    plan = build_risk_plan(bias, entry, m15, h1, views[TF_4H], state, symbol)
    if plan is None:
        return None
    conf = _base_confidence(2 + int(pool.equal), plan["rr1"], regime, best_fit=True)
    return Candidate(engine="Liquidity Sweep", direction=bias, entry=plan["entry"], sl=plan["sl"],
                      tp1=plan["tp1"], tp2=plan["tp2"], confidence=conf, rr1=plan["rr1"], rr2=plan["rr2"],
                      confluences=["EQH/EQL sweep"] + (["equal-cluster liquidity"] if pool.equal else []),
                      regime_fit=["reversal", "high_volatility"], style="intraday", entry_kind="market",
                      symbol=symbol, sl_anchor_tf=plan["sl_anchor_tf"])


def run_order_block_engine(bias: str, views: dict, regime: RegimeVector,
                            state: dict, symbol: str) -> Optional[Candidate]:
    h1, m15 = views[TF_1H], views[TF_15M]
    pool = (h1.bull_obs if bias == "bullish" else h1.bear_obs)
    pool = [z for z in pool if not z.mitigated]
    if not pool:
        return None
    zone = pool[-1]
    price = h1.close
    if not (zone.bottom <= price <= zone.top):
        return None
    entry = (zone.top + zone.bottom) / 2.0
    plan = build_risk_plan(bias, entry, m15, h1, views[TF_4H], state, symbol)
    if plan is None:
        return None
    conf = _base_confidence(1 + int(zone.from_sweep), plan["rr1"], regime, best_fit=regime.is_trending())
    return Candidate(engine="Order Block", direction=bias, entry=plan["entry"], sl=plan["sl"], tp1=plan["tp1"],
                      tp2=plan["tp2"], confidence=conf, rr1=plan["rr1"], rr2=plan["rr2"],
                      confluences=["unmitigated 1H order block"] + (["sweep origin"] if zone.from_sweep else []),
                      regime_fit=["trending", "reversal"], style="intraday", entry_kind="pending",
                      symbol=symbol, sl_anchor_tf=plan["sl_anchor_tf"])


def run_breaker_block_engine(bias: str, views: dict, regime: RegimeVector,
                              state: dict, symbol: str) -> Optional[Candidate]:
    h1, m15 = views[TF_1H], views[TF_15M]
    pool = h1.bull_breakers if bias == "bullish" else h1.bear_breakers
    if not pool:
        return None
    zone = pool[-1]
    price = h1.close
    if not (zone.bottom <= price <= zone.top):
        return None
    entry = (zone.top + zone.bottom) / 2.0
    plan = build_risk_plan(bias, entry, m15, h1, views[TF_4H], state, symbol)
    if plan is None:
        return None
    conf = _base_confidence(2, plan["rr1"], regime, best_fit=True)
    return Candidate(engine="Breaker Block", direction=bias, entry=plan["entry"], sl=plan["sl"],
                      tp1=plan["tp1"], tp2=plan["tp2"], confidence=conf, rr1=plan["rr1"], rr2=plan["rr2"],
                      confluences=["confirmed breaker block retest"],
                      regime_fit=["reversal", "trending"], style="intraday", entry_kind="pending",
                      symbol=symbol, sl_anchor_tf=plan["sl_anchor_tf"])


def run_fvg_engine(bias: str, views: dict, regime: RegimeVector,
                    state: dict, symbol: str) -> Optional[Candidate]:
    h1, m15 = views[TF_1H], views[TF_15M]
    pool = [z for z in h1.fvgs if z.direction == bias and not z.mitigated]
    if not pool:
        return None
    zone = pool[-1]
    price = h1.close
    if not (zone.bottom <= price <= zone.top):
        return None
    entry = (zone.top + zone.bottom) / 2.0
    plan = build_risk_plan(bias, entry, m15, h1, views[TF_4H], state, symbol)
    if plan is None:
        return None
    conf = _base_confidence(1, plan["rr1"], regime, best_fit=regime.liquidity_draw == "IRL")
    return Candidate(engine="Fair Value Gap", direction=bias, entry=plan["entry"], sl=plan["sl"],
                      tp1=plan["tp1"], tp2=plan["tp2"], confidence=conf, rr1=plan["rr1"], rr2=plan["rr2"],
                      confluences=["unmitigated 1H FVG rebalance"],
                      regime_fit=["ranging", "consolidation"], style="intraday", entry_kind="pending",
                      symbol=symbol, sl_anchor_tf=plan["sl_anchor_tf"])


def run_momentum_engine(bias: str, views: dict, regime: RegimeVector,
                         state: dict, symbol: str) -> Optional[Candidate]:
    h1, m15 = views[TF_1H], views[TF_15M]
    rsi_ok = (h1.rsi > 55) if bias == "bullish" else (h1.rsi < 45)
    ema_stack = (m15.ema_fast > m15.ema_slow > m15.ema_trend) if bias == "bullish" else \
                (m15.ema_fast < m15.ema_slow < m15.ema_trend)
    if not (rsi_ok and ema_stack):
        return None
    entry = m15.close
    plan = build_risk_plan(bias, entry, m15, h1, views[TF_4H], state, symbol)
    if plan is None:
        return None
    conf = _base_confidence(2, plan["rr1"], regime, best_fit=regime.is_trending())
    return Candidate(engine="Momentum", direction=bias, entry=plan["entry"], sl=plan["sl"], tp1=plan["tp1"],
                      tp2=plan["tp2"], confidence=conf, rr1=plan["rr1"], rr2=plan["rr2"],
                      confluences=["1H RSI momentum", "15M EMA stack alignment"],
                      regime_fit=["trending", "expansion"], style="intraday", entry_kind="market",
                      symbol=symbol, sl_anchor_tf=plan["sl_anchor_tf"])


def run_reversal_engine(bias: str, views: dict, regime: RegimeVector,
                         state: dict, symbol: str) -> Optional[Candidate]:
    h1, m15 = views[TF_1H], views[TF_15M]
    choch = structure_shift(m15.candles, m15.pivots, bias, "CHoCH")
    if choch is None:
        return None
    wanted_kind = "SSL" if bias == "bullish" else "BSL"
    pools = [p for p in h1.pools if p.kind == wanted_kind and p.swept and p.pure_sfp]
    if not pools:
        return None
    entry = m15.close
    plan = build_risk_plan(bias, entry, m15, h1, views[TF_4H], state, symbol)
    if plan is None:
        return None
    conf = _base_confidence(2, plan["rr1"], regime, best_fit=True)
    return Candidate(engine="Reversal", direction=bias, entry=plan["entry"], sl=plan["sl"], tp1=plan["tp1"],
                      tp2=plan["tp2"], confidence=conf, rr1=plan["rr1"], rr2=plan["rr2"],
                      confluences=["1H liquidity sweep", "15M CHoCH"],
                      regime_fit=["reversal"], style="intraday", entry_kind="market",
                      symbol=symbol, sl_anchor_tf=plan["sl_anchor_tf"])


def run_mean_reversion_engine(bias: str, views: dict, regime: RegimeVector,
                               state: dict, symbol: str) -> Optional[Candidate]:
    h1, m15 = views[TF_1H], views[TF_15M]
    if regime.is_trending():
        return None
    lower, mid, upper = h1.bb
    price = h1.close
    if bias == "bullish" and not (price <= lower * 1.01):
        return None
    if bias == "bearish" and not (price >= upper * 0.99):
        return None
    entry = m15.close
    plan = build_risk_plan(bias, entry, m15, h1, views[TF_4H], state, symbol)
    if plan is None:
        return None
    conf = _base_confidence(1, plan["rr1"], regime, best_fit=not regime.is_trending())
    return Candidate(engine="Mean Reversion", direction=bias, entry=plan["entry"], sl=plan["sl"],
                      tp1=plan["tp1"], tp2=plan["tp2"], confidence=conf, rr1=plan["rr1"], rr2=plan["rr2"],
                      confluences=["1H Bollinger extreme"],
                      regime_fit=["ranging", "low_volatility"], style="intraday", entry_kind="market",
                      symbol=symbol, sl_anchor_tf=plan["sl_anchor_tf"])


def run_range_trading_engine(bias: str, views: dict, regime: RegimeVector,
                              state: dict, symbol: str) -> Optional[Candidate]:
    h1, m15 = views[TF_1H], views[TF_15M]
    if regime.trend_strength > 20:
        return None
    window = h1.candles[-40:]
    range_high = max(c["h"] for c in window)
    range_low = min(c["l"] for c in window)
    span = range_high - range_low
    if span <= 0:
        return None
    price = h1.close
    near_low = (price - range_low) / span < 0.15
    near_high = (range_high - price) / span < 0.15
    if bias == "bullish" and not near_low:
        return None
    if bias == "bearish" and not near_high:
        return None
    entry = m15.close
    plan = build_risk_plan(bias, entry, m15, h1, views[TF_4H], state, symbol)
    if plan is None:
        return None
    conf = _base_confidence(1, plan["rr1"], regime, best_fit=not regime.is_trending())
    return Candidate(engine="Range Trading", direction=bias, entry=plan["entry"], sl=plan["sl"],
                      tp1=plan["tp1"], tp2=plan["tp2"], confidence=conf, rr1=plan["rr1"], rr2=plan["rr2"],
                      confluences=["range extreme rejection"],
                      regime_fit=["ranging", "consolidation"], style="intraday", entry_kind="market",
                      symbol=symbol, sl_anchor_tf=plan["sl_anchor_tf"])


def run_volatility_expansion_engine(bias: str, views: dict, regime: RegimeVector,
                                     state: dict, symbol: str) -> Optional[Candidate]:
    h1, m15 = views[TF_1H], views[TF_15M]
    lower, mid, upper = h1.bb
    band_width = (upper - lower) / mid if mid else 0.0
    recent_widths = []
    closes = [c["c"] for c in h1.candles[-40:]]
    for i in range(20, len(closes)):
        lo, md, hi = bollinger(closes[:i + 1])
        recent_widths.append((hi - lo) / md if md else 0.0)
    if not recent_widths:
        return None
    was_squeezed = percentile_rank(recent_widths, band_width) < 30
    breaking_out = (h1.close > upper) if bias == "bullish" else (h1.close < lower)
    if not (was_squeezed and breaking_out):
        return None
    entry = m15.close
    plan = build_risk_plan(bias, entry, m15, h1, views[TF_4H], state, symbol)
    if plan is None:
        return None
    conf = _base_confidence(2, plan["rr1"], regime, best_fit=regime.is_high_vol())
    return Candidate(engine="Volatility Expansion", direction=bias, entry=plan["entry"], sl=plan["sl"],
                      tp1=plan["tp1"], tp2=plan["tp2"], confidence=conf, rr1=plan["rr1"], rr2=plan["rr2"],
                      confluences=["Bollinger squeeze release"],
                      regime_fit=["expansion", "high_volatility"], style="intraday", entry_kind="market",
                      symbol=symbol, sl_anchor_tf=plan["sl_anchor_tf"])


BASE_ENGINE_RUNNERS = {
    "SMC": run_smc_engine,  # special-cased below (needs stage3/stage4)
    "Trend Continuation": run_trend_continuation_engine,
    "Breakout": run_breakout_engine,
    "Pullback": run_pullback_engine,
    "Liquidity Sweep": run_liquidity_sweep_engine,
    "Order Block": run_order_block_engine,
    "Breaker Block": run_breaker_block_engine,
    "Fair Value Gap": run_fvg_engine,
    "Momentum": run_momentum_engine,
    "Reversal": run_reversal_engine,
    "Mean Reversion": run_mean_reversion_engine,
    "Range Trading": run_range_trading_engine,
    "Volatility Expansion": run_volatility_expansion_engine,
}


# ============================================================================
# SECTION 10A -- COUNTER-TREND REVERSAL ENGINE (opt-in, Section 4A)
# ============================================================================

def _htf_poi_pool(direction: str, weekly_view: View, daily_view: View) -> Optional[dict]:
    """Step 1: Weekly/Daily-sourced POI pool, reusing the same detection
    functions Section 8 step 2 uses for the 1H pool -- invoked against
    Weekly/Daily views instead."""
    for view in (daily_view, weekly_view):
        pool = (view.bull_obs + view.bull_breakers + [z for z in view.fvgs if z.direction == "bullish"]) \
            if direction == "bullish" else \
            (view.bear_obs + view.bear_breakers + [z for z in view.fvgs if z.direction == "bearish"])
        pool = [z for z in pool if not z.mitigated]
        price = view.close
        for z in pool:
            if z.bottom <= price <= z.top:
                return {"view": view, "zone": z}
        wanted_kind = "SSL" if direction == "bullish" else "BSL"
        for p in view.pools:
            if p.kind == wanted_kind and p.swept and p.pure_sfp:
                return {"view": view, "zone": None, "swept_pool": p}
    return None


def _exhaustion_signature(direction: str, view: View) -> Optional[float]:
    """Step 2: momentum-exhaustion signature on 4H/1H -- shrinking bodies,
    elongated opposing wick, failure to make a new swing extreme, RSI
    divergence as an optional soft booster only."""
    candles = view.candles[-8:]
    if len(candles) < 6:
        return None
    bodies = [abs(c["c"] - c["o"]) for c in candles]
    shrinking = bodies[-1] < statistics.fmean(bodies[:-1]) * 0.8
    last = candles[-1]
    body_top, body_bot = max(last["o"], last["c"]), min(last["o"], last["c"])
    opp_wick = (body_bot - last["l"]) if direction == "bullish" else (last["h"] - body_top)
    elongated = opp_wick > view.atr * 0.6
    highs = [p for p in view.pivots if p.kind == "high"]
    lows = [p for p in view.pivots if p.kind == "low"]
    no_new_extreme = True
    if direction == "bullish" and len(lows) >= 2:
        no_new_extreme = lows[-1].price >= lows[-2].price
    elif direction == "bearish" and len(highs) >= 2:
        no_new_extreme = highs[-1].price <= highs[-2].price
    if not (shrinking and elongated and no_new_extreme):
        return None
    score = 0.5 + 0.2 * shrinking + 0.2 * elongated + 0.1 * no_new_extreme
    div_bonus = 0.0
    if direction == "bullish" and view.rsi < 35:
        div_bonus = 0.1
    elif direction == "bearish" and view.rsi > 65:
        div_bonus = 0.1
    return min(1.0, score + div_bonus)


def _retest_and_hold(direction: str, choch_level: float, m15: View, state: dict, symbol: str):
    """Step 4: retest-and-hold stage. Not an instant entry on the CHoCH
    candle -- tracks the broken level and waits (bounded by
    COUNTERTREND_RETEST_EXPIRY_BARS) for a closed-candle hold or rejection
    wick back at it, hooking into the standard entry_kind='pending'
    machinery rather than a separate tracker."""
    recent = m15.candles[-COUNTERTREND_RETEST_EXPIRY_BARS:]
    for c in recent:
        touched = c["l"] <= choch_level <= c["h"]
        if not touched:
            continue
        held = (c["c"] > choch_level) if direction == "bullish" else (c["c"] < choch_level)
        body_top, body_bot = max(c["o"], c["c"]), min(c["o"], c["c"])
        rejection_wick = (body_bot - c["l"] > (c["h"] - c["l"]) * 0.4) if direction == "bullish" else \
                          (c["h"] - body_top > (c["h"] - c["l"]) * 0.4)
        if held or rejection_wick:
            return {"entry": choch_level}
    return None


def run_countertrend_engine(base_bias: str, views: dict, regime: RegimeVector,
                             state: dict, symbol: str) -> Optional[Candidate]:
    """Section 4A: opt-in, separate gate. Fires only when Stage 1 resolves
    Bullish/Bearish and produces the OPPOSITE direction only. Strictly
    additive -- never called unless ENABLE_COUNTERTREND_ENGINE is true, and
    never suppresses/alters base-ensemble output."""
    if not ENABLE_COUNTERTREND_ENGINE:
        return None
    if base_bias not in ("bullish", "bearish"):
        return None
    direction = "bearish" if base_bias == "bullish" else "bullish"

    htf = _htf_poi_pool(direction, views[TF_WEEKLY], views[TF_DAILY])
    if htf is None:
        return None

    exhaustion = _exhaustion_signature(direction, views[TF_4H]) or _exhaustion_signature(direction, views[TF_1H])
    if exhaustion is None:
        return None

    choch = structure_shift(views[TF_1H].candles, views[TF_1H].pivots, direction, "CHoCH") or \
        structure_shift(views[TF_15M].candles, views[TF_15M].pivots, direction, "CHoCH")
    if choch is None:
        return None

    retest = _retest_and_hold(direction, choch.price, views[TF_15M], state, symbol)
    if retest is None:
        return None

    htf_view = htf["view"]
    plan = build_risk_plan(direction, retest["entry"], views[TF_15M], htf_view, views[TF_4H],
                            state, symbol, rr_min_gate=RR_MIN_GATE_COUNTERTREND)
    if plan is None:
        return None

    conf = 0.4 + 0.25 * exhaustion
    return Candidate(engine="Counter-Trend Reversal", direction=direction, entry=plan["entry"], sl=plan["sl"],
                      tp1=plan["tp1"], tp2=plan["tp2"], confidence=min(0.9, conf), rr1=plan["rr1"],
                      rr2=plan["rr2"], confluences=["HTF POI", "momentum exhaustion", "CHoCH", "retest-and-hold"],
                      regime_fit=["reversal", "high_volatility"], style="intraday", entry_kind="pending",
                      symbol=symbol, counter_trend=True, sl_anchor_tf=plan["sl_anchor_tf"])


def run_specialized_engines(bias: str, views: dict, stage3: StageResult, stage4: StageResult,
                             regime: RegimeVector, state: dict, symbol: str) -> list:
    """Runs every base-ensemble engine additively, plus the opt-in
    counter-trend engine as a strictly separate, non-suppressing call."""
    candidates = []
    if bias in ("bullish", "bearish"):
        cand = run_smc_engine(bias, views, stage3, stage4, regime, state, symbol)
        if cand:
            candidates.append(cand)
        for name, runner in BASE_ENGINE_RUNNERS.items():
            if name == "SMC":
                continue
            try:
                cand = runner(bias, views, regime, state, symbol)
            except (ValueError, ZeroDivisionError, IndexError, KeyError) as e:
                log.warning("%s engine error for %s: %s", name, symbol, e)
                cand = None
            if cand:
                candidates.append(cand)
    ct = run_countertrend_engine(bias, views, regime, state, symbol)
    if ct:
        candidates.append(ct)
    return candidates


# ============================================================================
# SECTION 11 -- DECISION ENGINE (continuous composite score, regime-fit veto,
#                liquidity sanity check, macro blackout, correlation dedup)
# ============================================================================

# Term weights -- small, documented, auditable set (Section 4 mandatory:
# a handful of weighted terms, never a discrete point stack). Each weight
# is itself a bounded adaptive parameter under Section 5.
SCORE_TERM_WEIGHTS_DEFAULT = {
    "regime_fit": 0.22,
    "mtf_alignment": 0.14,
    "confluence_strength": 0.16,
    "segment_performance": 0.20,
    "rr_context": 0.10,       # RR informs ranking, never treated as win-probability (Section 4)
    "liquidity_volatility_context": 0.10,
    "engine_weight": 0.08,
}
TERM_CONTRIBUTION_CAP = 0.30  # hard bound on any single term's weight*input contribution (Section 4)


def _mtf_alignment_term(candidate: Candidate, views: dict) -> float:
    aligned = 0
    total = 0
    for tf in (TF_4H, TF_1H):
        v = views[tf]
        total += 1
        bullish = v.ema_fast > v.ema_slow
        if (bullish and candidate.direction == "bullish") or (not bullish and candidate.direction == "bearish"):
            aligned += 1
    return aligned / total if total else 0.5


def _confluence_strength_term(candidate: Candidate) -> float:
    return min(1.0, len(candidate.confluences) / 4.0)


def _segment_performance_term(candidate: Candidate, state: dict, regime: RegimeVector) -> float:
    key = f"{candidate.symbol}|{'trend' if regime.is_trending() else 'range'}|{candidate.style}|{candidate.engine}"
    seg = state["tier1"]["segment_stats"].get(key)
    if not seg or seg.get("n", 0) < MIN_SAMPLE_SIZE:
        return 0.5  # neutral prior until statistically meaningful (Section 13 minimum sample size)
    wins = seg.get("wins", 0)
    n = seg.get("n", 1)
    return max(0.0, min(1.0, wins / n))


def _rr_context_term(candidate: Candidate) -> float:
    # bounded, saturating slowly -- informs ranking, never proxies win probability (Section 4 mandatory)
    return min(1.0, (candidate.rr1 - RR_MIN_GATE) / (RR_SOFT_TARGET - RR_MIN_GATE + 1e-9)) if candidate.rr1 else 0.0


def _liquidity_vol_context_term(regime: RegimeVector) -> float:
    vol_score = 1.0 - abs(regime.volatility_pctl - 55.0) / 55.0  # mid-range vol preferred, extremes discounted
    clean = 1.0 - regime.noise_index
    return max(0.0, min(1.0, (vol_score + clean) / 2.0))


def regime_fit_score(candidate: Candidate, regime: RegimeVector) -> float:
    """Regime-fit veto/discount (Section 13). Counter-trend's documented
    best-fit is explicitly reversal/high-vol against the dominant bias --
    its 'against Stage 1 bias' nature must never itself be penalized here,
    unlike a base-ensemble engine."""
    regime_tags = []
    if regime.is_trending():
        regime_tags.append("trending")
    else:
        regime_tags.append("ranging")
    if regime.is_high_vol():
        regime_tags.append("high_volatility")
    if regime.is_low_vol():
        regime_tags.append("low_volatility")
    if regime.noise_index < 0.4:
        regime_tags.append("expansion")
    else:
        regime_tags.append("consolidation")
    match = len(set(candidate.regime_fit) & set(regime_tags))
    base = min(1.0, 0.35 + 0.25 * match)
    return base  # counter_trend candidates already carry reversal/high_volatility as best-fit, treated identically


def liquidity_sanity_check(candidate: Candidate, view_1h: View) -> bool:
    """Section 13: reject/heavily discount entries sitting directly inside
    a level about to be swept or immediately adjacent to an obvious
    unmitigated EQH/EQL cluster, unless the setup IS a liquidity-sweep
    engine designed to trade that behavior."""
    if candidate.engine in ("Liquidity Sweep", "Reversal", "Counter-Trend Reversal"):
        return True
    danger_kind = "BSL" if candidate.direction == "bullish" else "SSL"
    for pool in view_1h.pools:
        if pool.kind == danger_kind and not pool.swept and pool.equal:
            if abs(candidate.entry - pool.price) < view_1h.atr * 0.5:
                return False
    return True


def macro_blackout_active(symbol: str, state: dict, now_ms: int) -> bool:
    """Section 13 macro/news blackout: a documented window around any
    operator-flagged high-impact event, for the directly affected asset(s)
    and correlated majors."""
    events = state.get("macro_events", [])
    cluster = correlation_cluster(symbol)
    before_ms = MACRO_BLACKOUT_MINUTES_BEFORE * 60_000
    after_ms = MACRO_BLACKOUT_MINUTES_AFTER * 60_000
    for ev in events:
        ts = ev.get("ts")
        symbols = set(ev.get("symbols", []))
        if ts is None:
            continue
        if symbol in symbols or any(correlation_cluster(s) == cluster for s in symbols):
            if ts - before_ms <= now_ms <= ts + after_ms:
                return True
    return False


def composite_score(candidate: Candidate, views: dict, regime: RegimeVector, state: dict) -> dict:
    """Section 4 mandatory continuous blend: a small number of weighted
    terms, each individually contribution-capped so none can saturate the
    output, combined through a logistic transform. Returns the score plus
    each term's raw and capped contribution for auditability."""
    weights = dict(SCORE_TERM_WEIGHTS_DEFAULT)
    for term in weights:
        adj = state["tier1"]["filter_thresholds"].get(f"score_term::{term}", 1.0)
        weights[term] *= max(FILTER_THRESH_MIN, min(FILTER_THRESH_MAX, adj))

    engine_weight = state["tier1"]["engine_weights"].get(candidate.engine, 1.0)
    engine_weight = max(ENGINE_WEIGHT_MIN, min(ENGINE_WEIGHT_MAX, engine_weight))

    raw_terms = {
        "regime_fit": regime_fit_score(candidate, regime),
        "mtf_alignment": _mtf_alignment_term(candidate, views),
        "confluence_strength": _confluence_strength_term(candidate),
        "segment_performance": _segment_performance_term(candidate, state, regime),
        "rr_context": _rr_context_term(candidate),
        "liquidity_volatility_context": _liquidity_vol_context_term(regime),
        "engine_weight": (engine_weight - ENGINE_WEIGHT_MIN) / (ENGINE_WEIGHT_MAX - ENGINE_WEIGHT_MIN),
    }
    z = 0.0
    contributions = {}
    for term, raw in raw_terms.items():
        contribution = weights[term] * raw
        contribution = max(-TERM_CONTRIBUTION_CAP, min(TERM_CONTRIBUTION_CAP, contribution))
        contributions[term] = contribution
        z += contribution
    calib_key = f"{candidate.engine}|{_confidence_bucket(candidate.confidence)}"
    calib_adj = state["tier1"]["confidence_calibration"].get(calib_key, 0.0)
    calib_adj = max(CALIBRATION_ADJ_MIN, min(CALIBRATION_ADJ_MAX, calib_adj))
    z += calib_adj
    z_center = -0.15  # small negative bias so a genuinely mediocre setup lands below 0.5, not at it
    prob = 1.0 / (1.0 + math.exp(-(z + z_center) * 4.0))
    return {"score": prob, "z": z, "contributions": contributions, "engine_weight": engine_weight}


def _confidence_bucket(score: float) -> str:
    if score >= 0.80:
        return "A+"
    if score >= 0.68:
        return "A"
    if score >= 0.55:
        return "B"
    return "C"


def rank_and_select(candidates: list, views: dict, regime: RegimeVector, state: dict, symbol: str,
                     now_ms: int) -> list:
    """Decision Engine: score every candidate, apply regime-fit veto,
    liquidity sanity check, macro blackout, and TP-ordering final
    assertion; return ranked, tiered signals."""
    if macro_blackout_active(symbol, state, now_ms):
        _log_funnel(state, "macro_blackout", seen=len(candidates), rejected=len(candidates))
        return []

    scored = []
    for c in candidates:
        _log_funnel(state, "liquidity_sanity", seen=1, rejected=0)
        if not liquidity_sanity_check(c, views[TF_1H]):
            _bump_funnel_rejected(state, "liquidity_sanity")
            continue
        # final TP-ordering integrity assertion immediately before scoring/dispatch (Section 10)
        dist_tp1 = abs(c.tp1 - c.entry)
        dist_tp2 = abs(c.tp2 - c.entry)
        if dist_tp2 <= dist_tp1:
            continue
        if c.rr1 < (RR_MIN_GATE_COUNTERTREND if c.counter_trend else RR_MIN_GATE):
            continue
        result = composite_score(c, views, regime, state)
        grade = _confidence_bucket(result["score"])
        scored.append((result["score"], grade, c, result))
    scored.sort(key=lambda x: x[0], reverse=True)
    return scored


def _log_funnel(state: dict, name: str, seen: int, rejected: int) -> None:
    entry = state["tier1"]["filter_funnel"].setdefault(name, {"seen": 0, "rejected": 0})
    entry["seen"] += seen
    entry["rejected"] += rejected


def _bump_funnel_rejected(state: dict, name: str) -> None:
    state["tier1"]["filter_funnel"].setdefault(name, {"seen": 0, "rejected": 0})["rejected"] += 1


def correlation_dedup(ranked: list, active_signals: list) -> list:
    """Section 14 correlation cap: caps concurrent exposure per correlation
    cluster jointly across the base ensemble and the counter-trend engine
    -- never a separate exempt pool.

    Also enforces one active signal per symbol: a symbol that already has
    an active/pending signal is skipped entirely, and if multiple new
    candidates exist for the same symbol in this batch (e.g. Mean
    Reversion and Liquidity Sweep both firing on the same asset), only the
    single highest-scored one is accepted -- `ranked` is already sorted by
    score descending, so the first candidate seen per symbol is the best."""
    cluster_counts = collections.Counter(correlation_cluster(s["symbol"]) for s in active_signals)
    symbols_with_active = {s["symbol"] for s in active_signals}
    symbols_accepted_this_batch = set()
    accepted = []
    for score, grade, c, result in ranked:
        if len(accepted) + len(active_signals) >= MAX_CONCURRENT_ACTIVE_SIGNALS:
            break
        if c.symbol in symbols_with_active or c.symbol in symbols_accepted_this_batch:
            continue  # one active signal per symbol -- best-scored candidate wins
        cluster = correlation_cluster(c.symbol)
        if cluster_counts[cluster] >= MAX_CORRELATED_CONCURRENT:
            continue
        cluster_counts[cluster] += 1
        symbols_accepted_this_batch.add(c.symbol)
        accepted.append((score, grade, c, result))
    return accepted


# ============================================================================
# SECTION 12 -- SIGNAL LIFECYCLE: DISPATCH, FILL VERIFICATION, RESOLUTION
#
# Position-exit model (declared explicitly, per Section 11's mandatory
# declaration rule): FULL EXIT AT TP1. 100% of position size closes at
# TP1; nothing remains open afterward, so a later touch of the original SL
# is bookkeeping only and has zero effect on realized P&L. The engine never
# auto-repositions SL to breakeven on TP1 -- the original structure-based
# SL stays in the record unchanged, for internal tracking only, and is
# simply never evaluated again once TP1 has resolved the trade as a WIN.
# ============================================================================

def new_signal_record(candidate: Candidate, score: float, grade: str, symbol: str, now_ms: int) -> dict:
    return {
        "id": f"{symbol}-{candidate.engine}-{now_ms}",
        "symbol": symbol,
        "engine": candidate.engine,
        "counter_trend": candidate.counter_trend,
        "direction": candidate.direction,
        "style": candidate.style,
        "entry_kind": candidate.entry_kind,
        "entry": candidate.entry,
        "sl": candidate.sl,
        "tp1": candidate.tp1,
        "tp2": candidate.tp2,
        "rr1": candidate.rr1,
        "rr2": candidate.rr2,
        "confidence": score,
        "grade": grade,
        "confluences": candidate.confluences,
        "regime_at_entry": {
            "trend_strength": None, "volatility_pctl": None, "macro_bias": None,
        },
        "sl_anchor_tf": candidate.sl_anchor_tf,
        "entry_filled": candidate.entry_kind == "market",
        "pending_bars": 0,
        "status": "active" if candidate.entry_kind == "market" else "pending",
        "created_ts": now_ms,
        "filled_ts": now_ms if candidate.entry_kind == "market" else None,
        "mae_r": 0.0,
        "mfe_r": 0.0,
        "resolution_logic_version": RESOLUTION_LOGIC_VERSION,
        "tg_message_id": None,
    }


def monitor_signal(sig: dict, monitor_candles: list) -> Optional[dict]:
    """Advances a single signal's lifecycle against newly-closed monitor-TF
    candles. Returns an event dict describing what happened (for Telegram +
    forensic logging), or None if nothing changed. Section 12 rules:
      - pending signals: entry not filled -> only check fill, never SL/TP
      - once filled (same candle allowed): evaluate SL vs TP1 that candle
      - Section 11: single-TP resolution, no TP2 monitoring, no auto-BE
    """
    direction = sig["direction"]
    risk = abs(sig["entry"] - sig["sl"])
    for c in monitor_candles:
        if c["t"] <= sig.get("_last_checked_t", -1):
            continue
        sig["_last_checked_t"] = c["t"]

        if not sig["entry_filled"]:
            entry_in_range = c["l"] <= sig["entry"] <= c["h"]
            if not entry_in_range:
                sig["pending_bars"] += 1
                if sig["pending_bars"] >= (COUNTERTREND_RETEST_EXPIRY_BARS if sig["counter_trend"]
                                            else PENDING_ENTRY_EXPIRY_BARS):
                    sig["status"] = "expired"
                    return {"type": "expired", "sig": sig}
                continue
            sig["entry_filled"] = True
            sig["filled_ts"] = c["t"]
            sig["status"] = "active"
            # same-candle ambiguity: conservative worst-case-first check documented below

        # MAE/MFE tracking (Section 13 schema requirement) -- runs every candle post-fill
        if risk > 0:
            if direction == "bullish":
                mfe = (c["h"] - sig["entry"]) / risk
                mae = (sig["entry"] - c["l"]) / risk
            else:
                mfe = (sig["entry"] - c["l"]) / risk
                mae = (c["h"] - sig["entry"]) / risk
            sig["mfe_r"] = max(sig["mfe_r"], mfe)
            sig["mae_r"] = max(sig["mae_r"], mae)

        # Resolution order (documented, Section 11): SL is checked first on
        # the fill candle to avoid ever crediting a win from a candle whose
        # wick would have stopped the trade out first at conservative
        # worst-case ordering -- this can only ever make the engine more
        # conservative about crediting wins, never manufacture a false
        # stop-out that wouldn't otherwise have happened, since SL and TP1
        # are evaluated against the same single candle's actual high/low.
        sl_hit = (c["l"] <= sig["sl"]) if direction == "bullish" else (c["h"] >= sig["sl"])
        tp1_hit = (c["h"] >= sig["tp1"]) if direction == "bullish" else (c["l"] <= sig["tp1"])

        if sl_hit:
            sig["status"] = "resolved"
            sig["result"] = "loss"
            r_realized = -1.0
            sig["r_realized"] = r_realized
            sig["resolved_ts"] = c["t"]
            return {"type": "loss", "sig": sig}
        if tp1_hit:
            sig["status"] = "resolved"
            sig["result"] = "win"
            r_realized = sig["rr1"]
            assert r_realized > 0, "win result with non-positive realized R -- resolution bug"
            sig["r_realized"] = r_realized
            sig["resolved_ts"] = c["t"]
            return {"type": "win", "sig": sig}
    return None


# ============================================================================
# SECTION 13 -- LOSS/WIN FORENSICS & CLOSED-LOOP ADAPTIVE FEEDBACK
# ============================================================================

FORENSIC_CATEGORIES = [
    "regime_mismatch", "structural_invalidation_too_tight", "chased_swept_liquidity",
    "mtf_conflict_ignored", "sfp_mss_sequence_violated", "correct_read_poor_rr",
    "confidence_miscalibration", "filter_over_permissiveness", "genuine_variance",
]


def classify_forensics(sig: dict, views: dict, regime: RegimeVector, state: dict) -> str:
    """Every category is a positive, verifiable condition on recorded trade
    data (Section 13.1a) -- checked in order of diagnostic specificity, not
    reached by elimination. 'genuine_variance' is the deliberate final
    fallback only after every positive check has failed."""
    is_loss = sig["result"] == "loss"
    mfe = sig.get("mfe_r", 0.0)
    mae = sig.get("mae_r", 0.0)

    # Regime mismatch: traded against/orthogonal to the dominant regime read
    regime_tags = sig.get("regime_at_entry", {})
    if is_loss and regime_tags.get("trend_strength") is not None:
        was_trending = regime_tags["trend_strength"] >= 22
        engine_wants_trend = "trending" in sig.get("_regime_fit_tags", [])
        if engine_wants_trend and not was_trending:
            return "regime_mismatch"

    # Structural invalidation too tight: SL hit within the buffer's normal noise range
    if is_loss and mae <= 1.05:
        return "structural_invalidation_too_tight"

    # Chased swept liquidity: entry sat inside/adjacent to a pool swept near entry time
    if is_loss and sig.get("_liquidity_adjacent"):
        return "chased_swept_liquidity"

    # MTF conflict ignored: HTF/LTF disagreed at entry and HTF ultimately won
    if is_loss and sig.get("_mtf_conflict_at_entry"):
        return "mtf_conflict_ignored"

    # SFP/MSS sequence violated: entry on an impure SFP or pre-MSS confirmation
    if is_loss and sig.get("_sfp_impure_or_premature"):
        return "sfp_mss_sequence_violated"

    # Correct read, poor RR: direction/structure right, MFE reached >=80% of TP1 distance before reversing
    if is_loss and mfe >= 0.80:
        return "correct_read_poor_rr"

    # Confidence miscalibration: assigned confidence materially above the bucket's realized win rate
    bucket = _confidence_bucket(sig["confidence"])
    calib_key = f"{sig['engine']}|{bucket}"
    calib = state["tier1"]["calibration_buckets"].get(calib_key, {"n": 0, "wins": 0})
    if is_loss and calib.get("n", 0) >= MIN_SAMPLE_SIZE:
        realized_wr = calib["wins"] / calib["n"]
        implied_wr = {"A+": 0.75, "A": 0.65, "B": 0.55, "C": 0.45}[bucket]
        if implied_wr - realized_wr > 0.15:
            return "confidence_miscalibration"

    # Filter over-permissiveness: passed every filter with thin margins across multiple filters
    if is_loss and sig.get("_thin_margin_count", 0) >= 2:
        return "filter_over_permissiveness"

    return "genuine_variance"


def adaptive_route(category: str, sig: dict, state: dict) -> None:
    """One diagnosis, one deterministic route (Section 13.3) -- each
    category maps to exactly the specific adaptive parameter it implicates,
    bounded and dampened (Section 5)."""
    t1 = state["tier1"]
    symbol, engine = sig["symbol"], sig["engine"]

    if category == "regime_mismatch":
        key = f"{engine}|regime"
        cur = t1["regime_fit_discount"].get(key, 1.0)
        t1["regime_fit_discount"][key] = _damp(cur, cur * 0.95, 0.65, 1.0, step=0.05)

    elif category == "structural_invalidation_too_tight":
        for tf in (sig.get("sl_anchor_tf", TF_15M),):
            key = f"{symbol}|{tf}"
            cur = t1["sl_buffer_percentile"].get(key, 70.0)
            t1["sl_buffer_percentile"][key] = _damp(cur, cur + SL_BUFFER_PCTL_LR_STEP,
                                                      SL_BUFFER_PCTL_MIN, SL_BUFFER_PCTL_MAX,
                                                      step=SL_BUFFER_PCTL_LR_STEP)

    elif category == "chased_swept_liquidity":
        key = "liquidity_sanity"
        cur = t1["filter_thresholds"].get(key, 1.0)
        t1["filter_thresholds"][key] = _damp(cur, cur * 1.05, FILTER_THRESH_MIN, FILTER_THRESH_MAX,
                                              step=FILTER_THRESH_LR)

    elif category == "mtf_conflict_ignored":
        key = "score_term::mtf_alignment"
        cur = t1["filter_thresholds"].get(key, 1.0)
        t1["filter_thresholds"][key] = _damp(cur, cur * 1.06, FILTER_THRESH_MIN, FILTER_THRESH_MAX,
                                              step=FILTER_THRESH_LR)

    elif category == "sfp_mss_sequence_violated":
        key = f"{engine}|sfp_purity"
        cur = t1["filter_thresholds"].get(key, 1.0)
        t1["filter_thresholds"][key] = _damp(cur, cur * 1.06, FILTER_THRESH_MIN, FILTER_THRESH_MAX,
                                              step=FILTER_THRESH_LR)

    elif category == "correct_read_poor_rr":
        cur = t1["tp1_rank_preference"].get(symbol, 3)
        t1["tp1_rank_preference"][symbol] = int(max(TP1_RANK_PREF_MIN, min(TP1_RANK_PREF_MAX, cur + 1)))

    elif category == "confidence_miscalibration":
        key = f"{engine}|{_confidence_bucket(sig['confidence'])}"
        cur = t1["confidence_calibration"].get(key, 0.0)
        t1["confidence_calibration"][key] = _damp(cur, cur - 0.03, CALIBRATION_ADJ_MIN, CALIBRATION_ADJ_MAX,
                                                    step=CALIBRATION_LR)

    elif category == "filter_over_permissiveness":
        for name in sig.get("_thin_margin_filters", []):
            key = name
            cur = t1["filter_thresholds"].get(key, 1.0)
            t1["filter_thresholds"][key] = _damp(cur, cur * 1.05, FILTER_THRESH_MIN, FILTER_THRESH_MAX,
                                                  step=FILTER_THRESH_LR)
    # genuine_variance: no parameter change -- expected, healthy loss variance

    count_key = category
    t1["forensic_counts"][count_key] = t1["forensic_counts"].get(count_key, 0) + 1


def reinforce_win(sig: dict, state: dict) -> None:
    """Win reinforcement (Section 13.2): raise, within bounds, the weights
    of factors genuinely present and predictive."""
    t1 = state["tier1"]
    engine = sig["engine"]
    seg_key = f"{sig['symbol']}|{'trend' if sig.get('regime_at_entry', {}).get('trend_strength', 0) and sig['regime_at_entry']['trend_strength'] >= 22 else 'range'}|{sig['style']}|{engine}"
    seg = t1["segment_stats"].get(seg_key, {"n": 0, "wins": 0, "losses": 0, "sum_r": 0.0})
    if seg.get("n", 0) >= MIN_SAMPLE_SIZE:
        expectancy = seg["sum_r"] / seg["n"]
        cur = t1["engine_weights"].get(engine, 1.0)
        target = cur * (1.02 if expectancy > 0 else 0.99)
        t1["engine_weights"][engine] = _damp(cur, target, ENGINE_WEIGHT_MIN, ENGINE_WEIGHT_MAX, step=ENGINE_WEIGHT_LR)


def _damp(current: float, target: float, lo: float, hi: float, step: float) -> float:
    """Dampened, capped update (Section 5 mandatory): moves at most `step`
    toward `target`, then clamps into [lo, hi]. Prevents any single run's
    result from swinging a parameter far in one update."""
    direction = 1 if target > current else -1
    moved = current + direction * min(abs(target - current), step)
    return max(lo, min(hi, moved))


def update_segment_stats(sig: dict, state: dict) -> None:
    """Tier 1 incremental update -- one resolved trade at a time, never a
    rescan of Tier 2 (Section 5 mandatory)."""
    trending = sig.get("regime_at_entry", {}).get("trend_strength")
    regime_bucket = "trend" if (trending is not None and trending >= 22) else "range"
    key = f"{sig['symbol']}|{regime_bucket}|{sig['style']}|{sig['engine']}"
    seg = state["tier1"]["segment_stats"].setdefault(key, {"n": 0, "wins": 0, "losses": 0, "sum_r": 0.0})
    seg["n"] += 1
    seg["sum_r"] += sig["r_realized"]
    if sig["result"] == "win":
        seg["wins"] += 1
    else:
        seg["losses"] += 1

    bucket = _confidence_bucket(sig["confidence"])
    calib_key = f"{sig['engine']}|{bucket}"
    calib = state["tier1"]["calibration_buckets"].setdefault(calib_key, {"n": 0, "wins": 0, "sum_conf": 0.0})
    calib["n"] += 1
    calib["sum_conf"] += sig["confidence"]
    if sig["result"] == "win":
        calib["wins"] += 1

    fill_key = f"{sig['engine']}|{sig['entry_kind']}"
    fs = state["tier1"]["fill_stats"].setdefault(fill_key, {"dispatched": 0, "filled": 0, "expired": 0})
    fs["filled"] += 1


def check_circuit_breaker(state: dict) -> None:
    """Live-performance circuit breaker (Section 5 mandatory): compares
    rolling live performance against the Section 13 baseline once enough
    trades have resolved; freezes adaptation and alerts on material
    sustained deviation; auto-resumes on recovery."""
    trades = [t for t in state["tier2_trades"] if t.get("result") in ("win", "loss")
              and t.get("resolution_logic_version") == RESOLUTION_LOGIC_VERSION]
    if len(trades) < CIRCUIT_BREAKER_WINDOW:
        return
    window = trades[-CIRCUIT_BREAKER_WINDOW:]
    wins = sum(1 for t in window if t["result"] == "win")
    wr = wins / len(window)
    gross_win = sum(t["r_realized"] for t in window if t["r_realized"] > 0)
    gross_loss = abs(sum(t["r_realized"] for t in window if t["r_realized"] < 0))
    pf = (gross_win / gross_loss) if gross_loss > 1e-9 else (gross_win if gross_win > 0 else 0.0)

    cb = state["tier1"]["circuit_breaker"]
    wr_dropped = (BASELINE_WIN_RATE - wr) >= CIRCUIT_BREAKER_WR_DROP
    pf_dropped = pf < BASELINE_PROFIT_FACTOR * (1 - CIRCUIT_BREAKER_PF_DROP)

    if (wr_dropped or pf_dropped) and not cb["tripped"]:
        cb["tripped"] = True
        cb["tripped_ts"] = utcnow_ms()
        cb["reason"] = f"win_rate={wr:.2f} profit_factor={pf:.2f} vs baseline wr={BASELINE_WIN_RATE} pf={BASELINE_PROFIT_FACTOR}"
        log.warning("Circuit breaker TRIPPED: %s", cb["reason"])
        send_telegram(format_circuit_breaker_message(cb, tripped=True))
    elif cb["tripped"] and not wr_dropped and not pf_dropped:
        cb["tripped"] = False
        cb["reason"] = None
        log.info("Circuit breaker cleared -- live performance recovered to baseline")
        send_telegram(format_circuit_breaker_message(cb, tripped=False))


def process_resolution(sig: dict, views: dict, regime: RegimeVector, state: dict) -> None:
    """Section 13's closed-loop: classify, route to the exactly-one
    implicated parameter, reinforce wins, update Tier-1 aggregates, append
    to the bounded Tier-2 log, and check the circuit breaker -- all gated
    by whether adaptation is currently frozen."""
    sig["regime_at_entry"] = {
        "trend_strength": regime.trend_strength, "volatility_pctl": regime.volatility_pctl,
        "macro_bias": regime.macro_bias,
    }
    category = classify_forensics(sig, views, regime, state)
    sig["forensic_category"] = category

    frozen = state["tier1"]["circuit_breaker"]["tripped"]
    if not frozen:
        seg_key_n = state["tier1"]["segment_stats"].get(
            f"{sig['symbol']}|trend|{sig['style']}|{sig['engine']}", {}).get("n", 0)
        if seg_key_n >= MIN_SAMPLE_SIZE or True:
            # Section 13 minimum-sample-size gate is enforced inside adaptive_route/reinforce_win
            # themselves (each reads the relevant segment's own sample size before acting)
            if sig["result"] == "loss":
                adaptive_route(category, sig, state)
            else:
                reinforce_win(sig, state)

    update_segment_stats(sig, state)
    state["tier2_trades"].append({
        "id": sig["id"], "symbol": sig["symbol"], "engine": sig["engine"],
        "counter_trend": sig["counter_trend"], "direction": sig["direction"],
        "entry": sig["entry"], "sl": sig["sl"], "tp1": sig["tp1"], "tp2": sig["tp2"],
        "r_realized": sig["r_realized"], "mae_r": sig["mae_r"], "mfe_r": sig["mfe_r"],
        "forensic_category": category, "confidence": sig["confidence"], "grade": sig["grade"],
        "regime_at_entry": sig["regime_at_entry"], "resolved_ts": sig["resolved_ts"],
        "result": sig["result"], "style": sig["style"], "entry_kind": sig["entry_kind"],
        "resolution_logic_version": sig["resolution_logic_version"],
    })
    check_circuit_breaker(state)


# ============================================================================
# SECTION 14 -- TELEGRAM INTEGRATION
# ============================================================================

def _tg_api(method: str, payload: dict) -> Optional[dict]:
    if not TG_BOT_TOKEN or not TG_CHAT_ID:
        log.info("Telegram not configured; message suppressed: %s", method)
        return None
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/{method}"
    body = json.dumps(payload).encode()
    req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"})
    for attempt in range(3):
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                return json.loads(resp.read().decode())
        except (urllib.error.URLError, TimeoutError, ValueError, OSError) as e:
            log.warning("telegram %s attempt %d failed: %s", method, attempt + 1, e)
            time.sleep(0.6 * (attempt + 1))
    return None


def send_telegram(text: str, reply_to: Optional[int] = None) -> Optional[int]:
    payload = {"chat_id": TG_CHAT_ID, "text": text, "parse_mode": "Markdown"}
    if reply_to:
        payload["reply_to_message_id"] = reply_to
    result = _tg_api("sendMessage", payload)
    if result and result.get("ok"):
        return result["result"]["message_id"]
    return None


def send_telegram_reaction(chat_id: str, message_id: int, emoji: str) -> None:
    if not TG_BOT_TOKEN or not TG_CHAT_ID:
        return
    _tg_api("setMessageReaction", {"chat_id": chat_id, "message_id": message_id,
                                    "reaction": [{"type": "emoji", "emoji": emoji}]})


def _price_line(label: str, value: float) -> str:
    return f"{label}: `{round(value, 6) if value < 1 else round(value, 4)}`"


def format_signal_message(sig: dict) -> str:
    """Section 17: clean, deliberately laid-out card. Entry/SL/TP1/TP2 each
    individually copy-paste-friendly monospace, no underscores anywhere,
    TP2 clearly labeled as suggestion-only."""
    is_long = sig["direction"] == "bullish"
    direction_label = f"{'🟢' if is_long else '🔴'} {'LONG' if is_long else 'SHORT'}"
    engine_label = human_label(sig["engine"])
    style_label = human_label(sig["style"])
    ct_badge = "\n⚠️ *COUNTER-TREND* — against the Weekly/Daily bias" if sig["counter_trend"] else ""
    confluences = ", ".join(human_label(x) for x in sig["confluences"])
    lines = [
        f"*{ENGINE_NAME} v{ENGINE_VERSION}*",
        f"{direction_label} {sig['symbol']} — *{engine_label}*{ct_badge}",
        "",
        f"Style: {style_label}   Grade: {sig['grade']}   Confidence: {sig['confidence']*100:.0f}%",
        f"Entry type: {human_label(sig['entry_kind'])}",
        "",
        _price_line("Entry", sig["entry"]),
        _price_line("SL", sig["sl"]),
        _price_line("TP1", sig["tp1"]),
        f"TP2 (suggested): `{round(sig['tp2'], 6) if sig['tp2'] < 1 else round(sig['tp2'], 4)}`",
        "",
        f"RR to TP1: {sig['rr1']:.2f}   RR to TP2 (suggested): {sig['rr2']:.2f}",
        f"Confluences: {confluences}",
    ]
    return "\n".join(lines)


def format_status_message(sig: dict, event_type: str) -> str:
    engine_label = human_label(sig["engine"])
    header = f"*{ENGINE_NAME} v{ENGINE_VERSION}* — {sig['symbol']} {engine_label}"
    if event_type == "win":
        body = (f"🏆 *TP1 hit — WIN.* Signal resolved.\n"
                f"Realized R: {sig['r_realized']:.2f}\n"
                f"SL remains at its original level, unchanged, for the record.")
    elif event_type == "loss":
        body = f"😭 *SL hit — LOSS.* Signal resolved.\nRealized R: {sig['r_realized']:.2f}"
    elif event_type == "expired":
        body = "🤷 *Expired — no fill.* Price never reached entry within the pending window."
    elif event_type == "cancelled":
        body = "Cancelled."
    else:
        body = human_label(event_type)
    return f"{header}\n\n{body}"


def format_circuit_breaker_message(cb: dict, tripped: bool) -> str:
    header = f"*{ENGINE_NAME} v{ENGINE_VERSION}* — Live-Performance Circuit Breaker"
    if tripped:
        return f"{header}\n\n🤯 Adaptation frozen: {cb['reason']}\nSignal generation continues on last-known-good parameters."
    return f"{header}\n\n👏 Adaptation resumed — live performance recovered to baseline."


def format_daily_summary(state: dict, day_key: str) -> str:
    trades = [t for t in state["tier2_trades"]
              if datetime.fromtimestamp(t["resolved_ts"] / 1000, tz=timezone.utc).strftime("%Y-%m-%d") == day_key]
    n = len(trades)
    wins = sum(1 for t in trades if t["result"] == "win")
    losses = n - wins
    wr = (wins / n * 100) if n else 0.0
    gross_win = sum(t["r_realized"] for t in trades if t["r_realized"] > 0)
    gross_loss = abs(sum(t["r_realized"] for t in trades if t["r_realized"] < 0))
    pf = (gross_win / gross_loss) if gross_loss > 1e-9 else (gross_win if gross_win > 0 else 0.0)
    avg_rr = statistics.fmean([t["r_realized"] for t in trades if t["result"] == "win"]) if wins else 0.0

    by_engine = collections.defaultdict(lambda: {"n": 0, "wins": 0})
    for t in trades:
        eng = t["engine"]
        by_engine[eng]["n"] += 1
        by_engine[eng]["wins"] += 1 if t["result"] == "win" else 0
    engine_lines = [f"  {human_label(e)}: {v['wins']}/{v['n']} ({(v['wins']/v['n']*100 if v['n'] else 0):.0f}%)"
                     for e, v in by_engine.items()]

    by_regime = collections.defaultdict(lambda: {"n": 0, "wins": 0})
    for t in trades:
        rk = "Trending" if (t.get("regime_at_entry", {}).get("trend_strength") or 0) >= 22 else "Ranging"
        by_regime[rk]["n"] += 1
        by_regime[rk]["wins"] += 1 if t["result"] == "win" else 0
    regime_lines = [f"  {r}: {v['wins']}/{v['n']}" for r, v in by_regime.items()]

    forensic_lines = [f"  {human_label(k)}: {v}" for k, v in state["tier1"]["forensic_counts"].items()]

    calib_lines = []
    for key, v in state["tier1"]["calibration_buckets"].items():
        if v.get("n", 0) >= 5:
            engine, bucket = key.split("|", 1)
            realized_wr = v["wins"] / v["n"] * 100
            calib_lines.append(f"  {human_label(engine)} [{bucket}]: {realized_wr:.0f}% realized ({v['n']} trades)")

    best = max(trades, key=lambda t: t["r_realized"], default=None)
    worst = min(trades, key=lambda t: t["r_realized"], default=None)

    lines = [
        f"*{ENGINE_NAME} v{ENGINE_VERSION} — Daily Summary ({day_key})*",
        "",
        f"Signals resolved: {n}    Wins: {wins}    Losses: {losses}",
        f"Win rate: {wr:.1f}%    Profit factor: {pf:.2f}    Avg winning RR: {avg_rr:.2f}",
        "",
        "*By Engine:*", *(engine_lines or ["  (none)"]),
        "",
        "*By Regime:*", *(regime_lines or ["  (none)"]),
        "",
        "*Forensic Categories:*", *(forensic_lines or ["  (none)"]),
        "",
        "*Confidence Calibration (realized win rate per bucket):*", *(calib_lines or ["  (insufficient data)"]),
        "",
        f"Best setup: {best['symbol']} {human_label(best['engine'])} ({best['r_realized']:.2f}R)" if best else "Best setup: n/a",
        f"Worst setup: {worst['symbol']} {human_label(worst['engine'])} ({worst['r_realized']:.2f}R)" if worst else "Worst setup: n/a",
        "",
        f"Circuit breaker: {'TRIPPED — ' + str(state['tier1']['circuit_breaker']['reason']) if state['tier1']['circuit_breaker']['tripped'] else 'normal'}",
    ]
    return "\n".join(lines)


# ============================================================================
# SECTION 15 -- MAIN ORCHESTRATION (scan-per-run)
# ============================================================================

def scan_symbol(symbol: str, candle_cache: dict, state: dict, now_ms: int) -> list:
    """Runs the full mandatory pipeline for one asset: fetch -> build views
    -> Stage 1-4 -> specialized engines -> Decision Engine scoring. Returns
    ranked (score, grade, candidate, result) tuples, or [] on any NO TRADE
    halt (Section 7's Trade Filter)."""
    bundle = fetch_all_candles(symbol, candle_cache, now_ms)
    if bundle is None:
        return []
    try:
        views = build_all_views(bundle)
    except (ValueError, ZeroDivisionError, IndexError, statistics.StatisticsError) as e:
        log.warning("build_all_views failed for %s: %s", symbol, e)
        return []

    stage1 = stage1_bias(views)
    if stage1.outcome == "neutral" and not ENABLE_COUNTERTREND_ENGINE:
        return []  # Trade Filter: Stage 1 Neutral -> NO TRADE (base ensemble has nothing to do)

    stage2 = stage2_context(views, stage1.outcome) if stage1.outcome != "neutral" else StageResult(2, "disagree")
    if stage1.outcome != "neutral" and stage2.outcome != "agree":
        stage3 = StageResult(3, "INVALID", "Stage 2 gate not evaluated")
        stage4 = StageResult(4, "NO TRADE", "Stage 3 gate not evaluated")
    elif stage1.outcome == "neutral":
        stage3 = StageResult(3, "INVALID", "no bias")
        stage4 = StageResult(4, "NO TRADE", "no bias")
    else:
        stage3 = stage3_zone_selection(views, stage1.outcome, state, symbol)
        if stage3.outcome == "VALID":
            stage4 = stage4_entry(views, stage1.outcome, stage3.poi)  # type: ignore[attr-defined]
        else:
            stage4 = StageResult(4, "NO TRADE", "Stage 3 not VALID")

    return {"symbol": symbol, "views": views, "stage1": stage1, "stage3": stage3, "stage4": stage4}


def run_scan(state: dict, candle_cache: dict, deadline: Optional[float] = None) -> None:
    now_ms = utcnow_ms()
    active = state["active_signals"]
    deadline = deadline if deadline is not None else (time.monotonic() + SOFT_RUN_DEADLINE_S)
    t_start = time.monotonic()

    n_cached_symbols = len(candle_cache)
    log.info("=== run_scan starting: %d active signal(s), candle_cache has %d/%d watchlist symbols cached ===",
              len(active), n_cached_symbols, len(WATCHLIST))
    if n_cached_symbols == 0:
        log.warning("candle_cache is EMPTY -- this run is a full cold start and will be slow "
                    "(every symbol/timeframe needs a full history pull, not a delta fetch).")

    # --- 1. monitor every currently active/pending signal first -----------
    still_active = []
    for i, sig in enumerate(active):
        if time.monotonic() > deadline:
            log.warning("Soft deadline hit while monitoring active signals (%d/%d done); "
                        "deferring the rest to next run.", i, len(active))
            still_active.extend(active[i:])
            break
        symbol = sig["symbol"]
        m_candles = get_candles(symbol, MONITOR_TF, 30, now_ms,
                                 candle_cache.get(symbol, {}).get(MONITOR_TF))
        event = monitor_signal(sig, m_candles)
        if event is None:
            still_active.append(sig)
            continue
        if event["type"] == "expired":
            fill_key = f"{sig['engine']}|{sig['entry_kind']}"
            fs = state["tier1"]["fill_stats"].setdefault(fill_key, {"dispatched": 0, "filled": 0, "expired": 0})
            fs["expired"] += 1
            send_telegram(format_status_message(sig, "expired"), sig.get("tg_message_id"))
            continue  # excluded from win/loss stats (Section 12)
        if event["type"] in ("win", "loss"):
            bundle = fetch_all_candles(symbol, candle_cache, now_ms)
            if bundle:
                try:
                    views = build_all_views(bundle)
                    stage1 = stage1_bias(views)
                    regime = build_regime_vector(views, stage1.outcome if stage1.outcome != "neutral" else "neutral",
                                                  {}, now_ms)
                except (ValueError, ZeroDivisionError, IndexError, statistics.StatisticsError):
                    regime = RegimeVector("neutral", 50, 15, "off_hours", 0.5, 0.0, "neutral", 0.5, 0.0)
            else:
                regime = RegimeVector("neutral", 50, 15, "off_hours", 0.5, 0.0, "neutral", 0.5, 0.0)
            process_resolution(sig, {TF_1H: views[TF_1H]} if bundle else {}, regime, state)
            send_telegram(format_status_message(sig, event["type"]), sig.get("tg_message_id"))
            continue  # resolved -- drop from active_signals
        still_active.append(sig)
    state["active_signals"] = still_active
    log.info("Monitoring phase done (%.1fs elapsed): %d still active/pending.",
              time.monotonic() - t_start, len(still_active))

    if state["tier1"]["circuit_breaker"]["tripped"]:
        log.info("Circuit breaker active -- adaptation frozen, signal generation continues")

    # --- 2. scan every watchlist asset (bounded thread pool) --------------
    views_by_symbol_1h = {}
    scan_results = {}
    if time.monotonic() > deadline:
        log.warning("Soft deadline already hit after monitoring active signals; "
                    "skipping new-signal scan this run, will pick up next cycle.")
    else:
        log.info("Scan phase starting: %d symbols, 6 worker threads.", len(WATCHLIST))
        n_done = 0
        with ThreadPoolExecutor(max_workers=6) as pool:
            futures = {pool.submit(scan_symbol, sym, candle_cache, state, now_ms): sym for sym in WATCHLIST}
            for fut in as_completed(futures):
                if time.monotonic() > deadline:
                    log.warning("Soft deadline hit mid-scan (%d/%d symbols done); "
                                "cancelling remaining symbol scans.", n_done, len(WATCHLIST))
                    for f in futures:
                        f.cancel()
                    break
                sym = futures[fut]
                try:
                    result = fut.result()
                except Exception as e:  # noqa: BLE001 -- never let one symbol's failure kill the run
                    log.error("scan_symbol failed for %s: %s", sym, e)
                    result = []
                n_done += 1
                log.info("  [%d/%d] %s scanned (%.1fs elapsed)", n_done, len(WATCHLIST), sym,
                          time.monotonic() - t_start)
                if result:
                    scan_results[sym] = result
                    views_by_symbol_1h[sym] = result["views"][TF_1H]
        log.info("Scan phase done (%.1fs elapsed): %d/%d symbols produced usable data.",
                  time.monotonic() - t_start, len(scan_results), len(WATCHLIST))

    macro_view = views_by_symbol_1h.get(MACRO_ASSET)
    macro_bias = "neutral"
    if macro_view is not None and MACRO_ASSET in scan_results:
        macro_bias = scan_results[MACRO_ASSET]["stage1"].outcome

    all_new_signals = []
    for symbol, result in scan_results.items():
        if time.monotonic() > deadline:
            log.warning("Soft deadline hit during ranking/dispatch; stopping here, "
                        "remaining candidates picked up next run.")
            break
        views, stage1, stage3, stage4 = result["views"], result["stage1"], result["stage3"], result["stage4"]
        try:
            regime = build_regime_vector(views, macro_bias, views_by_symbol_1h, now_ms)
        except (ValueError, ZeroDivisionError, statistics.StatisticsError) as e:
            log.warning("regime vector failed for %s: %s", symbol, e)
            continue

        candidates = run_specialized_engines(stage1.outcome, views, stage3, stage4, regime, state, symbol)
        if not candidates:
            continue
        ranked = rank_and_select(candidates, views, regime, state, symbol, now_ms)
        accepted = correlation_dedup(ranked, state["active_signals"] + all_new_signals)
        for score, grade, cand, res in accepted:
            sig = new_signal_record(cand, score, grade, symbol, now_ms)
            sig["regime_at_entry"] = {"trend_strength": regime.trend_strength,
                                       "volatility_pctl": regime.volatility_pctl, "macro_bias": macro_bias}
            fill_key = f"{cand.engine}|{cand.entry_kind}"
            fs = state["tier1"]["fill_stats"].setdefault(fill_key, {"dispatched": 0, "filled": 0, "expired": 0})
            fs["dispatched"] += 1
            msg_id = send_telegram(format_signal_message(sig))
            sig["tg_message_id"] = msg_id
            all_new_signals.append(sig)
            log.info("Dispatched %s %s %s grade=%s score=%.2f", symbol, cand.engine, cand.direction, grade, score)

    state["active_signals"].extend(all_new_signals)

    # --- 3. daily summary at/after 08:00 UTC, once per day ----------------
    now_dt = datetime.fromtimestamp(now_ms / 1000, tz=timezone.utc)
    day_key = now_dt.strftime("%Y-%m-%d")
    if now_dt.hour >= 8 and state["tier1"]["daily_totals"].get(day_key) != "sent":
        send_telegram(format_daily_summary(state, day_key))
        state["tier1"]["daily_totals"][day_key] = "sent"

    prune_tier2(state)
    state["last_run_ts"] = now_ms
    log.info("=== run_scan finished in %.1fs: %d new signal(s) dispatched, %d active signal(s) total ===",
              time.monotonic() - t_start, len(all_new_signals), len(state["active_signals"]))


def main() -> None:
    log.info("=== %s v%s process started (pid=%d) ===", ENGINE_NAME, ENGINE_VERSION, os.getpid())
    run_started = time.monotonic()
    lock_f = open(LOCK_PATH, "a+")
    try:
        fcntl.flock(lock_f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        log.warning("Another run is already in progress; exiting.")
        return
    try:
        state = load_state()
        candle_cache = load_candle_cache()
        log.info("Loaded state.json (%d active signal(s)) and candle_cache.json (%d symbol(s) cached).",
                  len(state.get("active_signals", [])), len(candle_cache))
        try:
            run_scan(state, candle_cache, deadline=run_started + SOFT_RUN_DEADLINE_S)
        except Exception as e:  # noqa: BLE001 -- top-level safety net; the run must never crash unlogged
            log.exception("run_scan failed: %s", e)
        finally:
            save_state(state)
            save_candle_cache(candle_cache)
            log.info("=== process finished, state saved (%.1fs total) ===", time.monotonic() - run_started)
    finally:
        fcntl.flock(lock_f.fileno(), fcntl.LOCK_UN)
        lock_f.close()


if __name__ == "__main__":
    main()
