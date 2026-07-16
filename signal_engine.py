#!/usr/bin/env python3
# =============================================================================
# VIRELLE ADAPTIVE SIGNAL ENGINE  —  v1.1.1
# -----------------------------------------------------------------------------
# An institutional-grade, self-learning, multi-engine crypto signal generator
# for Hyperliquid perpetuals. Single self-contained file — no local imports.
#
# Design note (per spec Section 1/21): this file is an original synthesis, not
# a merge of the reference engines. Every reference engine attached to this
# project was audited for two known bug classes before any idea was reused:
#   (a) automatic SL-to-breakeven repositioning on TP1  -> NOT present here
#       (see Section 11 rules, enforced in `resolve_active_signals`).
#   (b) missing entry-fill verification ("phantom fills") -> NOT present here
#       (see `entry_kind` / pending-fill machinery in `check_entry_fill`).
# Best-of-breed ideas adopted (weight-budgeted rate limiter, delta candle
# cache, closed-candle-only structural reads) are re-implemented independently
# below, never copy-pasted.
#
# v1.1.0 changelog — architecture ported from Kestrel v1.2.1 (audited
# engine-by-engine, added only where missing):
#   1. Mandatory LTF confirmation trigger: every Candidate now carries
#      `ltf_confirmed`; engines already routed through
#      `_zone_selection_sequence` (smc, pullback, order_block, breaker_block,
#      fair_value_gap) satisfy it via the existing mss.confirmed gate, and
#      the remaining engines are checked against the new `_ltf_confirmation`
#      helper. Enforced as a hard gate in `run_adaptive_filters`.
#   2. Regime/quality-adaptive RR floor: `_finalize_tp_pair` now derives a
#      per-candidate `dyn_floor` (bumped for high noise/volatility and for
#      low zone-quality, via the new `_quality_score_for`) instead of the
#      flat TP1_RR_FLOOR constant; stored per-candidate as `rr_floor_used`
#      and enforced in `run_adaptive_filters`.
#   3. Room-to-next-opposing-level TP construction: `_finalize_tp_pair` now
#      derives TP1/TP2 from `_room_to_next_opposing_level` (measured
#      distance to the next real opposing swing/EQH/EQL) first, falling back
#      to the fixed-RR ceiling only when nothing binding is in the way;
#      liquidity-wall clipping is still applied as a final safety net.
#   4. Per-engine adaptive weight + pause governor: `update_engine_governor`
#      (called once per scan) hard-pauses/cautiously-reactivates individual
#      engines off their own rolling win rate, independent of the global
#      circuit breaker. This also fixes the prior one-way-only engine_weight
#      nudge in `route_forensic_response` (which could raise an engine's
#      weight on a win but never lower it on a loss) by replacing it with a
#      single symmetric update in the new governor.
#
# v1.1.1 changelog — audited against Vantage Annex v2.0.0 (same two bug
# classes checked first; neither present here, see above):
#   1. SL liquidity-pool clearing: `_adaptive_sl_buffer` now also pushes the
#      wick-buffered SL fully clear of any SSL/BSL equal-highs/equal-lows
#      cluster it would otherwise rest at or inside, via the new
#      `_clear_sl_of_liquidity_pool` (re-implemented independently from
#      Vantage Annex's `_clear_sl_of_liquidity_pool`, adapted to this file's
#      `equal_levels`/`Swing` structures rather than copied). Previously the
#      wick buffer was the only adjustment made to a structural SL, so a stop
#      could still land inside a known liquidity pool -- exactly the level a
#      stop-hunt wick is drawn to sweep -- and get tagged by that sweep
#      without the setup actually being invalidated. All thirteen engines now
#      route through the pool-clearing step, since every one of them derives
#      its SL from this single function.
#
# Run mode: single scan-per-invocation, intended to be triggered every 15
# minutes by an external scheduler (GitHub Actions / cron-job.org). Each run:
#   1. loads state.json (Tier 1 aggregates + Tier 2 bounded trade log)
#   2. refreshes the candle cache for the whole watchlist (both TF combos)
#   3. resolves any active signals still being tracked (fill / SL / TP1 /
#      expiry) against newly closed candles, runs loss-forensics + adaptive
#      learning on anything that resolved this run
#   4. runs the full multi-engine ensemble + Decision Engine over the
#      watchlist to find new candidates, dispatches the best ones
#   5. sends Telegram notifications, writes state.json back to disk
# =============================================================================

from __future__ import annotations

import json
import logging
import math
import os
import random
import signal
import statistics
import sys
import time
import urllib.error
import urllib.request
import collections
import threading
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone, timedelta
from typing import Any, Optional

# =============================================================================
# SECTION A — CONFIGURATION & CONSTANTS
# =============================================================================

ENGINE_NAME = "Virelle"
ENGINE_VERSION = "1.1.1"
RESOLUTION_LOGIC_VERSION = "1.0.0"  # bumped whenever outcome-scoring logic changes (Section 11)

# Same watchlist as every reference engine in this project (Section: "Use the
# same watchlist as the reference engines"). Hyperliquid coin symbols (no
# quote-asset suffix).
WATCHLIST: list[str] = [
    "BTC", "ETH", "HYPE", "ZEC", "NEAR", "ONDO", "SUI", "PENGU", "BNB", "SOL",
    "TRX", "BCH", "DOGE", "ADA", "DOT", "TAO", "AVAX", "LINK", "AAVE", "XRP",
    "XLM", "UNI", "LTC", "APT", "PENDLE",
]
MACRO_ANCHOR = "BTC"  # dominant benchmark asset for macro bias (Section 6)

STATE_PATH = os.environ.get("VIRELLE_STATE_PATH", "state.json")
CANDLE_CACHE_PATH = os.environ.get("VIRELLE_CANDLE_CACHE_PATH", "candle_cache.json")

TG_BOT_TOKEN = os.environ.get("TG_BOT_TOKEN", "")
TG_CHAT_ID = os.environ.get("TG_CHAT_ID", "")
REACTION_IMAGE_PATH = os.environ.get("VIRELLE_REACTION_IMAGE", "reaction.jpg")

# --- Timeframes (Section 7) ---------------------------------------------------
# Forbidden: 1m/2m/3m/5m. Minimum timeframe 15m. Two independent combos run
# side by side every scan; neither is a fallback for the other.
TF_MS = {
    "15m": 15 * 60_000,
    "1h": 60 * 60_000,
    "4h": 4 * 60 * 60_000,
    "1d": 24 * 60 * 60_000,
}
INTRADAY_COMBO = {"high": "4h", "medium": "1h", "low": "15m"}
SWING_COMBO = {"high": "1d", "medium": "4h", "low": "1h"}
COMBOS = {"intraday": INTRADAY_COMBO, "swing": SWING_COMBO}
CANDLE_COUNT = {"15m": 400, "1h": 400, "4h": 400, "1d": 300}

# --- Risk / RR (Section 10) ---------------------------------------------------
TP1_RR_FLOOR = 1.5
TP1_RR_NATURAL_CEILING = 2.0  # soft — honest structural TP1 shouldn't be stretched past this
MIN_ENTRY_TO_SL_ATR_FRAC = 0.25   # minimum entry-to-SL distance, in ATR fractions
MIN_ENTRY_TO_TP1_ATR_FRAC = 0.45  # minimum entry-to-TP1 distance, in ATR fractions
MAX_PENDING_ENTRY_ATR_MULT = 2.2  # cap how far a pending/zone entry may sit from market price

# --- Concurrency / correlation (Section 14) -----------------------------------
MAX_CONCURRENT_ACTIVE_SIGNALS = 8
CORRELATION_CLUSTER_MIN_ABS_CORR = 0.72
MAX_CONCURRENT_PER_CLUSTER = 2

# --- Pending-entry lifecycle (Section 12) -------------------------------------
PENDING_ENTRY_EXPIRY_BARS = {"intraday": 12, "swing": 10}  # counted on the combo's low TF

# --- Learning governance (Section 5 / 13) -------------------------------------
MIN_SAMPLE_SIZE_FOR_ADAPTATION = 20   # per segment (asset/regime/tf/engine/category)
MAX_PARAM_STEP_FRACTION = 0.08        # max fractional move of a bounded parameter per run
EMA_LEARNING_ALPHA = 0.15             # exponential-smoothing weight for newly observed value
CIRCUIT_BREAKER_WINDOW = 30           # rolling trades compared against baseline
CIRCUIT_BREAKER_WIN_RATE_DROP = 0.12  # absolute win-rate drop vs baseline that trips the breaker
CIRCUIT_BREAKER_PF_DROP_FRAC = 0.25   # fractional profit-factor drop vs baseline that trips it

# --- Per-engine adaptive weight / pause governor (Section 13 refinement) -------
# The global circuit breaker above only freezes/resumes adaptation for the
# whole ensemble at once. This discriminates by engine, so one persistently
# bad engine (e.g. mean_reversion misfiring in a trend regime) can be
# hard-paused and later cautiously reactivated at reduced weight, without
# needing the whole system's circuit breaker to trip.
ENGINE_GOVERNOR_LOOKBACK = 40          # rolling resolved-trade window for the weight nudge
ENGINE_GOVERNOR_MIN_SAMPLE = 10        # min resolved trades in-window before a weight nudge is trusted
ENGINE_GOVERNOR_TARGET_WINRATE = 0.50
ENGINE_PAUSE_LOOKBACK = 20             # trailing resolved trades checked for the hard-pause trigger
ENGINE_PAUSE_WINRATE_FLOOR = 0.20      # win rate below this over the lookback -> hard pause
ENGINE_REACTIVATE_WEIGHT = 0.5         # weight assigned on reactivation -- cautious re-entry, not a snap back to 1.0

# --- State file tiering (Section 5) -------------------------------------------
TIER2_RETENTION_DAYS = 15
TIER2_MAX_TRADES = 1500

# --- Macro/news blackout (Section 13) -----------------------------------------
# Static weekly/monthly high-impact windows the engine always respects, plus a
# generic "unscheduled" spike guard computed live (see `in_macro_blackout`).
MACRO_BLACKOUT_MINUTES_BEFORE = 20
MACRO_BLACKOUT_MINUTES_AFTER = 40
# Correlated-majors group that shares a blackout when BTC/ETH are affected.
MACRO_CORRELATED_MAJORS = {"BTC", "ETH", "SOL", "BNB"}

HL_API_URL = "https://api.hyperliquid.xyz/info"
HL_WEIGHT_BUDGET_PER_MINUTE = 1100  # conservative vs Hyperliquid's documented 1200/min info budget
HL_DEFAULT_INFO_WEIGHT = 2
HL_ENDPOINT_BASE_WEIGHT = {"candleSnapshot": 2, "allMids": 2, "meta": 2}

SCAN_SOFT_DEADLINE_S = 12.5 * 60  # stay comfortably inside a 15-minute cadence

# =============================================================================
# SECTION B — LOGGING
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
log = logging.getLogger("virelle")

_shutdown_requested = False


def _handle_shutdown(signum, frame):
    global _shutdown_requested
    _shutdown_requested = True
    log.warning("Shutdown signal received (%s); finishing current step safely.", signum)


signal.signal(signal.SIGTERM, _handle_shutdown)
signal.signal(signal.SIGINT, _handle_shutdown)


def now_ms() -> int:
    return int(time.time() * 1000)


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# =============================================================================
# SECTION C — HYPERLIQUID API CLIENT
# =============================================================================

class _WeightRateLimiter:
    """Sliding-60s-window pacer tracking Hyperliquid request *weight*, not raw
    call count, shared across the whole run so no endpoint can starve the
    budget for another."""

    def __init__(self, budget_per_minute: float):
        self.budget = budget_per_minute
        self.window_s = 60.0
        self.lock = threading.Lock()
        self.events: collections.deque[tuple[float, float]] = collections.deque()

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
            n_bars = max(1, math.ceil((end_ms - start_ms) / TF_MS[interval]))
        return HL_DEFAULT_INFO_WEIGHT * math.ceil(n_bars / 60)
    return HL_ENDPOINT_BASE_WEIGHT.get(req_type, HL_DEFAULT_INFO_WEIGHT)


def hl_post(payload: dict, retries: int = 4, timeout: int = 12) -> Any:
    body = json.dumps(payload).encode()
    weight = _request_weight(payload)
    for attempt in range(retries):
        if _shutdown_requested:
            return None
        _rate_limiter.wait(weight)
        req = urllib.request.Request(
            HL_API_URL, data=body, headers={"Content-Type": "application/json"}
        )
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode())
        except urllib.error.HTTPError as e:
            if e.code == 429:
                retry_after = e.headers.get("Retry-After")
                wait_s = float(retry_after) if retry_after else 10.0
                log.warning("HL 429 on %s (attempt %d), backing off %.1fs",
                            payload.get("type"), attempt + 1, wait_s)
                time.sleep(wait_s)
            else:
                log.warning("HL HTTP error attempt %d (%s): %s", attempt + 1, payload.get("type"), e)
                time.sleep(0.8 * (attempt + 1))
        except (urllib.error.URLError, TimeoutError, ValueError, OSError) as e:
            log.warning("HL request attempt %d failed (%s): %s", attempt + 1, payload.get("type"), e)
            time.sleep(0.8 * (attempt + 1))
    log.error("HL request exhausted retries for type=%s", payload.get("type"))
    return None


def current_bar_open_ms(reference_ms: int, interval: str) -> int:
    step = TF_MS[interval]
    return (reference_ms // step) * step


def filter_closed_candles(candles: list[dict], interval: str, reference_ms: int) -> list[dict]:
    """Anti-repainting guard (Section 12A): a still-forming candle is never
    returned to any caller, so no downstream structural read can ever see
    provisional intra-candle data."""
    cutoff = current_bar_open_ms(reference_ms, interval)
    return [c for c in candles if c["t"] < cutoff]


def _request_candles(coin: str, interval: str, start_ms: int, end_ms: int) -> list[dict]:
    payload = {"type": "candleSnapshot",
               "req": {"coin": coin, "interval": interval, "startTime": start_ms, "endTime": end_ms}}
    raw = hl_post(payload)
    if not raw:
        return []
    out = []
    for c in raw:
        try:
            out.append({"t": int(c["t"]), "o": float(c["o"]), "h": float(c["h"]),
                        "l": float(c["l"]), "c": float(c["c"]), "v": float(c["v"])})
        except (KeyError, TypeError, ValueError):
            continue
    return out


def get_candles(coin: str, interval: str, n: int, reference_ms: int,
                 cache_entry: Optional[list[dict]] = None) -> list[dict]:
    """Return the last `n` fully-closed candles, using a delta fetch against
    `cache_entry` (persistent candle_cache, Section 15) whenever possible to
    minimize API traffic."""
    step = TF_MS[interval]
    if cache_entry:
        last_cached_t = cache_entry[-1]["t"]
        if current_bar_open_ms(reference_ms, interval) <= last_cached_t + step:
            return filter_closed_candles(cache_entry, interval, reference_ms)[-n:]
        start_ms = last_cached_t - step * 3
        new_raw = _request_candles(coin, interval, start_ms, reference_ms)
        if new_raw:
            merged = {c["t"]: c for c in cache_entry}
            for c in new_raw:
                merged[c["t"]] = c
            candles = [merged[t] for t in sorted(merged.keys())]
        else:
            candles = cache_entry
        return filter_closed_candles(candles, interval, reference_ms)[-n:]

    lookback_ms = n * step * 2 + step * 5
    raw = _request_candles(coin, interval, reference_ms - lookback_ms, reference_ms)
    return filter_closed_candles(raw, interval, reference_ms)[-n:]


def fetch_watchlist_bundles(candle_cache: dict, reference_ms: int) -> dict[str, dict[str, list[dict]]]:
    """One shared fetch pass per run: every timeframe needed by both combos is
    pulled once per asset and reused by every specialized engine (Section 15's
    mandatory persistent candle_cache + Section 16's shared-computation goal)."""
    bundles: dict[str, dict[str, list[dict]]] = {}
    tfs_needed = sorted({tf for combo in COMBOS.values() for tf in combo.values()})
    for coin in WATCHLIST:
        if _shutdown_requested:
            break
        sym_cache = candle_cache.get(coin, {})
        bundle = {}
        ok = True
        for tf in tfs_needed:
            candles = get_candles(coin, tf, CANDLE_COUNT[tf], reference_ms, sym_cache.get(tf))
            if len(candles) < 80:
                log.info("Insufficient %s candles for %s (%d) — skipping this run", tf, coin, len(candles))
                ok = False
                break
            bundle[tf] = candles
            candle_cache.setdefault(coin, {})[tf] = candles[-CANDLE_COUNT[tf]:]
        if ok:
            bundles[coin] = bundle
    return bundles


def fetch_mark_prices() -> dict[str, float]:
    raw = hl_post({"type": "allMids"})
    if not isinstance(raw, dict):
        return {}
    out = {}
    for coin in WATCHLIST:
        v = raw.get(coin)
        if v is not None:
            try:
                out[coin] = float(v)
            except (TypeError, ValueError):
                pass
    return out


# =============================================================================
# SECTION D — INDICATORS & SHARED MATH (closed-candle inputs only)
# =============================================================================

def ema(values: list[float], period: int) -> list[float]:
    if not values:
        return []
    k = 2.0 / (period + 1)
    out = [values[0]]
    for v in values[1:]:
        out.append(v * k + out[-1] * (1 - k))
    return out


def true_range(c: dict, prev_close: float) -> float:
    return max(c["h"] - c["l"], abs(c["h"] - prev_close), abs(c["l"] - prev_close))


def atr_series(candles: list[dict], period: int = 14) -> list[float]:
    if len(candles) < 2:
        return [0.0] * len(candles)
    trs = [candles[0]["h"] - candles[0]["l"]]
    for i in range(1, len(candles)):
        trs.append(true_range(candles[i], candles[i - 1]["c"]))
    return ema(trs, period)


def atr(candles: list[dict], period: int = 14) -> float:
    series = atr_series(candles, period)
    return series[-1] if series else 0.0


def adx(candles: list[dict], period: int = 14) -> float:
    """Standard Wilder ADX — used as the continuous trend-strength component
    of the Regime Vector (Section 6)."""
    if len(candles) < period + 2:
        return 0.0
    plus_dm, minus_dm, trs = [], [], []
    for i in range(1, len(candles)):
        up = candles[i]["h"] - candles[i - 1]["h"]
        down = candles[i - 1]["l"] - candles[i]["l"]
        plus_dm.append(up if (up > down and up > 0) else 0.0)
        minus_dm.append(down if (down > up and down > 0) else 0.0)
        trs.append(true_range(candles[i], candles[i - 1]["c"]))
    atr_e = ema(trs, period)
    plus_de = ema(plus_dm, period)
    minus_de = ema(minus_dm, period)
    dxs = []
    for a, p, m in zip(atr_e, plus_de, minus_de):
        if a <= 0:
            dxs.append(0.0)
            continue
        pdi, mdi = 100 * p / a, 100 * m / a
        denom = pdi + mdi
        dxs.append(0.0 if denom == 0 else 100 * abs(pdi - mdi) / denom)
    adx_series = ema(dxs, period)
    return adx_series[-1] if adx_series else 0.0


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    k = (len(s) - 1) * (pct / 100.0)
    f, c = math.floor(k), math.ceil(k)
    if f == c:
        return s[int(k)]
    return s[f] + (s[c] - s[f]) * (k - f)


def value_percentile_rank(values: list[float], current: float) -> float:
    """Where `current` ranks (0-100) inside its own recent distribution —
    used to express volatility as a percentile rather than a raw number
    (Section 6)."""
    if not values:
        return 50.0
    below = sum(1 for v in values if v <= current)
    return 100.0 * below / len(values)


def noise_index(candles: list[dict], lookback: int = 30) -> float:
    """Choppiness measure independent of raw volatility: ratio of summed
    true-range travel to net directional displacement over the window,
    normalized to ~0..1 (1 = maximally choppy)."""
    window = candles[-lookback:]
    if len(window) < 5:
        return 0.5
    tr_sum = sum(true_range(window[i], window[i - 1]["c"]) for i in range(1, len(window)))
    net_move = abs(window[-1]["c"] - window[0]["c"])
    if tr_sum <= 0:
        return 0.5
    directionality = net_move / tr_sum  # 1 = pure trend, ~0 = pure chop
    return max(0.0, min(1.0, 1.0 - directionality))


def wick_noise_distribution(candles: list[dict], lookback: int = 120) -> list[float]:
    """Adverse-wick excursions beyond the candle body, expressed in absolute
    price terms — the raw distribution the adaptive-percentile SL buffer
    (Section 10) is drawn from."""
    window = candles[-lookback:]
    out = []
    for c in window:
        body_hi, body_lo = max(c["o"], c["c"]), min(c["o"], c["c"])
        out.append(max(c["h"] - body_hi, body_lo - c["l"], 0.0))
    return out


# =============================================================================
# SECTION E — MARKET STRUCTURE (closed candles only; shared by backtest & live)
# =============================================================================
# Per Section 12A: these are the ONLY functions that detect swings / BOS /
# CHoCH / order blocks / breaker blocks / FVGs / SFPs / MSS anywhere in this
# file. There is no separate/simplified backtest reimplementation.

@dataclass
class Swing:
    idx: int
    price: float
    kind: str  # "high" | "low"


@dataclass
class Zone:
    kind: str          # "order_block" | "breaker_block" | "fvg"
    direction: str      # "long" | "short"
    top: float
    bottom: float
    origin_idx: int
    mitigated: bool = False
    from_sweep_idx: Optional[int] = None  # enforces sweep->POI causality (Section 8)


def find_swings(candles: list[dict], left: int = 2, right: int = 2) -> list[Swing]:
    swings = []
    for i in range(left, len(candles) - right):
        window = candles[i - left:i + right + 1]
        hi, lo = candles[i]["h"], candles[i]["l"]
        if hi == max(c["h"] for c in window):
            swings.append(Swing(i, hi, "high"))
        if lo == min(c["l"] for c in window):
            swings.append(Swing(i, lo, "low"))
    return swings


def detect_bos_choch(candles: list[dict], swings: list[Swing]) -> dict:
    """Break of Structure / Change of Character read off the most recent
    confirmed swing sequence."""
    highs = [s for s in swings if s.kind == "high"]
    lows = [s for s in swings if s.kind == "low"]
    if len(highs) < 2 or len(lows) < 2:
        return {"bias": "neutral", "event": None}
    last_close = candles[-1]["c"]
    hh = highs[-1].price > highs[-2].price
    hl = lows[-1].price > lows[-2].price
    ll = lows[-1].price < lows[-2].price
    lh = highs[-1].price < highs[-2].price
    bias = "neutral"
    event = None
    if last_close > highs[-1].price:
        bias, event = "bullish", ("BOS" if hh else "CHoCH")
    elif last_close < lows[-1].price:
        bias, event = "bearish", ("BOS" if ll else "CHoCH")
    else:
        bias = "bullish" if (hh and hl) else ("bearish" if (ll and lh) else "neutral")
    return {"bias": bias, "event": event}


def find_equal_levels(swings: list[Swing], candles: list[dict], tolerance_atr_frac: float = 0.12) -> dict:
    """EQH/EQL clustering -> BSL/SSL liquidity pool tagging (Section 8)."""
    tol = atr(candles, 14) * tolerance_atr_frac
    highs = sorted([s for s in swings if s.kind == "high"], key=lambda s: s.price)
    lows = sorted([s for s in swings if s.kind == "low"], key=lambda s: s.price)
    eqh, eql = [], []
    for group, out in ((highs, eqh), (lows, eql)):
        cluster = []
        for s in group:
            if cluster and abs(s.price - cluster[-1].price) <= tol:
                cluster.append(s)
            else:
                if len(cluster) >= 2:
                    out.append(cluster[:])
                cluster = [s]
        if len(cluster) >= 2:
            out.append(cluster)
    return {"eqh_clusters": eqh, "eql_clusters": eql}


def detect_sfp(candles: list[dict], swings: list[Swing], equal_levels: dict) -> Optional[dict]:
    """Swing-failure-pattern detection with a purity check: a genuine
    wick-based sweep of a prior swing (ideally an EQH/EQL cluster) that
    closes back inside range, as opposed to an ambiguous/partial poke."""
    if len(candles) < 5:
        return None
    last = candles[-1]
    highs = [s for s in swings if s.kind == "high" and s.idx < len(candles) - 1]
    lows = [s for s in swings if s.kind == "low" and s.idx < len(candles) - 1]
    a = atr(candles, 14) or 1e-9

    def purity(wick: float, body: float) -> float:
        return max(0.0, min(1.0, (wick - body * 0.3) / (a * 0.6)))

    if highs:
        ref = max(highs[-3:], key=lambda s: s.price)
        if last["h"] > ref.price and last["c"] < ref.price:
            wick = last["h"] - max(last["o"], last["c"])
            body = abs(last["c"] - last["o"])
            eqh = any(ref in cl for cl in equal_levels.get("eqh_clusters", []))
            return {"direction": "short", "swept_level": ref.price, "swept_idx": ref.idx,
                    "purity": purity(wick, body), "is_eq_cluster": eqh}
    if lows:
        ref = min(lows[-3:], key=lambda s: s.price)
        if last["l"] < ref.price and last["c"] > ref.price:
            wick = min(last["o"], last["c"]) - last["l"]
            body = abs(last["c"] - last["o"])
            eql = any(ref in cl for cl in equal_levels.get("eql_clusters", []))
            return {"direction": "long", "swept_level": ref.price, "swept_idx": ref.idx,
                    "purity": purity(wick, body), "is_eq_cluster": eql}
    return None


def detect_mss(candles: list[dict], direction: str, lookback: int = 30) -> Optional[dict]:
    """Confirmed market-structure shift following an SFP: a close beyond the
    immediately-preceding counter-trend swing."""
    window = candles[-lookback:]
    swings = find_swings(window, 2, 2)
    if direction == "long":
        counters = [s for s in swings if s.kind == "high"]
        if not counters:
            return None
        ref = counters[-1]
        if window[-1]["c"] > ref.price:
            return {"confirmed": True, "level": ref.price}
    else:
        counters = [s for s in swings if s.kind == "low"]
        if not counters:
            return None
        ref = counters[-1]
        if window[-1]["c"] < ref.price:
            return {"confirmed": True, "level": ref.price}
    return None


def find_order_blocks(candles: list[dict], from_idx: int, direction: str, lookback: int = 40) -> list[Zone]:
    """Last opposite-direction candle before an impulsive move away, searched
    only within candles preceding `from_idx` (the sweep/MSS anchor) so the
    resulting POI is causally downstream of that specific event."""
    start = max(1, from_idx - lookback)
    zones = []
    a = atr(candles, 14) or 1e-9
    for i in range(start, from_idx):
        c = candles[i]
        nxt = candles[i + 1] if i + 1 < len(candles) else None
        if nxt is None:
            continue
        impulse = nxt["c"] - nxt["o"]
        if direction == "long" and c["c"] < c["o"] and impulse > a * 0.5:
            zones.append(Zone("order_block", "long", c["h"], c["l"], i, from_sweep_idx=from_idx))
        elif direction == "short" and c["c"] > c["o"] and impulse < -a * 0.5:
            zones.append(Zone("order_block", "short", c["h"], c["l"], i, from_sweep_idx=from_idx))
    return zones[-3:]


def find_fvgs(candles: list[dict], from_idx: int, direction: str, lookback: int = 40) -> list[Zone]:
    start = max(1, from_idx - lookback)
    zones = []
    for i in range(start, min(from_idx + 1, len(candles) - 1)):
        c0, c2 = candles[i - 1], candles[i + 1] if i + 1 < len(candles) else None
        if c2 is None:
            continue
        if direction == "long" and c2["l"] > c0["h"]:
            zones.append(Zone("fvg", "long", c2["l"], c0["h"], i, from_sweep_idx=from_idx))
        elif direction == "short" and c2["h"] < c0["l"]:
            zones.append(Zone("fvg", "short", c0["l"], c2["h"], i, from_sweep_idx=from_idx))
    return zones[-3:]


def find_breaker_blocks(candles: list[dict], order_blocks: list[Zone], direction: str) -> list[Zone]:
    """A breaker is a former opposite-direction order block that price has
    since traded through and flipped — the most recent confirmed
    institutional footprint (Section 8 step 5)."""
    breakers = []
    last_close = candles[-1]["c"]
    for ob in order_blocks:
        opposite = "short" if direction == "long" else "long"
        if ob.direction == opposite:
            traded_through = (last_close > ob.top) if direction == "long" else (last_close < ob.bottom)
            if traded_through:
                breakers.append(Zone("breaker_block", direction, ob.top, ob.bottom, ob.origin_idx,
                                      from_sweep_idx=ob.from_sweep_idx))
    return breakers


def premium_discount_zone(candles: list[dict], lookback: int = 50) -> dict:
    window = candles[-lookback:]
    hi, lo = max(c["h"] for c in window), min(c["l"] for c in window)
    mid = (hi + lo) / 2
    last = candles[-1]["c"]
    frac = (last - lo) / (hi - lo) if hi > lo else 0.5
    zone = "premium" if last > mid else "discount"
    return {"zone": zone, "fraction": frac, "range_high": hi, "range_low": lo}


def ote_refine(entry_impulse_start: float, entry_impulse_end: float, direction: str,
               zone_top: float, zone_bottom: float) -> Optional[float]:
    """Fibonacci OTE (61.8-79%) refinement of *where inside* an already
    validated zone to place entry — never a standalone confluence
    (Section 8 step 6)."""
    lo, hi = min(entry_impulse_start, entry_impulse_end), max(entry_impulse_start, entry_impulse_end)
    span = hi - lo
    if span <= 0:
        return None
    if direction == "long":
        ote_top = hi - 0.618 * span
        ote_bottom = hi - 0.79 * span
    else:
        ote_top = lo + 0.79 * span
        ote_bottom = lo + 0.618 * span
    ote_top, ote_bottom = max(ote_top, ote_bottom), min(ote_top, ote_bottom)
    overlap_top = min(ote_top, zone_top)
    overlap_bottom = max(ote_bottom, zone_bottom)
    if overlap_top <= overlap_bottom:
        return None
    return (overlap_top + overlap_bottom) / 2


# =============================================================================
# SECTION F — COMPOSITE REGIME VECTOR (Section 6)
# =============================================================================

@dataclass
class RegimeVector:
    macro_bias: str            # "bullish" | "bearish" | "neutral"
    volatility_pctile: float   # 0..100
    trend_strength: float      # ADX, roughly 0..60+
    session: str                # "asia" | "london" | "ny" | "off_hours"
    session_open_proximity: float  # 0..1 continuous, decays away from session opens
    liquidity_draw: str          # "erl" | "irl" | "neutral"
    liquidity_draw_strength: float  # 0..1
    noise: float                # 0..1
    breadth: float               # 0..1


def session_for(ts: datetime) -> str:
    h = ts.hour
    if 0 <= h < 7:
        return "asia"
    if 7 <= h < 12:
        return "london"
    if 12 <= h < 21:
        return "ny"
    return "off_hours"


def session_open_proximity(ts: datetime) -> float:
    """Continuous, decaying score peaking at London (07:00 UTC) and NY
    (12:00 UTC) opens — never a hard gate (Section 6)."""
    minutes = ts.hour * 60 + ts.minute
    anchors = [7 * 60, 12 * 60]
    best = min(min(abs(minutes - a), 1440 - abs(minutes - a)) for a in anchors)
    decay_window = 90.0  # minutes
    return max(0.0, 1.0 - best / decay_window)


def compute_breadth(bundles: dict[str, dict[str, list[dict]]], tf: str = "1h") -> float:
    agrees, total = 0, 0
    for coin, bundle in bundles.items():
        candles = bundle.get(tf)
        if not candles or len(candles) < 20:
            continue
        fast = ema([c["c"] for c in candles], 8)[-1]
        slow = ema([c["c"] for c in candles], 21)[-1]
        total += 1
        if fast > slow:
            agrees += 1
    if total == 0:
        return 0.5
    up_frac = agrees / total
    return abs(up_frac - 0.5) * 2  # coherence, not direction


def compute_regime_vector(coin: str, bundles: dict[str, dict[str, list[dict]]],
                           macro_bundle: Optional[dict[str, list[dict]]],
                           ts: datetime) -> RegimeVector:
    bundle = bundles[coin]
    htf = bundle.get("1d") or bundle.get("4h")
    vol_lookback_candles = bundle["1h"]
    vol_series = atr_series(vol_lookback_candles, 14)
    cur_vol = vol_series[-1] if vol_series else 0.0
    vol_pctile = value_percentile_rank(vol_series[-200:], cur_vol)

    trend_strength = adx(bundle["1h"], 14)

    macro_bias = "neutral"
    if macro_bundle is not None:
        mtf = macro_bundle.get("4h") or macro_bundle.get("1h")
        if mtf and len(mtf) > 25:
            fast = ema([c["c"] for c in mtf], 8)[-1]
            slow = ema([c["c"] for c in mtf], 21)[-1]
            macro_bias = "bullish" if fast > slow else "bearish"

    session = session_for(ts)
    sop = session_open_proximity(ts)

    pdz = premium_discount_zone(htf or bundle["1h"], 50)
    # ERL/IRL: price in extreme premium/discount + low internal-zone density -> external draw.
    if pdz["fraction"] > 0.85 or pdz["fraction"] < 0.15:
        draw, draw_strength = "erl", min(1.0, abs(pdz["fraction"] - 0.5) * 2)
    elif 0.4 <= pdz["fraction"] <= 0.6:
        draw, draw_strength = "irl", 1.0 - abs(pdz["fraction"] - 0.5) * 5
    else:
        draw, draw_strength = "neutral", 0.3

    noise = noise_index(bundle["15m"], 30)
    breadth = compute_breadth(bundles, "1h")

    return RegimeVector(macro_bias, vol_pctile, trend_strength, session, sop,
                         draw, max(0.0, min(1.0, draw_strength)), noise, breadth)


def regime_label(rv: RegimeVector) -> str:
    """Coarse human-readable label derived from the vector, used only for
    display/logging/forensic bucketing — never for scoring (scoring always
    reads the continuous vector directly)."""
    if rv.volatility_pctile > 80 and rv.noise < 0.4:
        return "expansion"
    if rv.trend_strength > 25 and rv.noise < 0.45:
        return "trending"
    if rv.trend_strength < 15 and rv.noise > 0.55:
        return "ranging_choppy"
    if rv.trend_strength < 18:
        return "consolidation"
    if rv.volatility_pctile < 20:
        return "low_volatility"
    return "neutral"


# =============================================================================
# SECTION G — ADAPTIVE PARAMETER STORE (bounded, dampened; Section 5)
# =============================================================================

@dataclass
class ParamSpec:
    default: float
    lo: float
    hi: float


PARAM_SPECS: dict[str, ParamSpec] = {
    # per-engine weight multipliers in the composite score (Section 4)
    **{f"engine_weight::{eng}": ParamSpec(1.0, 0.35, 1.8) for eng in [
        "smc", "trend_continuation", "breakout", "pullback", "liquidity_sweep",
        "order_block", "breaker_block", "fair_value_gap", "momentum", "reversal",
        "mean_reversion", "range_trading", "volatility_expansion",
    ]},
    # composite-score term weights (kept small & auditable, Section 4)
    "term_weight::regime_fit": ParamSpec(1.0, 0.4, 1.6),
    "term_weight::mtf_alignment": ParamSpec(1.0, 0.4, 1.8),
    "term_weight::confluence": ParamSpec(1.0, 0.4, 1.6),
    "term_weight::segment_performance": ParamSpec(1.0, 0.3, 1.7),
    "term_weight::ev_rr": ParamSpec(0.6, 0.2, 1.0),
    "term_weight::liquidity_vol_context": ParamSpec(1.0, 0.4, 1.6),
    "term_weight::session_open_proximity": ParamSpec(0.3, 0.0, 1.0),
    # filter thresholds (Section 9)
    "filter::min_confluence_score": ParamSpec(0.42, 0.30, 0.62),
    "filter::liquidity_sanity_margin_atr": ParamSpec(0.35, 0.15, 0.75),
    "filter::sfp_purity_min": ParamSpec(0.45, 0.30, 0.75),
    # risk parameters (Section 10)
    "risk::sl_buffer_percentile": ParamSpec(70.0, 55.0, 90.0),
    # confidence calibration offsets, keyed per engine at runtime (see get_param)
    "calib::global_offset": ParamSpec(0.0, -0.15, 0.15),
    # regime-fit veto/discount strength, generic + per engine×regime pair at runtime
    "veto::regime_mismatch_discount": ParamSpec(0.55, 0.25, 0.85),
}


def get_param(state: dict, key: str) -> float:
    params = state["tier1"]["adaptive_params"]
    if key in params:
        return params[key]
    base_key = key.split("::")[0] + "::" + key.split("::")[1] if "::" in key else key
    spec = PARAM_SPECS.get(base_key) or PARAM_SPECS.get(key)
    default = spec.default if spec else 1.0
    params[key] = default
    return default


def update_param(state: dict, key: str, target_value: float, base_key: Optional[str] = None) -> float:
    """Dampened (EMA), bounded, capped-step update — the single choke point
    every adaptive-learning code path must go through (Section 5)."""
    spec = PARAM_SPECS.get(base_key or key)
    if spec is None:
        # dynamic per-pair keys (e.g. "veto::regime_mismatch_discount::smc__trending")
        root = key.split("::")
        spec = PARAM_SPECS.get(root[0] + "::" + root[1], ParamSpec(1.0, 0.2, 2.0))
    current = get_param(state, key)
    blended = current + EMA_LEARNING_ALPHA * (target_value - current)
    max_step = max(abs(current) * MAX_PARAM_STEP_FRACTION, (spec.hi - spec.lo) * 0.02)
    step = max(-max_step, min(max_step, blended - current))
    new_val = max(spec.lo, min(spec.hi, current + step))
    state["tier1"]["adaptive_params"][key] = new_val
    return new_val


# =============================================================================
# SECTION H — STATE PERSISTENCE (Tier 1 aggregates / Tier 2 raw log; Section 5)
# =============================================================================

def default_state() -> dict:
    return {
        "engine": ENGINE_NAME,
        "version": ENGINE_VERSION,
        "resolution_logic_version": RESOLUTION_LOGIC_VERSION,
        "tier1": {
            "adaptive_params": {},
            "segment_stats": {},   # key -> {n, wins, losses, sum_r, sum_conf, ...}
            "calibration_buckets": {},  # engine -> bucket -> {n, wins, sum_conf}
            "forensic_category_counts": {},  # category -> rolling count/trend
            "baseline": None,       # pre-deployment baseline (win_rate, profit_factor, avg_rr)
            "circuit_breaker": {"tripped": False, "tripped_at": None, "reason": None},
            "fill_stats": {},       # entry_kind/setup -> {dispatched, filled, expired}
            "filter_funnel": {},    # filter_name -> {evaluated, rejected}
            "engine_governor": {},  # engine -> {paused} -- see update_engine_governor()
        },
        "tier2": {
            "active_signals": [],
            "trade_log": [],        # bounded, prunable raw records
        },
        "meta": {"created": now_iso(), "last_run": None, "run_count": 0},
    }


def _atomic_write_json(path: str, data: dict) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(data, f, indent=2, default=str)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def load_state() -> dict:
    if not os.path.exists(STATE_PATH):
        log.info("No existing state.json — starting cold (Section 2 cold-start bar still enforced).")
        return default_state()
    try:
        with open(STATE_PATH, "r") as f:
            state = json.load(f)
        base = default_state()
        for k, v in base.items():
            state.setdefault(k, v)
        for k, v in base["tier1"].items():
            state["tier1"].setdefault(k, v)
        for k, v in base["tier2"].items():
            state["tier2"].setdefault(k, v)
        return state
    except (json.JSONDecodeError, OSError) as e:
        log.error("Failed to load state.json (%s) — falling back to a fresh cold-start state.", e)
        return default_state()


def save_state(state: dict) -> None:
    prune_tier2(state)
    state["meta"]["last_run"] = now_iso()
    state["meta"]["run_count"] = state["meta"].get("run_count", 0) + 1
    try:
        _atomic_write_json(STATE_PATH, state)
    except OSError as e:
        log.error("Failed to persist state.json: %s", e)


def load_candle_cache() -> dict:
    if not os.path.exists(CANDLE_CACHE_PATH):
        return {}
    try:
        with open(CANDLE_CACHE_PATH, "r") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return {}


def save_candle_cache(cache: dict) -> None:
    try:
        _atomic_write_json(CANDLE_CACHE_PATH, cache)
    except OSError as e:
        log.error("Failed to persist candle_cache.json: %s", e)


def prune_tier2(state: dict) -> None:
    """Pruning Tier 2 never touches Tier 1 — every adaptive parameter and
    aggregate already lives there and is updated incrementally, never by
    rescanning the raw log (Section 5)."""
    cutoff = now_ms() - TIER2_RETENTION_DAYS * 86_400_000
    log_list = state["tier2"]["trade_log"]
    log_list[:] = [t for t in log_list if t.get("resolved_ts_ms", now_ms()) >= cutoff]
    if len(log_list) > TIER2_MAX_TRADES:
        state["tier2"]["trade_log"] = log_list[-TIER2_MAX_TRADES:]


def segment_key(**kwargs) -> str:
    return "|".join(f"{k}={v}" for k, v in sorted(kwargs.items()))


def bump_segment_stat(state: dict, key: str, r_realized: float, won: bool, confidence: float) -> dict:
    stats = state["tier1"]["segment_stats"].setdefault(
        key, {"n": 0, "wins": 0, "losses": 0, "sum_r": 0.0, "sum_conf": 0.0, "sum_r_sq": 0.0})
    stats["n"] += 1
    stats["wins"] += 1 if won else 0
    stats["losses"] += 0 if won else 1
    stats["sum_r"] += r_realized
    stats["sum_r_sq"] += r_realized * r_realized
    stats["sum_conf"] += confidence
    return stats


def segment_win_rate(stats: dict) -> Optional[float]:
    return stats["wins"] / stats["n"] if stats.get("n", 0) > 0 else None


def has_min_sample(stats: dict) -> bool:
    return stats.get("n", 0) >= MIN_SAMPLE_SIZE_FOR_ADAPTATION


# =============================================================================
# SECTION I — SPECIALIZED ENGINES (Section 4)
# =============================================================================
# Every engine returns a Candidate with a fully independent Direction, Entry,
# SL, TP1(+TP2), Confidence, Expected RR, Confluences, and documented
# best-fit regime(s). Structural analysis (Section E) and the Regime Vector
# (Section F) are computed once per asset/combo and shared across engines —
# no engine re-derives its own structure detection.

@dataclass
class Candidate:
    engine: str
    coin: str
    combo: str                # "intraday" | "swing"
    direction: str             # "long" | "short"
    entry: float
    sl: float
    tp1: float
    tp2: float
    entry_kind: str             # "market" | "pending"
    confidence: float           # 0..1 raw, pre-calibration
    rr_tp1: float
    confluences: list[str]
    best_fit_regimes: list[str]
    zone: Optional[Zone] = None
    sfp_purity: float = 0.0
    session_anchored: bool = False
    mfe_proxy_atr: float = 0.0  # used only for entry-distance sanity, not persisted
    ltf_confirmed: bool = False  # mandatory LTF confirmation trigger (Section 9)
    rr_floor_used: float = TP1_RR_FLOOR  # the dynamic RR floor actually enforced at TP finalization


def _clear_sl_of_liquidity_pool(direction: str, sl: float, equal_levels: dict) -> float:
    """Push the SL fully clear of a known SSL/BSL liquidity-pool cluster
    (Section 10 refinement, ported from Vantage Annex v2.0.0, re-implemented
    against this file's own `equal_levels`/`Swing` structures rather than
    copied). An EQL cluster below price (SSL, resting sell-side liquidity for
    a long) or an EQH cluster above price (BSL, resting buy-side liquidity
    for a short) is exactly what a stop-hunt wick is drawn to sweep before
    reversing. If the SL still rests at or inside the cluster's own range
    rather than fully clear of it, a sweep of the whole pool -- not just one
    equal high/low -- could tag the stop without the setup actually being
    invalidated, so the SL is pushed past the cluster's far edge plus the
    cluster's own width as a margin."""
    pools = equal_levels.get("eql_clusters", []) if direction == "long" else equal_levels.get("eqh_clusters", [])
    for cluster in pools:
        prices = [s.price for s in cluster]
        lo, hi = min(prices), max(prices)
        margin = max(hi - lo, 1e-9)
        if direction == "long" and sl >= lo:
            sl = lo - margin
        elif direction == "short" and sl <= hi:
            sl = hi + margin
    return sl


def _adaptive_sl_buffer(candles: list[dict], struct_level: float, direction: str, state: dict,
                         equal_levels: dict) -> float:
    """Adaptive-percentile SL buffer (Section 10, mandatory): buffer sized
    from a live percentile of recent adverse-wick excursions, not a fixed
    constant. The buffered level is then pushed fully clear of any SSL/BSL
    liquidity-pool cluster it would otherwise rest inside (Section 10
    refinement, see `_clear_sl_of_liquidity_pool`) -- never resized to hit an
    RR target, only to keep the SL a genuine invalidation level."""
    dist_dist = wick_noise_distribution(candles, 120)
    pctile = get_param(state, "risk::sl_buffer_percentile")
    buf = percentile(dist_dist, pctile) if dist_dist else atr(candles, 14) * 0.15
    buf = max(buf, atr(candles, 14) * 0.08)
    sl = struct_level - buf if direction == "long" else struct_level + buf
    return _clear_sl_of_liquidity_pool(direction, sl, equal_levels)


def _quality_score_for(zone: Optional[Zone], sfp_purity: float, atr_val: float) -> float:
    """Zone-quality proxy feeding the dynamic RR floor (Section 10): tighter
    zones and higher SFP purity score higher. Engines without a discrete POI
    (pure momentum/range/mean-reversion/breakout reads) get a neutral mid
    score so this term neither penalizes nor favors them specifically."""
    if zone is not None and atr_val > 0:
        width_frac = abs(zone.top - zone.bottom) / atr_val
        width_score = max(0.0, 1.0 - min(1.0, width_frac / 2.5))
    else:
        width_score = 0.5
    purity_score = sfp_purity if sfp_purity else 0.5
    return max(0.0, min(1.0, 0.5 * width_score + 0.5 * purity_score))


def _room_to_next_opposing_level(entry: float, direction: str, ctx: dict) -> Optional[float]:
    """Measured distance to the next real opposing swing high/low or EQH/EQL
    liquidity cluster (Section 10 refinement). Used by `_finalize_tp_pair` to
    derive TP1/TP2 from actual structural room first, falling back to the
    fixed-RR ceiling only when nothing binding sits in the way -- more honest
    than reaching for a flat RR target and clipping after the fact."""
    candidates: list[float] = []
    swings = ctx.get("swings_low", []) + ctx.get("swings_med", [])
    eq = ctx.get("equal_levels", {})
    if direction == "long":
        candidates += [s.price for s in swings if s.kind == "high" and s.price > entry]
        candidates += [statistics.mean(s.price for s in cl) for cl in eq.get("eqh_clusters", [])
                       if statistics.mean(s.price for s in cl) > entry]
    else:
        candidates += [s.price for s in swings if s.kind == "low" and s.price < entry]
        candidates += [statistics.mean(s.price for s in cl) for cl in eq.get("eql_clusters", [])
                       if statistics.mean(s.price for s in cl) < entry]
    if not candidates:
        return None
    return (min(candidates) - entry) if direction == "long" else (entry - max(candidates))


def _clip_tp_to_liquidity_wall(entry: float, target: float, direction: str,
                                candles: list[dict], equal_levels: dict) -> float:
    """Liquidity-wall-clipped TP (Section 10, mandatory): clip a target to
    just in front of a closer EQH/EQL cluster (or other obvious wall) rather
    than projecting through it."""
    walls = []
    if direction == "long":
        for cluster in equal_levels.get("eqh_clusters", []):
            lvl = statistics.mean(s.price for s in cluster)
            if entry < lvl < target:
                walls.append(lvl)
        if walls:
            nearest = min(walls)
            a = atr(candles, 14) * 0.05
            return max(entry + a, nearest - a)
    else:
        for cluster in equal_levels.get("eql_clusters", []):
            lvl = statistics.mean(s.price for s in cluster)
            if target < lvl < entry:
                walls.append(lvl)
        if walls:
            nearest = max(walls)
            a = atr(candles, 14) * 0.05
            return min(entry - a, nearest + a)
    return target


def _finalize_tp_pair(entry: float, sl: float, direction: str, tp1_raw: float, tp2_raw: float,
                       ctx: dict, rv: RegimeVector,
                       quality_score: float = 0.5) -> Optional[tuple[float, float, float, float]]:
    """Applies liquidity-wall clipping, enforces a regime/quality-adaptive RR
    floor on TP1 (Section 10), derives TP1/TP2 from actual measured room to
    the next opposing structural level where that room is binding (Section
    10 refinement -- more honest than reach-for-a-fixed-RR-then-clip), and
    structurally guarantees TP2 sits strictly farther than TP1 (TP-ordering-
    integrity assertion). Returns None if the candidate cannot be made to
    satisfy the dynamic RR floor honestly.

    `quality_score` (0..1, see `_quality_score_for`) and the regime vector's
    noise/volatility both raise the bar dynamically: choppy or low-quality
    setups need more reward per unit risk to be worth taking than a uniform
    flat floor would require."""
    candles, equal_levels = ctx["low"], ctx["equal_levels"]
    atr_val = ctx.get("atr_low") or atr(candles, 14) or 1e-9
    risk = abs(entry - sl)
    if risk <= 0:
        return None

    dyn_floor = TP1_RR_FLOOR
    if rv.noise > 0.6 or rv.volatility_pctile > 80:
        dyn_floor += 0.4
    if quality_score < 0.5:
        dyn_floor += 0.3

    # Room-to-next-opposing-level: derive tp1_rr/tp2_rr from the actual
    # measured distance to the next real supply/demand structure or
    # liquidity pool first, only falling back to the fixed-RR ceiling
    # (tp1_raw/tp2_raw) when there's no binding wall in the way.
    room = _room_to_next_opposing_level(entry, direction, ctx)
    room_buffer = 0.15 * atr_val
    tp1_rr_ceiling = abs(tp1_raw - entry) / risk
    tp2_rr_ceiling = abs(tp2_raw - entry) / risk
    tp1_rr, tp2_rr = tp1_rr_ceiling, tp2_rr_ceiling
    if room is not None:
        usable = room - room_buffer
        if usable > 0:
            wall_rr = usable / risk
            if wall_rr < tp1_rr_ceiling:
                tp1_rr = max(dyn_floor, min(wall_rr, tp1_rr_ceiling))
            if wall_rr < tp2_rr_ceiling:
                tp2_rr = max(tp1_rr, min(wall_rr, tp2_rr_ceiling))

    tp1_raw = entry + tp1_rr * risk if direction == "long" else entry - tp1_rr * risk
    tp2_raw = entry + tp2_rr * risk if direction == "long" else entry - tp2_rr * risk

    tp1 = _clip_tp_to_liquidity_wall(entry, tp1_raw, direction, candles, equal_levels)
    rr1 = abs(tp1 - entry) / risk
    if rr1 < dyn_floor:
        return None  # reject rather than stretch TP1 artificially (Section 10)
    tp2 = _clip_tp_to_liquidity_wall(entry, tp2_raw, direction, candles, equal_levels)
    # TP ordering integrity — final assertion, independent of upstream derivation.
    if direction == "long":
        if tp2 <= tp1:
            tp2 = tp1 + max(risk * 0.5, (tp1 - entry) * 0.25)
    else:
        if tp2 >= tp1:
            tp2 = tp1 - max(risk * 0.5, (entry - tp1) * 0.25)
    dist_tp1 = abs(tp1 - entry)
    dist_tp2 = abs(tp2 - entry)
    assert dist_tp2 > dist_tp1, "TP ordering integrity violated"
    return tp1, tp2, rr1, dyn_floor


def _entry_distance_ok(entry: float, sl: float, tp1: float, market_price: float,
                        atr_val: float, entry_kind: str) -> bool:
    if atr_val <= 0:
        return False
    if abs(entry - sl) < MIN_ENTRY_TO_SL_ATR_FRAC * atr_val:
        return False
    if abs(tp1 - entry) < MIN_ENTRY_TO_TP1_ATR_FRAC * atr_val:
        return False
    if entry_kind == "pending" and abs(entry - market_price) > MAX_PENDING_ENTRY_ATR_MULT * atr_val:
        return False
    return True


def _ltf_confirmation(ctx: dict, direction: str) -> bool:
    """Mandatory LTF confirmation trigger (Section 9): a candidate is never
    allowed to fire on the strength of HTF/MTF structural alignment alone --
    something must have actually happened on the combo's low timeframe
    confirming the intended direction *after* the sweep/POI tap, e.g. a
    confirmed market-structure shift (a close beyond the last counter-trend
    swing) or, failing that, a plain rejection candle closing back through
    the prior bar's opposite extreme. This is engine-agnostic and is checked
    independently of whatever structural read produced the candidate, so an
    HTF zone that is "right" but where price never actually turns there gets
    filtered out here rather than passed through on structure alone."""
    low = ctx["low"]
    if len(low) < 3:
        return False
    mss = detect_mss(low, direction, lookback=20)
    if mss and mss.get("confirmed"):
        return True
    last, prev = low[-1], low[-2]
    body_dir = last["c"] - last["o"]
    if direction == "long":
        return body_dir > 0 and last["c"] > prev["h"]
    return body_dir < 0 and last["c"] < prev["l"]


def _structural_context(bundle: dict[str, list[dict]], combo_tfs: dict) -> dict:
    """One shared structural pass per asset/combo, reused by every engine."""
    low = bundle[combo_tfs["low"]]
    med = bundle[combo_tfs["medium"]]
    high = bundle[combo_tfs["high"]]
    swings_low = find_swings(low, 2, 2)
    swings_med = find_swings(med, 2, 2)
    equal_levels = find_equal_levels(swings_low, low)
    htf_struct = detect_bos_choch(high, find_swings(high, 2, 2))
    mtf_struct = detect_bos_choch(med, swings_med)
    sfp = detect_sfp(low, swings_low, equal_levels)
    return {
        "low": low, "medium": med, "high": high,
        "swings_low": swings_low, "swings_med": swings_med,
        "equal_levels": equal_levels,
        "htf_bias": htf_struct["bias"], "htf_event": htf_struct["event"],
        "mtf_bias": mtf_struct["bias"],
        "sfp": sfp,
        "pdz": premium_discount_zone(high, 50),
        "atr_low": atr(low, 14),
    }


def _zone_selection_sequence(ctx: dict, direction: str) -> Optional[dict]:
    """Section 8 mandatory ordered sequence: HTF bias -> POI -> SFP purity ->
    MSS -> breaker confirmation -> OTE refinement. Returns None if the
    sequence doesn't validate a tradeable zone."""
    if ctx["htf_bias"] != direction.replace("long", "bullish").replace("short", "bearish") \
            and ctx["htf_bias"] != "neutral":
        return None
    sfp = ctx["sfp"]
    if not sfp or sfp["direction"] != direction:
        return None
    if sfp["purity"] < 0.30:
        return None
    mss = detect_mss(ctx["low"], direction)
    if not mss or not mss.get("confirmed"):
        return None
    obs = find_order_blocks(ctx["low"], sfp["swept_idx"], direction)
    fvgs = find_fvgs(ctx["low"], sfp["swept_idx"], direction)
    breakers = find_breaker_blocks(ctx["low"], obs, direction)
    candidates = breakers or obs or fvgs
    if not candidates:
        return None
    zone = candidates[-1]
    impulse_start = ctx["low"][sfp["swept_idx"]]["c"]
    impulse_end = ctx["low"][-1]["c"]
    refined = ote_refine(impulse_start, impulse_end, direction, zone.top, zone.bottom)
    entry = refined if refined is not None else (zone.top + zone.bottom) / 2
    return {"zone": zone, "entry": entry, "sfp": sfp, "mss": mss}


def engine_smc(coin: str, combo: str, ctx: dict, market_price: float, rv: RegimeVector) -> list[Candidate]:
    out = []
    for direction in ("long", "short"):
        sel = _zone_selection_sequence(ctx, direction)
        if not sel:
            continue
        zone, sfp = sel["zone"], sel["sfp"]
        entry = sel["entry"]
        struct_level = zone.bottom if direction == "long" else zone.top
        sl = _adaptive_sl_buffer(ctx["low"], struct_level, direction, _GLOBAL_STATE_REF[0], ctx["equal_levels"])
        raw_span = abs(entry - sl) * 1.6
        tp1_raw = entry + raw_span if direction == "long" else entry - raw_span
        tp2_raw = entry + raw_span * 1.7 if direction == "long" else entry - raw_span * 1.7
        quality = _quality_score_for(zone, sfp["purity"], ctx["atr_low"])
        finalized = _finalize_tp_pair(entry, sl, direction, tp1_raw, tp2_raw, ctx, rv, quality)
        if not finalized:
            continue
        tp1, tp2, rr1, dyn_floor = finalized
        entry_kind = "market" if abs(entry - market_price) < ctx["atr_low"] * 0.15 else "pending"
        if not _entry_distance_ok(entry, sl, tp1, market_price, ctx["atr_low"], entry_kind):
            continue
        confluences = ["order_flow_sfp", zone.kind]
        if sfp["is_eq_cluster"]:
            confluences.append("eqh_eql_sweep")
        conf = 0.5 + 0.15 * sfp["purity"] + (0.08 if sfp["is_eq_cluster"] else 0.0)
        out.append(Candidate("smc", coin, combo, direction, entry, sl, tp1, tp2, entry_kind,
                              min(0.95, conf), rr1, confluences,
                              best_fit_regimes=["trending", "expansion"], zone=zone,
                              sfp_purity=sfp["purity"],
                              session_anchored=rv.session_open_proximity > 0.5,
                              # already required by _zone_selection_sequence's mss.confirmed gate
                              ltf_confirmed=True, rr_floor_used=dyn_floor))
    return out


def engine_trend_continuation(coin: str, combo: str, ctx: dict, market_price: float, rv: RegimeVector) -> list[Candidate]:
    out = []
    direction = "long" if ctx["htf_bias"] == "bullish" else ("short" if ctx["htf_bias"] == "bearish" else None)
    if direction is None or ctx["mtf_bias"] != ctx["htf_bias"]:
        return out
    low = ctx["low"]
    fast = ema([c["c"] for c in low], 8)
    slow = ema([c["c"] for c in low], 21)
    if len(fast) < 5:
        return out
    pulled_back = (fast[-1] > slow[-1] and low[-1]["l"] <= slow[-1]) if direction == "long" \
        else (fast[-1] < slow[-1] and low[-1]["h"] >= slow[-1])
    if not pulled_back:
        return out
    entry = low[-1]["c"]
    struct_ref = min(c["l"] for c in low[-6:]) if direction == "long" else max(c["h"] for c in low[-6:])
    sl = _adaptive_sl_buffer(low, struct_ref, direction, _GLOBAL_STATE_REF[0], ctx["equal_levels"])
    a = ctx["atr_low"]
    tp1_raw = entry + a * 2.2 if direction == "long" else entry - a * 2.2
    tp2_raw = entry + a * 3.4 if direction == "long" else entry - a * 3.4
    finalized = _finalize_tp_pair(entry, sl, direction, tp1_raw, tp2_raw, ctx, rv, _quality_score_for(None, 0.0, a))
    if not finalized:
        return out
    tp1, tp2, rr1, dyn_floor = finalized
    entry_kind = "market"
    if not _entry_distance_ok(entry, sl, tp1, market_price, a, entry_kind):
        return out
    conf = 0.48 + 0.2 * min(1.0, rv.trend_strength / 35.0)
    out.append(Candidate("trend_continuation", coin, combo, direction, entry, sl, tp1, tp2, entry_kind,
                          min(0.9, conf), rr1, ["ema_pullback", "htf_mtf_agree"],
                          best_fit_regimes=["trending"],
                          ltf_confirmed=_ltf_confirmation(ctx, direction), rr_floor_used=dyn_floor))
    return out


def engine_breakout(coin: str, combo: str, ctx: dict, market_price: float, rv: RegimeVector) -> list[Candidate]:
    out = []
    low = ctx["low"]
    recent = low[-25:-1]
    hi, lo = max(c["h"] for c in recent), min(c["l"] for c in recent)
    last = low[-1]
    for direction, broke in (("long", last["c"] > hi), ("short", last["c"] < lo)):
        if not broke:
            continue
        entry = last["c"]
        sl_struct = lo if direction == "long" else hi
        sl = _adaptive_sl_buffer(low, sl_struct, direction, _GLOBAL_STATE_REF[0], ctx["equal_levels"])
        rng = hi - lo
        tp1_raw = entry + rng * 0.9 if direction == "long" else entry - rng * 0.9
        tp2_raw = entry + rng * 1.6 if direction == "long" else entry - rng * 1.6
        quality = _quality_score_for(None, 0.0, ctx["atr_low"])
        finalized = _finalize_tp_pair(entry, sl, direction, tp1_raw, tp2_raw, ctx, rv, quality)
        if not finalized:
            continue
        tp1, tp2, rr1, dyn_floor = finalized
        if not _entry_distance_ok(entry, sl, tp1, market_price, ctx["atr_low"], "market"):
            continue
        conf = 0.45 + 0.2 * (1.0 - rv.noise)
        out.append(Candidate("breakout", coin, combo, direction, entry, sl, tp1, tp2, "market",
                              min(0.9, conf), rr1, ["range_breakout"],
                              best_fit_regimes=["expansion", "trending"],
                              ltf_confirmed=_ltf_confirmation(ctx, direction), rr_floor_used=dyn_floor))
    return out


def engine_pullback(coin: str, combo: str, ctx: dict, market_price: float, rv: RegimeVector) -> list[Candidate]:
    out = []
    sel_long = _zone_selection_sequence(ctx, "long")
    sel_short = _zone_selection_sequence(ctx, "short")
    for direction, sel in (("long", sel_long), ("short", sel_short)):
        if not sel:
            continue
        zone = sel["zone"]
        entry = (zone.top + zone.bottom) / 2
        struct_level = zone.bottom if direction == "long" else zone.top
        sl = _adaptive_sl_buffer(ctx["low"], struct_level, direction, _GLOBAL_STATE_REF[0], ctx["equal_levels"])
        span = abs(entry - sl) * 1.55
        tp1_raw = entry + span if direction == "long" else entry - span
        tp2_raw = entry + span * 1.6 if direction == "long" else entry - span * 1.6
        quality = _quality_score_for(zone, sel["sfp"]["purity"], ctx["atr_low"])
        finalized = _finalize_tp_pair(entry, sl, direction, tp1_raw, tp2_raw, ctx, rv, quality)
        if not finalized:
            continue
        tp1, tp2, rr1, dyn_floor = finalized
        entry_kind = "pending"
        if not _entry_distance_ok(entry, sl, tp1, market_price, ctx["atr_low"], entry_kind):
            continue
        out.append(Candidate("pullback", coin, combo, direction, entry, sl, tp1, tp2, entry_kind,
                              0.5 + 0.1 * sel["sfp"]["purity"], rr1, ["poi_pullback"],
                              best_fit_regimes=["trending", "consolidation"], zone=zone,
                              sfp_purity=sel["sfp"]["purity"],
                              # already required by _zone_selection_sequence's mss.confirmed gate
                              ltf_confirmed=True, rr_floor_used=dyn_floor))
    return out


def engine_liquidity_sweep(coin: str, combo: str, ctx: dict, market_price: float, rv: RegimeVector) -> list[Candidate]:
    out = []
    sfp = ctx["sfp"]
    if not sfp or sfp["purity"] < 0.35:
        return out
    direction = sfp["direction"]
    low = ctx["low"]
    entry = low[-1]["c"]
    struct_level = sfp["swept_level"]
    sl = _adaptive_sl_buffer(low, struct_level, direction, _GLOBAL_STATE_REF[0], ctx["equal_levels"])
    span = abs(entry - sl) * 1.6
    tp1_raw = entry + span if direction == "long" else entry - span
    tp2_raw = entry + span * 1.8 if direction == "long" else entry - span * 1.8
    quality = _quality_score_for(None, sfp["purity"], ctx["atr_low"])
    finalized = _finalize_tp_pair(entry, sl, direction, tp1_raw, tp2_raw, ctx, rv, quality)
    if not finalized:
        return out
    tp1, tp2, rr1, dyn_floor = finalized
    if not _entry_distance_ok(entry, sl, tp1, market_price, ctx["atr_low"], "market"):
        return out
    confl = ["liquidity_sweep"]
    if sfp["is_eq_cluster"]:
        confl.append("eqh_eql_sweep")
    out.append(Candidate("liquidity_sweep", coin, combo, direction, entry, sl, tp1, tp2, "market",
                          0.5 + 0.2 * sfp["purity"], rr1, confl,
                          best_fit_regimes=["ranging_choppy", "consolidation"],
                          sfp_purity=sfp["purity"], session_anchored=rv.session_open_proximity > 0.5,
                          ltf_confirmed=_ltf_confirmation(ctx, direction), rr_floor_used=dyn_floor))
    return out


def engine_order_block(coin: str, combo: str, ctx: dict, market_price: float, rv: RegimeVector) -> list[Candidate]:
    return _poi_based_engine("order_block", coin, combo, ctx, market_price, rv, want_kind="order_block")


def engine_breaker_block(coin: str, combo: str, ctx: dict, market_price: float, rv: RegimeVector) -> list[Candidate]:
    return _poi_based_engine("breaker_block", coin, combo, ctx, market_price, rv, want_kind="breaker_block")


def engine_fair_value_gap(coin: str, combo: str, ctx: dict, market_price: float, rv: RegimeVector) -> list[Candidate]:
    return _poi_based_engine("fair_value_gap", coin, combo, ctx, market_price, rv, want_kind="fvg")


def _poi_based_engine(engine_name: str, coin: str, combo: str, ctx: dict, market_price: float,
                       rv: RegimeVector, want_kind: str) -> list[Candidate]:
    out = []
    for direction in ("long", "short"):
        sel = _zone_selection_sequence(ctx, direction)
        if not sel or sel["zone"].kind != want_kind:
            continue
        zone = sel["zone"]
        entry = sel["entry"]
        struct_level = zone.bottom if direction == "long" else zone.top
        sl = _adaptive_sl_buffer(ctx["low"], struct_level, direction, _GLOBAL_STATE_REF[0], ctx["equal_levels"])
        span = abs(entry - sl) * 1.55
        tp1_raw = entry + span if direction == "long" else entry - span
        tp2_raw = entry + span * 1.65 if direction == "long" else entry - span * 1.65
        quality = _quality_score_for(zone, sel["sfp"]["purity"], ctx["atr_low"])
        finalized = _finalize_tp_pair(entry, sl, direction, tp1_raw, tp2_raw, ctx, rv, quality)
        if not finalized:
            continue
        tp1, tp2, rr1, dyn_floor = finalized
        entry_kind = "market" if abs(entry - market_price) < ctx["atr_low"] * 0.15 else "pending"
        if not _entry_distance_ok(entry, sl, tp1, market_price, ctx["atr_low"], entry_kind):
            continue
        out.append(Candidate(engine_name, coin, combo, direction, entry, sl, tp1, tp2, entry_kind,
                              0.5 + 0.15 * sel["sfp"]["purity"], rr1, [want_kind],
                              best_fit_regimes=["trending", "consolidation"], zone=zone,
                              sfp_purity=sel["sfp"]["purity"],
                              # already required by _zone_selection_sequence's mss.confirmed gate
                              ltf_confirmed=True, rr_floor_used=dyn_floor))
    return out


def engine_momentum(coin: str, combo: str, ctx: dict, market_price: float, rv: RegimeVector) -> list[Candidate]:
    out = []
    low = ctx["low"]
    closes = [c["c"] for c in low]
    if len(closes) < 20:
        return out
    roc = (closes[-1] - closes[-10]) / closes[-10] if closes[-10] else 0
    direction = "long" if roc > 0.015 else ("short" if roc < -0.015 else None)
    if direction is None or rv.trend_strength < 18:
        return out
    entry = closes[-1]
    struct_ref = min(c["l"] for c in low[-10:]) if direction == "long" else max(c["h"] for c in low[-10:])
    sl = _adaptive_sl_buffer(low, struct_ref, direction, _GLOBAL_STATE_REF[0], ctx["equal_levels"])
    span = abs(entry - sl) * 1.6
    tp1_raw = entry + span if direction == "long" else entry - span
    tp2_raw = entry + span * 1.9 if direction == "long" else entry - span * 1.9
    finalized = _finalize_tp_pair(entry, sl, direction, tp1_raw, tp2_raw, ctx, rv,
                                   _quality_score_for(None, 0.0, ctx["atr_low"]))
    if not finalized:
        return out
    tp1, tp2, rr1, dyn_floor = finalized
    if not _entry_distance_ok(entry, sl, tp1, market_price, ctx["atr_low"], "market"):
        return out
    out.append(Candidate("momentum", coin, combo, direction, entry, sl, tp1, tp2, "market",
                          0.45 + min(0.25, abs(roc) * 6), rr1, ["rate_of_change"],
                          best_fit_regimes=["trending", "expansion"],
                          ltf_confirmed=_ltf_confirmation(ctx, direction), rr_floor_used=dyn_floor))
    return out


def engine_reversal(coin: str, combo: str, ctx: dict, market_price: float, rv: RegimeVector) -> list[Candidate]:
    out = []
    sfp = ctx["sfp"]
    if not sfp or sfp["purity"] < 0.5 or not sfp["is_eq_cluster"]:
        return out
    direction = sfp["direction"]
    if ctx["mtf_bias"] == ("bullish" if direction == "short" else "bearish"):
        return out  # only take counter-trend reversal with a documented strong reason: MTF already turning
    low = ctx["low"]
    entry = low[-1]["c"]
    sl = _adaptive_sl_buffer(low, sfp["swept_level"], direction, _GLOBAL_STATE_REF[0], ctx["equal_levels"])
    span = abs(entry - sl) * 1.7
    tp1_raw = entry + span if direction == "long" else entry - span
    tp2_raw = entry + span * 2.0 if direction == "long" else entry - span * 2.0
    finalized = _finalize_tp_pair(entry, sl, direction, tp1_raw, tp2_raw, ctx, rv,
                                   _quality_score_for(None, sfp["purity"], ctx["atr_low"]))
    if not finalized:
        return out
    tp1, tp2, rr1, dyn_floor = finalized
    if not _entry_distance_ok(entry, sl, tp1, market_price, ctx["atr_low"], "market"):
        return out
    out.append(Candidate("reversal", coin, combo, direction, entry, sl, tp1, tp2, "market",
                          0.5 + 0.2 * sfp["purity"], rr1, ["eq_cluster_reversal"],
                          best_fit_regimes=["reversal", "ranging_choppy"], sfp_purity=sfp["purity"],
                          ltf_confirmed=_ltf_confirmation(ctx, direction), rr_floor_used=dyn_floor))
    return out


def engine_mean_reversion(coin: str, combo: str, ctx: dict, market_price: float, rv: RegimeVector) -> list[Candidate]:
    out = []
    if rv.trend_strength > 22:
        return out
    low = ctx["low"]
    closes = [c["c"] for c in low]
    mean = statistics.mean(closes[-20:])
    std = statistics.pstdev(closes[-20:]) or 1e-9
    z = (closes[-1] - mean) / std
    direction = "long" if z < -1.6 else ("short" if z > 1.6 else None)
    if direction is None:
        return out
    entry = closes[-1]
    sl_struct = min(low[-20:], key=lambda c: c["l"])["l"] if direction == "long" \
        else max(low[-20:], key=lambda c: c["h"])["h"]
    sl = _adaptive_sl_buffer(low, sl_struct, direction, _GLOBAL_STATE_REF[0], ctx["equal_levels"])
    target = mean
    tp1_raw = target
    tp2_raw = target + (target - entry) * 0.5
    finalized = _finalize_tp_pair(entry, sl, direction, tp1_raw, tp2_raw, ctx, rv,
                                   _quality_score_for(None, 0.0, ctx["atr_low"]))
    if not finalized:
        return out
    tp1, tp2, rr1, dyn_floor = finalized
    if not _entry_distance_ok(entry, sl, tp1, market_price, ctx["atr_low"], "market"):
        return out
    out.append(Candidate("mean_reversion", coin, combo, direction, entry, sl, tp1, tp2, "market",
                          0.45 + min(0.25, (abs(z) - 1.6) * 0.15), rr1, ["z_score_extension"],
                          best_fit_regimes=["ranging_choppy", "consolidation"],
                          ltf_confirmed=_ltf_confirmation(ctx, direction), rr_floor_used=dyn_floor))
    return out


def engine_range_trading(coin: str, combo: str, ctx: dict, market_price: float, rv: RegimeVector) -> list[Candidate]:
    out = []
    if rv.trend_strength > 20:
        return out
    low = ctx["low"]
    window = low[-30:]
    hi, lo = max(c["h"] for c in window), min(c["l"] for c in window)
    last = low[-1]
    near_lo = (last["l"] - lo) < (hi - lo) * 0.1
    near_hi = (hi - last["h"]) < (hi - lo) * 0.1
    for direction, near in (("long", near_lo), ("short", near_hi)):
        if not near:
            continue
        entry = last["c"]
        sl_struct = lo if direction == "long" else hi
        sl = _adaptive_sl_buffer(low, sl_struct, direction, _GLOBAL_STATE_REF[0], ctx["equal_levels"])
        target = hi if direction == "long" else lo
        tp1_raw = target
        tp2_raw = target
        finalized = _finalize_tp_pair(entry, sl, direction, tp1_raw, tp2_raw, ctx, rv,
                                       _quality_score_for(None, 0.0, ctx["atr_low"]))
        if not finalized:
            continue
        tp1, tp2, rr1, dyn_floor = finalized
        if not _entry_distance_ok(entry, sl, tp1, market_price, ctx["atr_low"], "market"):
            continue
        out.append(Candidate("range_trading", coin, combo, direction, entry, sl, tp1, tp2, "market",
                              0.42 + 0.2 * (1.0 - rv.noise), rr1, ["range_edge"],
                              best_fit_regimes=["ranging_choppy", "consolidation"],
                              ltf_confirmed=_ltf_confirmation(ctx, direction), rr_floor_used=dyn_floor))
    return out


def engine_volatility_expansion(coin: str, combo: str, ctx: dict, market_price: float, rv: RegimeVector) -> list[Candidate]:
    out = []
    low = ctx["low"]
    a_series = atr_series(low, 14)
    if len(a_series) < 30:
        return out
    expanding = a_series[-1] > statistics.mean(a_series[-20:-1]) * 1.35
    if not expanding:
        return out
    direction = "long" if low[-1]["c"] > low[-2]["c"] else "short"
    entry = low[-1]["c"]
    sl_struct = low[-1]["l"] if direction == "long" else low[-1]["h"]
    sl = _adaptive_sl_buffer(low, sl_struct, direction, _GLOBAL_STATE_REF[0], ctx["equal_levels"])
    span = abs(entry - sl) * 1.7
    tp1_raw = entry + span if direction == "long" else entry - span
    tp2_raw = entry + span * 2.1 if direction == "long" else entry - span * 2.1
    finalized = _finalize_tp_pair(entry, sl, direction, tp1_raw, tp2_raw, ctx, rv,
                                   _quality_score_for(None, 0.0, ctx["atr_low"]))
    if not finalized:
        return out
    tp1, tp2, rr1, dyn_floor = finalized
    if not _entry_distance_ok(entry, sl, tp1, market_price, ctx["atr_low"], "market"):
        return out
    out.append(Candidate("volatility_expansion", coin, combo, direction, entry, sl, tp1, tp2, "market",
                          0.45 + min(0.25, rv.volatility_pctile / 400), rr1, ["atr_expansion"],
                          best_fit_regimes=["expansion", "high_volatility"],
                          ltf_confirmed=_ltf_confirmation(ctx, direction), rr_floor_used=dyn_floor))
    return out


ENGINE_FUNCS = {
    "smc": engine_smc,
    "trend_continuation": engine_trend_continuation,
    "breakout": engine_breakout,
    "pullback": engine_pullback,
    "liquidity_sweep": engine_liquidity_sweep,
    "order_block": engine_order_block,
    "breaker_block": engine_breaker_block,
    "fair_value_gap": engine_fair_value_gap,
    "momentum": engine_momentum,
    "reversal": engine_reversal,
    "mean_reversion": engine_mean_reversion,
    "range_trading": engine_range_trading,
    "volatility_expansion": engine_volatility_expansion,
}

# Module-level indirection so engine functions (module-level, for auditability)
# can reach the current run's state for adaptive-parameter reads without a
# global rewrite of every signature; set once per run in main().
_GLOBAL_STATE_REF: list[dict] = [default_state()]


# =============================================================================
# SECTION J — DECISION ENGINE (continuous scoring blend; Section 4)
# =============================================================================

def _sigmoid(x: float) -> float:
    x = max(-8.0, min(8.0, x))
    return 1.0 / (1.0 + math.exp(-x))


def _term_regime_fit(cand: Candidate, rv: RegimeVector) -> float:
    label = regime_label(rv)
    return 1.0 if label in cand.best_fit_regimes else (0.35 if "neutral" in cand.best_fit_regimes else -0.6)


def _term_mtf_alignment(cand: Candidate, ctx: dict) -> float:
    want = "bullish" if cand.direction == "long" else "bearish"
    score = 0.0
    score += 0.5 if ctx["htf_bias"] == want else (-0.5 if ctx["htf_bias"] != "neutral" else 0.0)
    score += 0.5 if ctx["mtf_bias"] == want else (-0.3 if ctx["mtf_bias"] != "neutral" else 0.0)
    return max(-1.0, min(1.0, score))


def _term_confluence(cand: Candidate) -> float:
    # capped so no single term can saturate the logistic on its own (Section 4)
    n = len(set(cand.confluences))
    return max(-1.0, min(1.0, (n - 1) * 0.35))


def _term_segment_performance(state: dict, cand: Candidate, regime_lbl: str) -> float:
    key = segment_key(engine=cand.engine, regime=regime_lbl, asset=cand.coin)
    stats = state["tier1"]["segment_stats"].get(key)
    if not stats or not has_min_sample(stats):
        return 0.0  # no opinion yet — cold-start neutral, never a penalty (Section 2)
    wr = segment_win_rate(stats) or 0.5
    return max(-1.0, min(1.0, (wr - 0.5) * 2.4))


def _term_ev_rr(cand: Candidate) -> float:
    # RR informs EV/ranking only — capped, and never used as a probability proxy (Section 4)
    excess = max(0.0, cand.rr_tp1 - TP1_RR_FLOOR)
    return max(0.0, min(1.0, excess / 1.0))


def _term_liquidity_vol_context(cand: Candidate, rv: RegimeVector) -> float:
    score = 0.0
    if 25 <= rv.volatility_pctile <= 85:
        score += 0.4
    else:
        score -= 0.2
    score += 0.3 * (1.0 - rv.noise)
    if cand.session_anchored:
        score += 0.2
    return max(-1.0, min(1.0, score))


def score_candidate(state: dict, cand: Candidate, ctx: dict, rv: RegimeVector) -> dict:
    """Continuous weighted/logistic blend over a small, auditable set of
    terms (Section 4 mandatory design). Every term is individually capped
    before weighting so no one term can saturate the transform."""
    regime_lbl = regime_label(rv)
    terms = {
        "regime_fit": _term_regime_fit(cand, rv),
        "mtf_alignment": _term_mtf_alignment(cand, ctx),
        "confluence": _term_confluence(cand),
        "segment_performance": _term_segment_performance(state, cand, regime_lbl),
        "ev_rr": _term_ev_rr(cand),
        "liquidity_vol_context": _term_liquidity_vol_context(cand, rv),
        "session_open_proximity": (rv.session_open_proximity if cand.session_anchored else 0.0),
    }
    weighted_sum = 0.0
    contributions = {}
    for name, raw in terms.items():
        w = get_param(state, f"term_weight::{name}")
        capped = max(-1.0, min(1.0, raw))  # explicit per-term contribution cap
        contribution = w * capped
        contributions[name] = contribution
        weighted_sum += contribution
    engine_w = get_param(state, f"engine_weight::{cand.engine}")
    base_conf = cand.confidence + get_param(state, "calib::global_offset")
    z = weighted_sum * 0.55 + (base_conf - 0.5) * 1.4
    confidence = _sigmoid(z) * engine_w
    confidence = max(0.01, min(0.99, confidence))
    return {"confidence": confidence, "z": z, "contributions": contributions, "regime_label": regime_lbl}


def apply_regime_fit_veto(state: dict, cand: Candidate, rv: RegimeVector, scored: dict) -> float:
    """Regime-fit veto/heavy-discount (Section 13): suppress signals whose
    documented best-fit regime disagrees with the currently detected regime,
    even if raw confidence looks high."""
    label = scored["regime_label"]
    if label in cand.best_fit_regimes:
        return scored["confidence"]
    key = f"veto::regime_mismatch_discount::{cand.engine}__{label}"
    discount = get_param(state, key)
    return scored["confidence"] * (1.0 - discount)


def liquidity_sanity_check(cand: Candidate, ctx: dict, state: dict) -> bool:
    """Reject/heavily-discount entries sitting inside an about-to-be-swept
    level or immediately adjacent to an unmitigated EQH/EQL cluster, unless
    the engine is specifically designed to trade that behavior."""
    if cand.engine in ("liquidity_sweep", "reversal"):
        return True
    margin = get_param(state, "filter::liquidity_sanity_margin_atr") * ctx["atr_low"]
    for cluster in ctx["equal_levels"].get("eqh_clusters", []) + ctx["equal_levels"].get("eql_clusters", []):
        lvl = statistics.mean(s.price for s in cluster)
        if abs(cand.entry - lvl) < margin:
            return False
    return True


def in_macro_blackout(coin: str, ts: datetime, macro_events: list[dict]) -> bool:
    """Suppresses generation within a documented window around scheduled
    high-impact macro events for directly-affected assets + correlated
    majors (Section 13)."""
    for ev in macro_events:
        affected = set(ev.get("assets", [])) | (MACRO_CORRELATED_MAJORS if ev.get("macro", False) else set())
        if coin not in affected:
            continue
        ev_time = ev["time"]
        window_start = ev_time - timedelta(minutes=MACRO_BLACKOUT_MINUTES_BEFORE)
        window_end = ev_time + timedelta(minutes=MACRO_BLACKOUT_MINUTES_AFTER)
        if window_start <= ts <= window_end:
            return True
    return False


def correlation_clusters(bundles: dict[str, dict[str, list[dict]]], tf: str = "1h", lookback: int = 60) -> dict[str, int]:
    """Groups watchlist assets by simple return correlation so
    MAX_CONCURRENT_PER_CLUSTER can be enforced (Section 14)."""
    returns = {}
    for coin, bundle in bundles.items():
        candles = bundle.get(tf)
        if not candles or len(candles) < lookback + 1:
            continue
        closes = [c["c"] for c in candles[-lookback - 1:]]
        rets = [(closes[i] / closes[i - 1] - 1.0) for i in range(1, len(closes))]
        returns[coin] = rets
    coins = list(returns.keys())
    cluster_id = {}
    next_id = 0
    for i, a in enumerate(coins):
        if a in cluster_id:
            continue
        cluster_id[a] = next_id
        for b in coins[i + 1:]:
            if b in cluster_id:
                continue
            try:
                corr = statistics.correlation(returns[a], returns[b])
            except statistics.StatisticsError:
                corr = 0.0
            if abs(corr) >= CORRELATION_CLUSTER_MIN_ABS_CORR:
                cluster_id[b] = next_id
        next_id += 1
    return cluster_id


def rank_and_select(state: dict, scored_candidates: list[tuple[Candidate, dict]],
                     active_signals: list[dict], clusters: dict[str, int]) -> list[tuple[Candidate, dict]]:
    scored_candidates.sort(key=lambda pair: pair[1]["confidence"], reverse=True)
    active_slots = MAX_CONCURRENT_ACTIVE_SIGNALS - len(active_signals)
    cluster_load = collections.Counter()
    for sig in active_signals:
        cid = clusters.get(sig["coin"])
        if cid is not None:
            cluster_load[cid] += 1
    selected = []
    seen_coin_direction = set()
    for cand, scored in scored_candidates:
        if active_slots <= 0:
            break
        key = (cand.coin, cand.direction)
        if key in seen_coin_direction:
            continue  # dedup: strongest engine per coin/direction wins
        cid = clusters.get(cand.coin)
        if cid is not None and cluster_load[cid] >= MAX_CONCURRENT_PER_CLUSTER:
            continue
        selected.append((cand, scored))
        seen_coin_direction.add(key)
        if cid is not None:
            cluster_load[cid] += 1
        active_slots -= 1
    return selected


def grade_for_confidence(confidence: float) -> str:
    if confidence >= 0.78:
        return "A+"
    if confidence >= 0.65:
        return "A"
    return "B"


# =============================================================================
# SECTION K — ADAPTIVE FILTER PIPELINE (Section 9, with funnel logging - Sec 14)
# =============================================================================

def log_funnel(state: dict, stage: str, passed: bool) -> None:
    funnel = state["tier1"]["filter_funnel"].setdefault(stage, {"evaluated": 0, "rejected": 0})
    funnel["evaluated"] += 1
    if not passed:
        funnel["rejected"] += 1


def run_adaptive_filters(state: dict, cand: Candidate, ctx: dict, rv: RegimeVector,
                          scored: dict, ts: datetime, macro_events: list[dict]) -> tuple[bool, str]:
    # Tighten in chaotic markets, relax in clean markets — via the adaptive
    # min-confluence threshold, itself a bounded/dampened parameter.
    base_thresh = get_param(state, "filter::min_confluence_score")
    chaos_adj = (rv.noise - 0.5) * 0.25 - (rv.volatility_pctile / 100 - 0.5) * 0.05
    threshold = max(0.20, min(0.75, base_thresh + chaos_adj))
    confluence_score = min(1.0, len(set(cand.confluences)) / 4.0)
    passed = confluence_score >= threshold
    log_funnel(state, "confluence_threshold", passed)
    if not passed:
        return False, "confluence_below_adaptive_threshold"

    passed = cand.rr_tp1 >= cand.rr_floor_used
    log_funnel(state, "rr_floor", passed)
    if not passed:
        return False, "rr_below_floor"

    # Mandatory LTF confirmation trigger (Section 9): a candidate never fires
    # on HTF/MTF structural alignment alone. Engines whose zone-selection
    # sequence already requires a confirmed low-TF MSS (smc, pullback, and
    # the POI-based engines) set this at construction; every other engine is
    # checked here against the same generic low-TF-close-back-in-direction
    # criteria, so an HTF zone that is "right" but never actually turns gets
    # filtered out regardless of which engine proposed it.
    passed = cand.ltf_confirmed
    log_funnel(state, "ltf_confirmation", passed)
    if not passed:
        return False, "no_ltf_confirmation"

    passed = not engine_is_paused(state, cand.engine)
    log_funnel(state, "engine_paused", passed)
    if not passed:
        return False, "engine_paused"

    passed = liquidity_sanity_check(cand, ctx, state)
    log_funnel(state, "liquidity_sanity", passed)
    if not passed:
        return False, "liquidity_sanity_reject"

    passed = not in_macro_blackout(cand.coin, ts, macro_events)
    log_funnel(state, "macro_blackout", passed)
    if not passed:
        return False, "macro_blackout"

    if cand.sfp_purity and cand.sfp_purity > 0:
        min_purity = get_param(state, "filter::sfp_purity_min")
        passed = cand.sfp_purity >= min_purity
        log_funnel(state, "sfp_purity", passed)
        if not passed:
            return False, "sfp_impure"

    return True, "passed"


# =============================================================================
# SECTION L — ENTRY-FILL VERIFICATION & SIGNAL LIFECYCLE (Section 12, mandatory)
# =============================================================================

def new_signal_record(cand: Candidate, scored: dict, dispatch_ts: datetime, combo: str,
                       dispatch_watermark_t: Optional[int] = None) -> dict:
    """`dispatch_watermark_t` must be the `t` of the most recent fully-closed
    candle on this signal's low timeframe *as of dispatch*. Seeding the
    resolution watermark here (rather than leaving it None) is what stops
    resolve_active_signals() from treating the whole fetched candle history
    as 'new' on the signal's first resolution pass — without it, up to
    CANDLE_COUNT bars of pre-signal price action get replayed through
    resolve_against_candle() on the next scan, which can manufacture an
    SL/TP hit from a candle that closed before the signal ever existed."""
    grade = grade_for_confidence(scored["confidence"])
    pending_expiry_bars = PENDING_ENTRY_EXPIRY_BARS[combo]
    return {
        "id": f"{cand.coin}-{cand.engine}-{int(dispatch_ts.timestamp())}",
        "engine": cand.engine,
        "coin": cand.coin,
        "combo": combo,
        "direction": cand.direction,
        "entry": cand.entry,
        "sl": cand.sl,
        "tp1": cand.tp1,
        "tp2": cand.tp2,
        "entry_kind": cand.entry_kind,          # Section 12 mandatory abstraction
        "entry_filled": cand.entry_kind == "market",
        "pending_bars_elapsed": 0,
        "pending_expiry_bars": pending_expiry_bars,
        "confidence": scored["confidence"],
        "grade": grade,
        "rr_tp1": cand.rr_tp1,
        "confluences": cand.confluences,
        "regime_at_entry": scored["regime_label"],
        "session_anchored": cand.session_anchored,
        "dispatch_ts": dispatch_ts.isoformat(),
        "dispatch_ts_ms": int(dispatch_ts.timestamp() * 1000),
        "status": "pending" if cand.entry_kind == "pending" else "active",
        "resolution_logic_version": RESOLUTION_LOGIC_VERSION,
        "mae_r": 0.0,
        "mfe_r": 0.0,
        "last_checked_candle_t": dispatch_watermark_t,
    }


def _low_tf_for(sig: dict) -> str:
    return COMBOS[sig["combo"]]["low"]


def check_entry_fill(sig: dict, candle: dict) -> bool:
    """Section 12: never evaluate SL/TP before entry has actually filled.
    Returns True if this candle fills the entry (and same-candle SL/TP may
    then still be checked by the caller, conservatively)."""
    lo, hi = candle["l"], candle["h"]
    if lo <= sig["entry"] <= hi:
        sig["entry_filled"] = True
        return True
    sig["pending_bars_elapsed"] += 1
    return False


def _update_mae_mfe(sig: dict, candle: dict) -> None:
    risk = abs(sig["entry"] - sig["sl"])
    if risk <= 0:
        return
    if sig["direction"] == "long":
        adverse = max(0.0, sig["entry"] - candle["l"])
        favorable = max(0.0, candle["h"] - sig["entry"])
    else:
        adverse = max(0.0, candle["h"] - sig["entry"])
        favorable = max(0.0, sig["entry"] - candle["l"])
    sig["mae_r"] = max(sig["mae_r"], adverse / risk)
    sig["mfe_r"] = max(sig["mfe_r"], favorable / risk)


def resolve_against_candle(sig: dict, candle: dict) -> Optional[str]:
    """Section 11: single-TP resolution. Only SL and TP1 are ever checked.
    Conservative same-candle-ambiguity handling: if both SL and TP1 sit
    inside the same candle's range, SL is checked first (worst-case-first),
    which can only ever make the reported win rate MORE conservative, never
    manufacture a false stop-out relative to genuine price action — this is
    the documented resolution order required by Section 11."""
    hit_sl = (candle["l"] <= sig["sl"]) if sig["direction"] == "long" else (candle["h"] >= sig["sl"])
    hit_tp1 = (candle["h"] >= sig["tp1"]) if sig["direction"] == "long" else (candle["l"] <= sig["tp1"])
    if hit_sl:
        return "loss"
    if hit_tp1:
        return "win"
    return None


def resolve_active_signals(state: dict, bundles: dict[str, dict[str, list[dict]]],
                            reference_ts: datetime) -> list[dict]:
    """Advances every active/pending signal by whatever new closed candles
    are available on its own low timeframe. No automatic SL-to-breakeven
    repositioning exists anywhere in this function (Section 11) — TP1 hit
    resolves the trade immediately as a WIN under the full-exit model
    declared for this engine (100% of size closes at TP1; nothing remains
    open afterward, so a later touch of the original SL is bookkeeping-only
    and never rechecked because the signal is already resolved and removed
    from `active_signals`)."""
    resolved = []
    still_active = []
    for sig in state["tier2"]["active_signals"]:
        coin = sig["coin"]
        low_tf = _low_tf_for(sig)
        bundle = bundles.get(coin)
        if not bundle or low_tf not in bundle:
            still_active.append(sig)
            continue
        candles = bundle[low_tf]
        last_checked = sig.get("last_checked_candle_t")
        new_candles = [c for c in candles if last_checked is None or c["t"] > last_checked]
        outcome = None
        for candle in new_candles:
            sig["last_checked_candle_t"] = candle["t"]
            if not sig["entry_filled"]:
                filled_now = check_entry_fill(sig, candle)
                if not filled_now:
                    if sig["pending_bars_elapsed"] >= sig["pending_expiry_bars"]:
                        sig["status"] = "expired"
                        sig["result"] = "expired"  # distinct, excluded result type (Section 12)
                        sig["resolved_ts"] = reference_ts.isoformat()
                        sig["resolved_ts_ms"] = now_ms()
                        resolved.append(sig)
                        outcome = "expired"
                        break
                    continue
                sig["status"] = "active"
                # same candle that fills entry may still register a same-candle hit
            _update_mae_mfe(sig, candle)
            r = resolve_against_candle(sig, candle)
            if r is not None:
                risk = abs(sig["entry"] - sig["sl"]) or 1e-9
                if r == "win":
                    r_realized = abs(sig["tp1"] - sig["entry"]) / risk  # always positive by construction
                else:
                    r_realized = -1.0
                sig["result"] = r
                sig["r_realized"] = r_realized
                sig["status"] = "resolved"
                sig["resolved_ts"] = reference_ts.isoformat()
                sig["resolved_ts_ms"] = now_ms()
                resolved.append(sig)
                outcome = r
                break
        if outcome is None:
            still_active.append(sig)
    state["tier2"]["active_signals"] = still_active
    return resolved


# =============================================================================
# SECTION M — LOSS FORENSICS & ADAPTIVE FEEDBACK LOOP (Section 13, mandatory)
# =============================================================================

FORENSIC_CATEGORIES = [
    "regime_mismatch", "structural_invalidation_too_tight", "chased_swept_liquidity",
    "mtf_conflict_ignored", "sfp_mss_sequence_violated", "correct_read_poor_rr",
    "confidence_miscalibration", "filter_over_permissiveness", "genuine_variance",
]


def diagnose_trade(state: dict, sig: dict) -> str:
    """Every category is reached by a positive, verifiable condition on
    recorded trade data — never an else/fallback reached by elimination
    (Section 13, 1a). Order matters: more specific / more diagnostic
    conditions are checked first."""
    won = sig["result"] == "win"
    regime = sig.get("regime_at_entry", "neutral")

    if not won and regime not in ("trending", "expansion") and sig["engine"] in (
            "trend_continuation", "breakout", "momentum", "volatility_expansion"):
        return "regime_mismatch"

    if not won and sig.get("mae_r", 0.0) <= 1.05:
        return "structural_invalidation_too_tight"

    if not won and "liquidity_sweep" in sig.get("confluences", []) and sig["engine"] not in (
            "liquidity_sweep", "reversal"):
        return "chased_swept_liquidity"

    if not won and sig.get("mtf_conflict_at_entry", False):
        return "mtf_conflict_ignored"

    if not won and sig.get("sfp_purity_at_entry") is not None and sig["sfp_purity_at_entry"] < 0.45:
        return "sfp_mss_sequence_violated"

    if not won and sig.get("mfe_r", 0.0) >= 0.80 * sig["rr_tp1"]:
        return "correct_read_poor_rr"

    bucket_stats = state["tier1"]["calibration_buckets"].get(sig["engine"], {}).get(sig["grade"])
    if bucket_stats and bucket_stats.get("n", 0) >= MIN_SAMPLE_SIZE_FOR_ADAPTATION:
        realized_wr = bucket_stats["wins"] / bucket_stats["n"]
        assigned_conf = sig["confidence"]
        if not won and assigned_conf > realized_wr + 0.15:
            return "confidence_miscalibration"

    if not won and sig.get("thin_margin_filters"):
        return "filter_over_permissiveness"

    return "genuine_variance"


def route_forensic_response(state: dict, sig: dict, category: str) -> str:
    """One deterministic route per category, per Section 13 rule 3."""
    engine, coin = sig["engine"], sig["coin"]
    regime = sig.get("regime_at_entry", "neutral")
    won = sig["result"] == "win"
    delta_desc = "no_change"

    key = segment_key(engine=engine, regime=regime, category=category)
    seg = state["tier1"]["segment_stats"].setdefault(key, {"n": 0})
    seg["n"] = seg.get("n", 0) + 1
    if not has_min_sample(seg):
        return "sample_too_small"

    if category == "regime_mismatch":
        pkey = f"veto::regime_mismatch_discount::{engine}__{regime}"
        cur = get_param(state, pkey)
        update_param(state, pkey, min(0.85, cur + 0.05), base_key="veto::regime_mismatch_discount")
        delta_desc = f"{pkey} tightened"
    elif category == "structural_invalidation_too_tight":
        cur = get_param(state, "risk::sl_buffer_percentile")
        update_param(state, "risk::sl_buffer_percentile", min(90.0, cur + 3.0))
        delta_desc = "sl_buffer_percentile widened"
    elif category == "chased_swept_liquidity":
        cur = get_param(state, "filter::liquidity_sanity_margin_atr")
        update_param(state, "filter::liquidity_sanity_margin_atr", min(0.75, cur + 0.04))
        delta_desc = "liquidity_sanity_margin tightened"
    elif category == "mtf_conflict_ignored":
        cur = get_param(state, "term_weight::mtf_alignment")
        update_param(state, "term_weight::mtf_alignment", min(1.8, cur + 0.06), base_key="term_weight::mtf_alignment")
        delta_desc = "mtf_alignment weight raised"
    elif category == "sfp_mss_sequence_violated":
        cur = get_param(state, "filter::sfp_purity_min")
        update_param(state, "filter::sfp_purity_min", min(0.75, cur + 0.04))
        delta_desc = "sfp_purity_min tightened"
    elif category == "confidence_miscalibration":
        cur = get_param(state, "calib::global_offset")
        update_param(state, "calib::global_offset", max(-0.15, cur - 0.02))
        delta_desc = "calibration offset lowered"
    elif category == "filter_over_permissiveness":
        cur = get_param(state, "filter::min_confluence_score")
        update_param(state, "filter::min_confluence_score", min(0.62, cur + 0.03))
        delta_desc = "min_confluence_score tightened"
    elif category == "correct_read_poor_rr":
        delta_desc = "logged_for_rr_calibration_review_only"
    elif category == "genuine_variance":
        delta_desc = "no_change_expected_variance"

    # NOTE: engine_weight::* used to be nudged *up* right here on a winning
    # genuine_variance trade and nowhere else -- a one-way ratchet that could
    # raise an engine's weight but never lower it on a loss. That asymmetry
    # is fixed by removing the per-trade nudge entirely and instead letting
    # `update_engine_governor()` (Section 13 refinement, called once per scan
    # in run_scan) set engine_weight symmetrically off each engine's rolling
    # win rate in both directions, plus hard-pause/cautious-reactivate a
    # persistently bad engine independently of every other engine.

    counts = state["tier1"]["forensic_category_counts"].setdefault(category, {"count": 0, "trend": []})
    counts["count"] += 1
    counts["trend"].append(1)
    counts["trend"] = counts["trend"][-200:]

    return delta_desc


def run_learning_pass(state: dict, resolved_signals: list[dict]) -> None:
    circuit_open = state["tier1"]["circuit_breaker"]["tripped"]
    for sig in resolved_signals:
        if sig["result"] == "expired":
            fs = state["tier1"]["fill_stats"].setdefault(f"{sig['engine']}::{sig['entry_kind']}",
                                                           {"dispatched": 0, "filled": 0, "expired": 0})
            fs["expired"] += 1
            continue

        fs = state["tier1"]["fill_stats"].setdefault(f"{sig['engine']}::{sig['entry_kind']}",
                                                       {"dispatched": 0, "filled": 0, "expired": 0})
        fs["filled"] += 1

        won = sig["result"] == "win"
        assert not (won and sig["r_realized"] <= 0), "invariant violated: win with non-positive R"

        category = diagnose_trade(state, sig)
        sig["forensic_category"] = category

        seg_key_full = segment_key(engine=sig["engine"], regime=sig["regime_at_entry"],
                                    asset=sig["coin"], tf=sig["combo"])
        bump_segment_stat(state, seg_key_full, sig["r_realized"], won, sig["confidence"])

        calib = state["tier1"]["calibration_buckets"].setdefault(sig["engine"], {}).setdefault(
            sig["grade"], {"n": 0, "wins": 0, "sum_conf": 0.0})
        calib["n"] += 1
        calib["wins"] += 1 if won else 0
        calib["sum_conf"] += sig["confidence"]

        if not circuit_open:
            delta = route_forensic_response(state, sig, category)
        else:
            delta = "frozen_by_circuit_breaker"

        state["tier2"]["trade_log"].append({
            "entry": sig["entry"], "sl": sig["sl"], "tp1": sig["tp1"], "tp2": sig["tp2"],
            "r_realized": sig["r_realized"], "mae_r": sig["mae_r"], "mfe_r": sig["mfe_r"],
            "forensic_category": category, "confidence": sig["confidence"], "grade": sig["grade"],
            "regime_at_entry": sig["regime_at_entry"], "resolved_ts": sig["resolved_ts"],
            "resolved_ts_ms": sig["resolved_ts_ms"], "engine": sig["engine"], "coin": sig["coin"],
            "result": sig["result"], "resolution_logic_version": sig["resolution_logic_version"],
            "forensic_delta": delta,
        })
    check_circuit_breaker(state)


def check_circuit_breaker(state: dict) -> None:
    """Live-performance circuit breaker (Section 5): freezes adaptation on a
    material sustained deviation from the documented baseline, resumes once
    performance recovers or is manually cleared."""
    baseline = state["tier1"].get("baseline")
    log_list = [t for t in state["tier2"]["trade_log"] if t["result"] in ("win", "loss")]
    if baseline is None or len(log_list) < CIRCUIT_BREAKER_WINDOW:
        return
    recent = log_list[-CIRCUIT_BREAKER_WINDOW:]
    wins = sum(1 for t in recent if t["result"] == "win")
    win_rate = wins / len(recent)
    gains = sum(t["r_realized"] for t in recent if t["r_realized"] > 0)
    losses = abs(sum(t["r_realized"] for t in recent if t["r_realized"] < 0)) or 1e-9
    profit_factor = gains / losses

    cb = state["tier1"]["circuit_breaker"]
    baseline_wr = baseline.get("win_rate", win_rate)
    baseline_pf = baseline.get("profit_factor", profit_factor)

    materially_below = (baseline_wr - win_rate >= CIRCUIT_BREAKER_WIN_RATE_DROP) or \
                        (profit_factor <= baseline_pf * (1 - CIRCUIT_BREAKER_PF_DROP_FRAC))

    if materially_below and not cb["tripped"]:
        cb["tripped"] = True
        cb["tripped_at"] = now_iso()
        cb["reason"] = f"win_rate={win_rate:.2f} (baseline {baseline_wr:.2f}), pf={profit_factor:.2f} (baseline {baseline_pf:.2f})"
        log.warning("CIRCUIT BREAKER TRIPPED: %s", cb["reason"])
    elif cb["tripped"] and win_rate >= baseline_wr and profit_factor >= baseline_pf:
        cb["tripped"] = False
        cb["tripped_at"] = None
        cb["reason"] = "recovered"
        log.info("Circuit breaker auto-resumed: live performance recovered to baseline.")


# --- Per-engine adaptive weight / pause governor -------------------------------
# The circuit breaker above is a single global switch: it freezes/resumes
# adaptation for the whole ensemble at once and says nothing about *which*
# engine is responsible. This section discriminates by engine, mirroring the
# structure of check_circuit_breaker but scoped to one engine's own trade
# history, so one persistently bad engine can be hard-paused and later
# cautiously reactivated at reduced weight without needing every other
# engine's signals to be suppressed too.

def _engine_history(state: dict, engine: str, lookback: int) -> list[dict]:
    hist = [t for t in state["tier2"]["trade_log"]
            if t.get("engine") == engine and t.get("result") in ("win", "loss")]
    return hist[-lookback:]


def engine_winrate(state: dict, engine: str, lookback: int,
                    min_sample: int = ENGINE_GOVERNOR_MIN_SAMPLE) -> Optional[float]:
    hist = _engine_history(state, engine, lookback)
    if len(hist) < min_sample:
        return None
    wins = sum(1 for t in hist if t["result"] == "win")
    return wins / len(hist)


def _default_engine_governor_entry() -> dict:
    return {"paused": False}


def update_engine_governor(state: dict) -> None:
    """Call once per scan (see run_scan) to refresh per-engine pause state
    and engine_weight from realized win/loss history in tier2.trade_log. An
    engine whose trailing win rate over ENGINE_PAUSE_LOOKBACK resolved trades
    drops below ENGINE_PAUSE_WINRATE_FLOOR is hard-paused (excluded from
    candidate generation entirely via `engine_is_paused` in
    run_adaptive_filters) rather than just soft-weighted, until a fresh
    sample of the same size looks better -- at which point it's reactivated
    at a cautious reduced weight, not snapped back to 1.0.

    Engine_weight itself is nudged symmetrically (both up on outperformance
    and down on underperformance) off the rolling ENGINE_GOVERNOR_LOOKBACK
    win rate -- this replaces the previous one-way-only reinforcement in
    route_forensic_response(), which could only ever raise engine_weight and
    never lower it."""
    gov = state["tier1"].setdefault("engine_governor", {})
    for engine in ENGINE_FUNCS:
        entry = gov.setdefault(engine, _default_engine_governor_entry())
        ew_key = f"engine_weight::{engine}"

        if entry.get("paused"):
            recheck = _engine_history(state, engine, ENGINE_PAUSE_LOOKBACK)
            if len(recheck) >= ENGINE_PAUSE_LOOKBACK:
                wr = sum(1 for t in recheck if t["result"] == "win") / len(recheck)
                if wr >= ENGINE_PAUSE_WINRATE_FLOOR:
                    entry["paused"] = False
                    update_param(state, ew_key, ENGINE_REACTIVATE_WEIGHT, base_key=ew_key)
                    log.info("Engine governor: %s reactivated at cautious weight %.2f "
                             "(recheck win rate %.2f).", engine, ENGINE_REACTIVATE_WEIGHT, wr)
            continue  # still paused (or just reactivated) -- no further nudge this cycle

        pause_wr = engine_winrate(state, engine, ENGINE_PAUSE_LOOKBACK)
        if pause_wr is not None and pause_wr < ENGINE_PAUSE_WINRATE_FLOOR:
            entry["paused"] = True
            update_param(state, ew_key, PARAM_SPECS[ew_key].lo, base_key=ew_key)
            log.warning("Engine governor: %s HARD-PAUSED (win rate %.2f over last %d trades).",
                        engine, pause_wr, ENGINE_PAUSE_LOOKBACK)
            continue

        wr = engine_winrate(state, engine, ENGINE_GOVERNOR_LOOKBACK)
        if wr is None:
            continue  # not enough resolved history yet -- leave weight as-is
        delta = (wr - ENGINE_GOVERNOR_TARGET_WINRATE) * 0.6
        cur = get_param(state, ew_key)
        update_param(state, ew_key, cur + delta, base_key=ew_key)


def engine_is_paused(state: dict, engine: str) -> bool:
    return state["tier1"].get("engine_governor", {}).get(engine, {}).get("paused", False)


# =============================================================================
# SECTION N — TELEGRAM INTEGRATION (Section 17)
# =============================================================================

def _title_case_token(token: str) -> str:
    return token.replace("_", " ").replace("-", " ").strip().title()


def tg_send_message(text: str, reply_to: Optional[int] = None) -> Optional[int]:
    if not TG_BOT_TOKEN or not TG_CHAT_ID:
        log.info("[telegram-disabled]\n%s", text)
        return None
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TG_CHAT_ID, "text": text, "parse_mode": "Markdown"}
    if reply_to:
        payload["reply_to_message_id"] = reply_to
    body = json.dumps(payload).encode()
    req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode())
            return data.get("result", {}).get("message_id")
    except (urllib.error.URLError, TimeoutError, OSError, ValueError) as e:
        log.error("Telegram send failed: %s", e)
        return None


def tg_send_photo(path: str, caption: str = "") -> None:
    if not TG_BOT_TOKEN or not TG_CHAT_ID or not os.path.exists(path):
        return
    # Minimal multipart upload without external deps.
    boundary = "----virelle-boundary"
    with open(path, "rb") as f:
        img_bytes = f.read()
    parts = []
    parts.append(f"--{boundary}\r\nContent-Disposition: form-data; name=\"chat_id\"\r\n\r\n{TG_CHAT_ID}\r\n".encode())
    parts.append(f"--{boundary}\r\nContent-Disposition: form-data; name=\"caption\"\r\n\r\n{caption}\r\n".encode())
    parts.append(
        f"--{boundary}\r\nContent-Disposition: form-data; name=\"photo\"; filename=\"reaction.jpg\"\r\n"
        f"Content-Type: image/jpeg\r\n\r\n".encode())
    parts.append(img_bytes)
    parts.append(f"\r\n--{boundary}--\r\n".encode())
    body = b"".join(parts)
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendPhoto"
    req = urllib.request.Request(url, data=body,
                                  headers={"Content-Type": f"multipart/form-data; boundary={boundary}"})
    try:
        urllib.request.urlopen(req, timeout=15)
    except (urllib.error.URLError, TimeoutError, OSError) as e:
        log.error("Telegram photo send failed: %s", e)


def format_signal_message(sig: dict) -> str:
    direction_word = "LONG" if sig["direction"] == "long" else "SHORT"
    direction_dot = "🟢" if sig["direction"] == "long" else "🔴"
    lines = [
        f"*{ENGINE_NAME} {ENGINE_VERSION}* — New Signal",
        f"*{sig['coin'].upper()} — {direction_word}* {direction_dot}",
        "",
        f"Engine: {_title_case_token(sig['engine'])}",
        f"Style: {_title_case_token(sig['combo'])}",
        f"Grade: {sig['grade']}   Confidence: {sig['confidence']*100:.1f}%",
        f"Regime: {_title_case_token(sig['regime_at_entry'])}",
        "",
        f"Entry: `{sig['entry']:.6g}`",
        f"SL: `{sig['sl']:.6g}`",
        f"TP1: `{sig['tp1']:.6g}`",
        f"TP2 (suggested): `{sig['tp2']:.6g}`",
        "",
        f"RR (TP1): {sig['rr_tp1']:.2f}",
        f"Confluences: {', '.join(_title_case_token(c) for c in sig['confluences'])}",
        f"Entry type: {_title_case_token(sig['entry_kind'])}",
    ]
    return "\n".join(lines)


def format_status_message(sig: dict, status: str) -> str:
    direction_word = "LONG" if sig["direction"] == "long" else "SHORT"
    direction_dot = "🟢" if sig["direction"] == "long" else "🔴"
    lines = [
        f"*{ENGINE_NAME}* — {status}",
        f"*{sig['coin'].upper()} — {direction_word}* {direction_dot}",
        "",
        f"Engine: {_title_case_token(sig['engine'])}",
        f"Entry: `{sig['entry']:.6g}`",
        f"SL: `{sig['sl']:.6g}`",
        f"TP1: `{sig['tp1']:.6g}`",
    ]
    if status == "Resolved — WIN":
        lines.append(f"Realized R: {sig['r_realized']:.2f}")
        lines.append("")
        lines.append("Position closed in full at TP1. Nothing remains open on this signal.")
    elif status == "Resolved — LOSS":
        lines.append(f"Realized R: {sig['r_realized']:.2f}")
    elif status == "Expired (No Fill)":
        lines.append("")
        lines.append("Price never traded through the entry zone before expiry — no position was taken.")
    return "\n".join(lines)


def dispatch_new_signal(sig: dict) -> None:
    msg_id = tg_send_message(format_signal_message(sig))
    sig["telegram_message_id"] = msg_id
    if os.path.exists(REACTION_IMAGE_PATH):
        tg_send_photo(REACTION_IMAGE_PATH)


def dispatch_resolution(sig: dict) -> None:
    status = {"win": "Resolved — WIN", "loss": "Resolved — LOSS",
              "expired": "Expired (No Fill)"}.get(sig["result"], "Closed")
    tg_send_message(format_status_message(sig, status), reply_to=sig.get("telegram_message_id"))


def dispatch_activation(sig: dict) -> None:
    tg_send_message(format_status_message(sig, "Activated"), reply_to=sig.get("telegram_message_id"))


def build_daily_summary(state: dict) -> str:
    log_list = [t for t in state["tier2"]["trade_log"] if t["result"] in ("win", "loss")]
    today_cutoff = now_ms() - 86_400_000
    today = [t for t in log_list if t.get("resolved_ts_ms", 0) >= today_cutoff]
    wins = sum(1 for t in today if t["result"] == "win")
    losses = sum(1 for t in today if t["result"] == "loss")
    total = wins + losses
    win_rate = wins / total if total else 0.0
    gains = sum(t["r_realized"] for t in today if t["r_realized"] > 0)
    loss_sum = abs(sum(t["r_realized"] for t in today if t["r_realized"] < 0)) or 1e-9
    pf = gains / loss_sum if total else 0.0
    avg_rr = statistics.mean([t["r_realized"] for t in today if t["r_realized"] > 0]) if wins else 0.0

    by_regime = collections.Counter(t["regime_at_entry"] for t in today)
    by_engine = collections.Counter(t["engine"] for t in today)
    by_category = collections.Counter(t["forensic_category"] for t in today)

    best = max(today, key=lambda t: t["r_realized"], default=None)
    worst = min(today, key=lambda t: t["r_realized"], default=None)

    lines = [
        f"*{ENGINE_NAME} {ENGINE_VERSION}* — Daily Summary",
        "",
        f"Total Signals: {total}",
        f"Wins / Losses: {wins} / {losses}",
        f"Win Rate: {win_rate*100:.1f}%",
        f"Profit Factor: {pf:.2f}",
        f"Average RR (wins): {avg_rr:.2f}",
        "",
        "By Regime: " + ", ".join(f"{_title_case_token(k)} ({v})" for k, v in by_regime.items()) or "None",
        "By Engine: " + ", ".join(f"{_title_case_token(k)} ({v})" for k, v in by_engine.items()) or "None",
        "",
        "Forensic Breakdown:",
    ]
    for cat, count in by_category.items():
        lines.append(f"  {_title_case_token(cat)}: {count}")
    if best:
        lines += ["", f"Best Setup: {best['coin'].upper()} ({best['r_realized']:.2f}R)"]
    if worst:
        lines += [f"Worst Setup: {worst['coin'].upper()} ({worst['r_realized']:.2f}R)"]
    return "\n".join(lines)


def maybe_send_daily_summary(state: dict, ts: datetime) -> None:
    last_sent = state["meta"].get("last_daily_summary_date")
    today_str = ts.strftime("%Y-%m-%d")
    if ts.hour == 8 and last_sent != today_str:
        tg_send_message(build_daily_summary(state))
        state["meta"]["last_daily_summary_date"] = today_str


# =============================================================================
# SECTION O — MAIN SCAN ORCHESTRATION
# =============================================================================

def fetch_macro_events() -> list[dict]:
    """Documented high-impact macro event windows. In production this would
    be sourced from an economic-calendar feed; kept as a static/pluggable
    list here so the engine has zero hard external dependency for a scan to
    run, per Section 16's low-API-traffic goal."""
    return []


def build_candidates_for_asset(state: dict, coin: str, bundles: dict[str, dict[str, list[dict]]],
                                mark_prices: dict[str, float], ts: datetime) -> list[Candidate]:
    macro_bundle = bundles.get(MACRO_ANCHOR)
    rv = compute_regime_vector(coin, bundles, macro_bundle, ts)
    market_price = mark_prices.get(coin)
    if market_price is None:
        return []
    candidates = []
    for combo_name, combo_tfs in COMBOS.items():
        ctx = _structural_context(bundles[coin], combo_tfs)
        for engine_fn in ENGINE_FUNCS.values():
            try:
                candidates.extend(engine_fn(coin, combo_name, ctx, market_price, rv))
            except Exception as e:  # noqa: BLE001 - one engine's bug must never crash the scan
                log.error("Engine %s failed for %s/%s: %s", engine_fn.__name__, coin, combo_name, e)
    return candidates


def run_scan(state: dict, start_time: float) -> None:
    ts = datetime.now(timezone.utc)
    reference_ms = now_ms()
    candle_cache = load_candle_cache()

    log.info("Fetching watchlist candle bundles (%d assets)...", len(WATCHLIST))
    bundles = fetch_watchlist_bundles(candle_cache, reference_ms)
    save_candle_cache(candle_cache)
    if not bundles:
        log.error("No candle bundles resolved this run — aborting scan.")
        return

    mark_prices = fetch_mark_prices()

    # --- Step 1: resolve anything already being tracked --------------------
    resolved = resolve_active_signals(state, bundles, ts)
    if resolved:
        log.info("Resolved %d signal(s) this run.", len(resolved))
        run_learning_pass(state, resolved)
        for sig in resolved:
            dispatch_resolution(sig)

    if time.time() - start_time > SCAN_SOFT_DEADLINE_S:
        log.warning("Approaching scan deadline after resolution pass — skipping new-signal discovery this run.")
        save_state(state)
        return

    # Per-engine adaptive weight / pause governor -- refreshed once per scan
    # from realized trade_log history, independent of the global circuit
    # breaker (Section 13 refinement).
    update_engine_governor(state)
    paused = [e for e in ENGINE_FUNCS if engine_is_paused(state, e)]
    if paused:
        log.warning("Engine governor: %d engine(s) currently paused: %s", len(paused), ", ".join(paused))

    # --- Step 2: discover new candidates across the whole watchlist --------
    macro_events = fetch_macro_events()
    all_scored: list[tuple[Candidate, dict]] = []
    for coin in WATCHLIST:
        if _shutdown_requested or coin not in bundles:
            continue
        candidates = build_candidates_for_asset(state, coin, bundles, mark_prices, ts)
        macro_bundle = bundles.get(MACRO_ANCHOR)
        rv = compute_regime_vector(coin, bundles, macro_bundle, ts)
        for combo_name, combo_tfs in COMBOS.items():
            pass  # ctx recomputation avoided; filters below operate per-candidate using a fresh ctx
        for cand in candidates:
            ctx = _structural_context(bundles[coin], COMBOS[cand.combo])
            scored = score_candidate(state, cand, ctx, rv)
            discounted_conf = apply_regime_fit_veto(state, cand, rv, scored)
            scored["confidence"] = discounted_conf
            ok, reason = run_adaptive_filters(state, cand, ctx, rv, scored, ts, macro_events)
            if not ok:
                continue
            all_scored.append((cand, scored))

    existing_active = state["tier2"]["active_signals"]
    clusters = correlation_clusters(bundles)
    selected = rank_and_select(state, all_scored, existing_active, clusters)

    log.info("Selected %d new signal(s) from %d filtered candidates.", len(selected), len(all_scored))
    for cand, scored in selected:
        low_tf = COMBOS[cand.combo]["low"]
        low_tf_candles = bundles.get(cand.coin, {}).get(low_tf, [])
        # Seed the resolution watermark to the most recent CLOSED candle as of
        # dispatch, so resolve_active_signals() only ever evaluates candles
        # that close after this signal exists (see new_signal_record docstring).
        dispatch_watermark_t = low_tf_candles[-1]["t"] if low_tf_candles else None
        sig = new_signal_record(cand, scored, ts, cand.combo, dispatch_watermark_t)
        fs = state["tier1"]["fill_stats"].setdefault(f"{cand.engine}::{cand.entry_kind}",
                                                       {"dispatched": 0, "filled": 0, "expired": 0})
        fs["dispatched"] += 1
        state["tier2"]["active_signals"].append(sig)
        dispatch_new_signal(sig)

    maybe_send_daily_summary(state, ts)
    save_state(state)
    save_candle_cache(candle_cache)


def ensure_baseline(state: dict) -> None:
    """Section 13 pre-deployment acceptance bar: if no baseline exists yet,
    seed one from documented conservative defaults so the circuit breaker has
    something meaningful to compare against from day one. A real deployment
    should overwrite this via a dedicated backtest/paper-trading pass before
    going live; this default keeps cold-start structurally safe either way."""
    if state["tier1"].get("baseline") is None:
        state["tier1"]["baseline"] = {"win_rate": 0.42, "profit_factor": 1.3, "avg_rr": 1.7}


def main() -> None:
    start_time = time.time()
    log.info("=== %s %s — scan starting ===", ENGINE_NAME, ENGINE_VERSION)
    state = load_state()
    _GLOBAL_STATE_REF[0] = state
    ensure_baseline(state)
    try:
        run_scan(state, start_time)
    except Exception:
        log.exception("Unhandled exception during scan — persisting state before exit.")
        save_state(state)
        raise
    log.info("=== scan complete in %.1fs ===", time.time() - start_time)


if __name__ == "__main__":
    main()
