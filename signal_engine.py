#!/usr/bin/env python3
"""
Zenith Prime v1.0.0
===================
Institutional-grade multi-timeframe SMC/ICT crypto perpetual signal engine
for Hyperliquid. Ground-up architecture synthesizing the strongest patterns
observed across Crucible Alpha, Vectis, Nyx, Meridian, and the scalp_swing_bot
lineage, plus original additions (Adaptive Frequency Governor, Three-Combo
Regime Router, Setup Grade-to-Style sizing table, session/liquidity-aware
scoring engine).

Single-file, production-ready. Scan-per-run model driven by an external
scheduler (cron-job.org, GitHub Actions, etc.) hitting this script every 15m.

Author: Zenith Prime project
"""

from __future__ import annotations

import json
import math
import os
import time
import signal
import logging
import statistics
import urllib.request
import urllib.error
from dataclasses import dataclass, field
from typing import Optional

# ============================================================================
# CONFIGURATION
# ============================================================================

HL_API_URL = "https://api.hyperliquid.xyz/info"
TG_BOT_TOKEN = os.environ.get("TG_BOT_TOKEN", "")
TG_CHAT_ID = os.environ.get("TG_CHAT_ID", "")

STATE_PATH = os.environ.get("ZENITH_STATE_PATH", "state.json")
LOG_PATH = os.environ.get("ZENITH_LOG_PATH", "zenith_prime.log")

WATCHLIST = [
    "BTC", "ETH", "HYPE", "ZEC", "NEAR", "ONDO", "SUI", "PENGU", "BNB", "SOL",
    "TRX", "BCH", "DOGE", "ADA", "DOT", "TAO", "AVAX", "LINK", "AAVE", "XRP",
    "XLM", "UNI", "LTC", "APT", "PENDLE",
]

# Timeframe combos: (bias_tf, structure_tf, execution_tf)
# The Regime Router selects one of these per scan per symbol based on
# volatility, ADX, and session conditions.
COMBOS = {
    "scalp":  {"bias": "1h",  "struct": "15m", "exec": "5m",  "hold_hint": "0.5-4h"},
    "intraday": {"bias": "4h", "struct": "1h",  "exec": "15m", "hold_hint": "4-24h"},
    "swing":  {"bias": "1d",  "struct": "4h",  "exec": "1h",  "hold_hint": "1-5d"},
}

TF_MS = {
    "5m": 5 * 60_000, "15m": 15 * 60_000, "1h": 60 * 60_000,
    "4h": 4 * 60 * 60_000, "1d": 24 * 60 * 60_000,
}

CANDLE_COUNT = {"5m": 300, "15m": 300, "1h": 300, "4h": 240, "1d": 180}

ATR_LEN = 14
RSI_LEN = 14
ADX_LEN = 14
EMA_FAST = 20
EMA_SLOW = 50
EMA_TREND = 200

# Adaptive Frequency Governor targets
TARGET_SIGNALS_MIN = 5
TARGET_SIGNALS_MAX = 10
GOVERNOR_ADJUST_STEP = 2.0     # points of confidence threshold to nudge per governor pass
GOVERNOR_FLOOR = 55.0
GOVERNOR_CEIL = 88.0

# Setup Grade -> Style sizing table (risk % of account equity per trade)
GRADE_SIZE_TABLE = {
    ("A+", "scalp"): 1.00, ("A+", "intraday"): 1.25, ("A+", "swing"): 1.50,
    ("A",  "scalp"): 0.75, ("A",  "intraday"): 1.00, ("A",  "swing"): 1.25,
    ("B",  "scalp"): 0.50, ("B",  "intraday"): 0.65, ("B",  "swing"): 0.85,
    ("C",  "scalp"): 0.25, ("C",  "intraday"): 0.35, ("C",  "swing"): 0.45,
}

MAX_CONCURRENT_PER_SYMBOL = 1
COOLDOWN_BARS_15M = 6           # bars of 15m equivalent between signals on same symbol+dir
DEDUP_PRICE_TOL_PCT = 0.0025

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.FileHandler(LOG_PATH), logging.StreamHandler()],
)
log = logging.getLogger("zenith")


def _handle_shutdown(sig_num, frame):
    log.warning("Received shutdown signal %s, exiting cleanly.", sig_num)
    raise SystemExit(0)


signal.signal(signal.SIGTERM, _handle_shutdown)
signal.signal(signal.SIGINT, _handle_shutdown)


# ============================================================================
# HYPERLIQUID API LAYER
# ============================================================================

def hl_post(payload: dict, retries: int = 3, timeout: int = 12) -> dict | list | None:
    body = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        HL_API_URL, data=body,
        headers={"Content-Type": "application/json"}, method="POST",
    )
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, ValueError) as e:
            log.warning("HL API attempt %d/%d failed: %s", attempt + 1, retries, e)
            time.sleep(1.5 * (attempt + 1))
    log.error("HL API call failed after %d retries: %s", retries, payload)
    return None


def hl_coin(symbol: str) -> str:
    return symbol.upper()


def current_bar_open_ms(reference_ms: int, interval: str) -> int:
    step = TF_MS[interval]
    return (reference_ms // step) * step


def get_candles(symbol: str, interval: str, n: int, reference_ms: Optional[int] = None) -> list[dict]:
    reference_ms = reference_ms or int(time.time() * 1000)
    step = TF_MS[interval]
    start = reference_ms - step * (n + 2)
    payload = {
        "type": "candleSnapshot",
        "req": {"coin": hl_coin(symbol), "interval": interval, "startTime": start, "endTime": reference_ms},
    }
    raw = hl_post(payload)
    if not raw or not isinstance(raw, list):
        return []
    out = []
    for c in raw:
        try:
            out.append({
                "t": int(c["t"]), "o": float(c["o"]), "h": float(c["h"]),
                "l": float(c["l"]), "c": float(c["c"]), "v": float(c["v"]),
                "n": int(c.get("n", 0)),
            })
        except (KeyError, TypeError, ValueError):
            continue
    # only fully-closed bars
    cur_open = current_bar_open_ms(reference_ms, interval)
    out = [c for c in out if c["t"] < cur_open]
    out.sort(key=lambda c: c["t"])
    return out[-n:]


def fetch_all_candles(symbol: str, reference_ms: Optional[int] = None) -> dict[str, list[dict]]:
    bundle = {}
    for tf in ("5m", "15m", "1h", "4h", "1d"):
        bundle[tf] = get_candles(symbol, tf, CANDLE_COUNT[tf], reference_ms)
    return bundle


def get_meta_and_ctx() -> tuple[list[str], list[dict]] | None:
    raw = hl_post({"type": "metaAndAssetCtxs"})
    if not raw or not isinstance(raw, list) or len(raw) < 2:
        return None
    universe = [a["name"] for a in raw[0].get("universe", [])]
    return universe, raw[1]


def get_market_snapshot() -> dict[str, dict]:
    """funding, open interest, mark price, day volume per symbol."""
    got = get_meta_and_ctx()
    if not got:
        return {}
    universe, ctxs = got
    snap = {}
    for name, ctx in zip(universe, ctxs):
        try:
            snap[name] = {
                "funding": float(ctx.get("funding", 0.0)),
                "oi_usd": float(ctx.get("openInterest", 0.0)) * float(ctx.get("markPx", 0.0)),
                "mark_px": float(ctx.get("markPx", 0.0)),
                "day_vol_usd": float(ctx.get("dayNtlVlm", 0.0)),
            }
        except (TypeError, ValueError):
            continue
    return snap


def get_l2_book(coin: str) -> dict | None:
    return hl_post({"type": "l2Book", "coin": hl_coin(coin)})


def analyze_orderbook(coin: str) -> dict:
    """Cheap L2 imbalance read: bid/ask depth ratio within top N levels."""
    book = get_l2_book(coin)
    default = {"imbalance": 0.0, "spread_bps": None}
    if not book or "levels" not in book:
        return default
    try:
        bids, asks = book["levels"][0], book["levels"][1]
        depth_bid = sum(float(l["sz"]) for l in bids[:10])
        depth_ask = sum(float(l["sz"]) for l in asks[:10])
        total = depth_bid + depth_ask
        imbalance = (depth_bid - depth_ask) / total if total > 0 else 0.0
        best_bid, best_ask = float(bids[0]["px"]), float(asks[0]["px"])
        mid = (best_bid + best_ask) / 2
        spread_bps = ((best_ask - best_bid) / mid) * 10_000 if mid else None
        return {"imbalance": imbalance, "spread_bps": spread_bps}
    except (KeyError, IndexError, ValueError, ZeroDivisionError):
        return default


# ============================================================================
# MATH / INDICATOR PRIMITIVES
# ============================================================================

def safe(v, fb=0.0):
    try:
        f = float(v)
        return f if math.isfinite(f) else fb
    except (TypeError, ValueError):
        return fb


def ema(vals: list[float], period: int) -> list[float]:
    if not vals:
        return []
    k = 2 / (period + 1)
    out = [vals[0]]
    for v in vals[1:]:
        out.append(v * k + out[-1] * (1 - k))
    return out


def sma(vals: list[float], period: int) -> list[float]:
    out = []
    for i in range(len(vals)):
        window = vals[max(0, i - period + 1): i + 1]
        out.append(sum(window) / len(window))
    return out


def stdev(vals: list[float], period: int) -> list[float]:
    out = []
    for i in range(len(vals)):
        window = vals[max(0, i - period + 1): i + 1]
        out.append(statistics.pstdev(window) if len(window) > 1 else 0.0)
    return out


def rsi(closes: list[float], period: int = RSI_LEN) -> list[float]:
    if len(closes) < 2:
        return [50.0] * len(closes)
    gains, losses = [0.0], [0.0]
    for i in range(1, len(closes)):
        d = closes[i] - closes[i - 1]
        gains.append(max(d, 0.0))
        losses.append(max(-d, 0.0))
    avg_g, avg_l = gains[0], losses[0]
    out = [50.0]
    for i in range(1, len(closes)):
        avg_g = (avg_g * (period - 1) + gains[i]) / period
        avg_l = (avg_l * (period - 1) + losses[i]) / period
        rs = avg_g / avg_l if avg_l > 1e-12 else 100.0
        out.append(100 - (100 / (1 + rs)))
    return out


def true_range(candles: list[dict]) -> list[float]:
    tr = []
    for i, c in enumerate(candles):
        if i == 0:
            tr.append(c["h"] - c["l"])
        else:
            pc = candles[i - 1]["c"]
            tr.append(max(c["h"] - c["l"], abs(c["h"] - pc), abs(c["l"] - pc)))
    return tr


def atr(candles: list[dict], period: int = ATR_LEN) -> list[float]:
    tr = true_range(candles)
    if not tr:
        return []
    out = [tr[0]]
    for i in range(1, len(tr)):
        out.append((out[-1] * (period - 1) + tr[i]) / period)
    return out


def adx_dmi(candles: list[dict], period: int = ADX_LEN) -> tuple[list[float], list[float], list[float]]:
    n = len(candles)
    if n < 2:
        return [0.0] * n, [0.0] * n, [0.0] * n
    plus_dm, minus_dm, tr = [0.0], [0.0], [candles[0]["h"] - candles[0]["l"]]
    for i in range(1, n):
        up = candles[i]["h"] - candles[i - 1]["h"]
        down = candles[i - 1]["l"] - candles[i]["l"]
        plus_dm.append(up if (up > down and up > 0) else 0.0)
        minus_dm.append(down if (down > up and down > 0) else 0.0)
        pc = candles[i - 1]["c"]
        tr.append(max(candles[i]["h"] - candles[i]["l"], abs(candles[i]["h"] - pc), abs(candles[i]["l"] - pc)))

    def wilder(series):
        out = [series[0]]
        for i in range(1, len(series)):
            out.append(out[-1] - out[-1] / period + series[i])
        return out

    tr_s, pdm_s, mdm_s = wilder(tr), wilder(plus_dm), wilder(minus_dm)
    plus_di = [100 * (pdm_s[i] / tr_s[i]) if tr_s[i] > 1e-12 else 0.0 for i in range(n)]
    minus_di = [100 * (mdm_s[i] / tr_s[i]) if tr_s[i] > 1e-12 else 0.0 for i in range(n)]
    dx = [100 * abs(plus_di[i] - minus_di[i]) / (plus_di[i] + minus_di[i]) if (plus_di[i] + minus_di[i]) > 1e-12 else 0.0
          for i in range(n)]
    adx = wilder(dx) if dx else []
    adx = [v / period for v in adx]  # renormalize Wilder cumulative scaling
    return adx, plus_di, minus_di


def bollinger_width_pct(closes: list[float], period: int = 20, mult: float = 2.0) -> list[float]:
    mid = sma(closes, period)
    sd = stdev(closes, period)
    out = []
    for i in range(len(closes)):
        width = (2 * mult * sd[i]) / mid[i] * 100 if mid[i] > 1e-9 else 0.0
        out.append(width)
    return out


def compute_indicators(candles: list[dict]) -> dict:
    if len(candles) < 5:
        return {}
    closes = [c["c"] for c in candles]
    ind = {
        "ema_fast": ema(closes, EMA_FAST),
        "ema_slow": ema(closes, EMA_SLOW),
        "ema_trend": ema(closes, min(EMA_TREND, max(10, len(closes) - 1))),
        "rsi": rsi(closes, RSI_LEN),
        "atr": atr(candles, ATR_LEN),
        "bb_width": bollinger_width_pct(closes),
    }
    adx, pdi, mdi = adx_dmi(candles, ADX_LEN)
    ind["adx"], ind["plus_di"], ind["minus_di"] = adx, pdi, mdi
    return ind


def cvd_proxy(candles: list[dict], lookback: int = 30) -> float:
    """Cumulative-delta-style volume proxy using candle body direction as a
    stand-in for trade-level buy/sell tagging (no tick data available)."""
    window = candles[-lookback:]
    cvd = 0.0
    for c in window:
        rng = c["h"] - c["l"]
        if rng <= 1e-12:
            continue
        buy_frac = (c["c"] - c["l"]) / rng
        signed = c["v"] * (2 * buy_frac - 1)
        cvd += signed
    return cvd


# ============================================================================
# STATE MANAGEMENT
# ============================================================================

def _default_state() -> dict:
    return {
        "version": "zenith-prime-1.0.0",
        "signals": {},          # signal_id -> record
        "recent_by_symbol": {},  # symbol -> [{"t":ms,"dir":.., "price":.., "combo":..}]
        "governor": {"threshold": 68.0, "history_days": {}},
        "win_history": {"by_grade": {}, "by_setup": {}},
        "atr_pct_memory": {},
        "last_run_ms": 0,
    }


def load_state() -> dict:
    if not os.path.exists(STATE_PATH):
        return _default_state()
    try:
        with open(STATE_PATH, "r") as f:
            state = json.load(f)
        base = _default_state()
        for k, v in base.items():
            state.setdefault(k, v)
        return state
    except (json.JSONDecodeError, OSError) as e:
        log.error("Failed to load state (%s), starting fresh.", e)
        return _default_state()


def save_state(state: dict):
    tmp = STATE_PATH + ".tmp"
    with open(tmp, "w") as f:
        json.dump(state, f)
    os.replace(tmp, STATE_PATH)


def prune_state(state: dict, max_signals: int = 500, max_days: int = 14):
    cutoff = int(time.time() * 1000) - max_days * 86_400_000
    state["signals"] = {
        sid: rec for sid, rec in state["signals"].items() if rec.get("t", 0) >= cutoff
    }
    if len(state["signals"]) > max_signals:
        ordered = sorted(state["signals"].items(), key=lambda kv: kv[1].get("t", 0))
        state["signals"] = dict(ordered[-max_signals:])
    for sym in list(state["recent_by_symbol"].keys()):
        state["recent_by_symbol"][sym] = [
            r for r in state["recent_by_symbol"][sym] if r.get("t", 0) >= cutoff
        ][-20:]


# ============================================================================
# MARKET STRUCTURE / SMC PRIMITIVES
# ============================================================================

@dataclass
class Swing:
    index: int
    price: float
    kind: str   # "high" | "low"
    t: int


def find_swings(candles: list[dict], left: int = 2, right: int = 2) -> list[Swing]:
    out = []
    n = len(candles)
    for i in range(left, n - right):
        h = candles[i]["h"]
        l = candles[i]["l"]
        if all(h >= candles[i - k]["h"] for k in range(1, left + 1)) and \
           all(h >= candles[i + k]["h"] for k in range(1, right + 1)):
            out.append(Swing(i, h, "high", candles[i]["t"]))
        if all(l <= candles[i - k]["l"] for k in range(1, left + 1)) and \
           all(l <= candles[i + k]["l"] for k in range(1, right + 1)):
            out.append(Swing(i, l, "low", candles[i]["t"]))
    return out


@dataclass
class StructureState:
    trend: str = "neutral"        # "bullish" | "bearish" | "neutral"
    last_bos: Optional[str] = None
    last_choch: Optional[str] = None
    last_swing_high: Optional[float] = None
    last_swing_low: Optional[float] = None


def analyze_structure(candles: list[dict], swings: list[Swing]) -> StructureState:
    st = StructureState()
    highs = [s for s in swings if s.kind == "high"]
    lows = [s for s in swings if s.kind == "low"]
    if not highs or not lows:
        return st
    st.last_swing_high = highs[-1].price
    st.last_swing_low = lows[-1].price

    # Determine HH/HL vs LH/LL sequence using the last 4 swings chronologically
    seq = sorted(swings, key=lambda s: s.index)[-6:]
    hh = hl = lh = ll = 0
    for i in range(1, len(seq)):
        a, b = seq[i - 1], seq[i]
        if a.kind == "high" and b.kind == "high":
            hh += 1 if b.price > a.price else 0
            lh += 1 if b.price < a.price else 0
        if a.kind == "low" and b.kind == "low":
            hl += 1 if b.price > a.price else 0
            ll += 1 if b.price < a.price else 0

    if hh + hl > lh + ll:
        st.trend = "bullish"
    elif lh + ll > hh + hl:
        st.trend = "bearish"
    else:
        st.trend = "neutral"

    last_close = candles[-1]["c"]
    if st.trend == "bullish" and last_close < st.last_swing_low:
        st.last_choch = "bearish_choch"
    elif st.trend == "bearish" and last_close > st.last_swing_high:
        st.last_choch = "bullish_choch"
    if last_close > st.last_swing_high:
        st.last_bos = "bullish_bos"
    elif last_close < st.last_swing_low:
        st.last_bos = "bearish_bos"
    return st


@dataclass
class POIZone:
    low: float
    high: float
    kind: str          # "order_block" | "fvg" | "breaker"
    direction: str      # "bullish" | "bearish"
    index: int
    quality: float = 1.0
    tested: bool = False

    def contains(self, price: float, buf: float = 0.0) -> bool:
        return (self.low - buf) <= price <= (self.high + buf)

    def mid(self) -> float:
        return (self.low + self.high) / 2


def find_order_blocks(candles: list[dict], atr_vals: list[float], lookback: int = 60) -> list[POIZone]:
    n = len(candles)
    zones = []
    start = max(1, n - lookback)
    for i in range(start, n - 1):
        c = candles[i]
        nxt = candles[i + 1]
        body = abs(c["c"] - c["o"])
        avg_atr = atr_vals[i] if i < len(atr_vals) else 0.0
        if avg_atr <= 0:
            continue
        # Bearish candle followed by strong bullish displacement => bullish OB
        if c["c"] < c["o"] and nxt["c"] > nxt["o"] and (nxt["c"] - nxt["o"]) > 1.2 * avg_atr:
            zones.append(POIZone(c["l"], c["h"], "order_block", "bullish", i, quality=min(2.0, body / avg_atr)))
        # Bullish candle followed by strong bearish displacement => bearish OB
        if c["c"] > c["o"] and nxt["c"] < nxt["o"] and (nxt["o"] - nxt["c"]) > 1.2 * avg_atr:
            zones.append(POIZone(c["l"], c["h"], "order_block", "bearish", i, quality=min(2.0, body / avg_atr)))
    return zones[-12:]


def find_fvgs(candles: list[dict], lookback: int = 60) -> list[POIZone]:
    n = len(candles)
    zones = []
    start = max(2, n - lookback)
    for i in range(start, n):
        a, b, c = candles[i - 2], candles[i - 1], candles[i]
        if a["h"] < c["l"]:
            zones.append(POIZone(a["h"], c["l"], "fvg", "bullish", i))
        if a["l"] > c["h"]:
            zones.append(POIZone(c["h"], a["l"], "fvg", "bearish", i))
    return zones[-12:]


def mark_untested(zones: list[POIZone], candles: list[dict]) -> list[POIZone]:
    for z in zones:
        touched = False
        for c in candles[z.index + 1:]:
            if c["l"] <= z.high and c["h"] >= z.low:
                touched = True
                break
        z.tested = touched
    return zones


def cluster_levels(levels: list[float], tol_pct: float = 0.0015) -> list[tuple[float, int]]:
    if not levels:
        return []
    levels = sorted(levels)
    clusters = [[levels[0]]]
    for lv in levels[1:]:
        if abs(lv - clusters[-1][-1]) / clusters[-1][-1] <= tol_pct:
            clusters[-1].append(lv)
        else:
            clusters.append([lv])
    return [(sum(cl) / len(cl), len(cl)) for cl in clusters]


def build_liquidity_pools(swings: list[Swing]) -> dict:
    highs = [s.price for s in swings if s.kind == "high"]
    lows = [s.price for s in swings if s.kind == "low"]
    return {
        "buy_side": cluster_levels(highs),   # resting liquidity above highs (sell stops/shorts)
        "sell_side": cluster_levels(lows),   # resting liquidity below lows (buy stops/longs)
    }


def detect_sweep(candles: list[dict], pools: dict, direction: str, lookback: int = 10) -> Optional[dict]:
    """direction = 'bullish' means we expect a sweep of sell-side liquidity
    (a low taken out) followed by reclaim, feeding a long. Symmetric for bearish."""
    recent = candles[-lookback:]
    targets = pools["sell_side"] if direction == "bullish" else pools["buy_side"]
    if not targets:
        return None
    for level, weight in sorted(targets, key=lambda t: -t[1]):
        for i, c in enumerate(recent):
            if direction == "bullish" and c["l"] < level and c["c"] > level:
                return {"level": level, "weight": weight, "bar_offset": len(recent) - i}
            if direction == "bearish" and c["h"] > level and c["c"] < level:
                return {"level": level, "weight": weight, "bar_offset": len(recent) - i}
    return None


def premium_discount_zone(candles: list[dict], lookback: int = 50) -> dict:
    window = candles[-lookback:]
    hi = max(c["h"] for c in window)
    lo = min(c["l"] for c in window)
    eq = (hi + lo) / 2
    last = candles[-1]["c"]
    zone = "premium" if last > eq else "discount" if last < eq else "equilibrium"
    return {"high": hi, "low": lo, "eq": eq, "zone": zone}


def detect_mss(candles_exec: list[dict], direction: str, lookback: int = 20) -> Optional[dict]:
    """Market structure shift on the execution timeframe after a sweep:
    a break of the most recent minor swing in the trade direction."""
    swings = find_swings(candles_exec[-lookback - 5:], left=1, right=1)
    if not swings:
        return None
    last_close = candles_exec[-1]["c"]
    if direction == "bullish":
        highs = [s for s in swings if s.kind == "high"]
        if not highs:
            return None
        ref = highs[-1].price
        if last_close > ref:
            return {"level": ref, "confirmed": True}
    else:
        lows = [s for s in swings if s.kind == "low"]
        if not lows:
            return None
        ref = lows[-1].price
        if last_close < ref:
            return {"level": ref, "confirmed": True}
    return None


# ============================================================================
# REGIME DETECTION
# ============================================================================

@dataclass
class RegimeVector:
    volatility_pctile: float = 0.5
    trend_strength: float = 0.0     # ADX-based, 0-1
    session_weight: float = 1.0
    noise_index: float = 0.5
    btc_bias: str = "neutral"
    btc_strength: float = 0.0
    breadth: float = 0.5

    def composite_favorability(self) -> float:
        return (
            0.30 * self.trend_strength +
            0.20 * (1 - self.noise_index) +
            0.20 * self.session_weight +
            0.15 * self.btc_strength +
            0.15 * self.breadth
        )


def session_weight_now() -> float:
    hour_utc = time.gmtime().tm_hour
    # London/NY overlap and NY session get full weight; Asia quieter for alts
    if 12 <= hour_utc <= 20:
        return 1.0
    if 6 <= hour_utc < 12:
        return 0.8
    return 0.6


def compute_noise_index(candles: list[dict], lookback: int = 30) -> float:
    window = candles[-lookback:]
    if len(window) < 5:
        return 0.5
    body_sum = sum(abs(c["c"] - c["o"]) for c in window)
    range_sum = sum(c["h"] - c["l"] for c in window) or 1e-9
    efficiency = body_sum / range_sum
    return max(0.0, min(1.0, 1 - efficiency))


def update_atr_pct_memory(state: dict, symbol: str, atr_pct: float) -> float:
    mem = state["atr_pct_memory"].setdefault(symbol, [])
    mem.append(atr_pct)
    if len(mem) > 200:
        mem.pop(0)
    if len(mem) < 10:
        return 0.5
    sorted_mem = sorted(mem)
    rank = sum(1 for v in sorted_mem if v <= atr_pct) / len(sorted_mem)
    return rank


def compute_btc_regime(btc_bundle: dict) -> tuple[str, float]:
    candles_4h = btc_bundle.get("4h", [])
    if len(candles_4h) < 30:
        return "neutral", 0.0
    ind = compute_indicators(candles_4h)
    if not ind:
        return "neutral", 0.0
    closes = [c["c"] for c in candles_4h]
    fast, slow = ind["ema_fast"][-1], ind["ema_slow"][-1]
    adx_val = ind["adx"][-1]
    strength = min(1.0, adx_val / 40.0)
    if fast > slow and closes[-1] > fast:
        return "bullish", strength
    if fast < slow and closes[-1] < fast:
        return "bearish", strength
    return "neutral", strength * 0.5


def build_regime_vector(state: dict, symbol: str, bundle: dict, btc_bias: str, btc_strength: float,
                         breadth: float) -> RegimeVector:
    candles_1h = bundle.get("1h", [])
    ind = compute_indicators(candles_1h) if candles_1h else {}
    r = RegimeVector()
    if ind:
        atr_val = ind["atr"][-1]
        atr_pct = atr_val / candles_1h[-1]["c"] * 100 if candles_1h[-1]["c"] else 0.0
        r.volatility_pctile = update_atr_pct_memory(state, symbol, atr_pct)
        r.trend_strength = min(1.0, ind["adx"][-1] / 40.0)
        r.noise_index = compute_noise_index(candles_1h)
    r.session_weight = session_weight_now()
    r.btc_bias = btc_bias
    r.btc_strength = btc_strength
    r.breadth = breadth
    return r


# ============================================================================
# THREE-COMBO REGIME ROUTER
# ============================================================================

def select_combo(regime: RegimeVector) -> str:
    """Original routing logic: pick the bias/structure/execution timeframe
    combo that best matches current volatility + trend regime.
    - High volatility + strong trend -> scalp (fast execution captures moves)
    - Moderate trend, normal vol -> intraday (workhorse combo)
    - Low volatility / range compressing / strong HTF trend -> swing
    """
    if regime.volatility_pctile >= 0.75 and regime.trend_strength >= 0.5:
        return "scalp"
    if regime.volatility_pctile <= 0.35 and regime.trend_strength >= 0.45:
        return "swing"
    return "intraday"


# ============================================================================
# ADAPTIVE FREQUENCY GOVERNOR
# ============================================================================

def governor_adjust_threshold(state: dict) -> float:
    """Original mechanism: tracks signals produced over the trailing 24h and
    nudges the confidence threshold up/down to keep output in the
    5-10 signals/day band without hard-capping quality."""
    gov = state["governor"]
    now = int(time.time() * 1000)
    cutoff = now - 86_400_000
    count_24h = sum(1 for rec in state["signals"].values() if rec.get("t", 0) >= cutoff)
    threshold = gov.get("threshold", 68.0)

    if count_24h < TARGET_SIGNALS_MIN:
        threshold -= GOVERNOR_ADJUST_STEP
    elif count_24h > TARGET_SIGNALS_MAX:
        threshold += GOVERNOR_ADJUST_STEP
    else:
        # gently relax back toward a neutral 70 when in-band
        threshold += (70.0 - threshold) * 0.1

    threshold = max(GOVERNOR_FLOOR, min(GOVERNOR_CEIL, threshold))
    gov["threshold"] = threshold
    gov.setdefault("history_days", {})
    today_key = time.strftime("%Y-%m-%d", time.gmtime())
    gov["history_days"][today_key] = count_24h
    # keep only last 14 days
    if len(gov["history_days"]) > 14:
        for k in sorted(gov["history_days"].keys())[:-14]:
            del gov["history_days"][k]
    return threshold


# ============================================================================
# CANDIDATE / SIGNAL DATA MODEL
# ============================================================================

@dataclass
class Candidate:
    symbol: str
    direction: str          # "LONG" | "SHORT"
    style: str              # "scalp" | "intraday" | "swing"
    entry: float
    sl: float
    tp1: float
    tp2: float
    confluences: list = field(default_factory=list)
    setup_family: str = "smc_reversal"
    raw_score: float = 0.0
    confidence: float = 0.0
    grade: str = "C"

    def rr(self) -> float:
        risk = abs(self.entry - self.sl)
        reward = abs(self.tp2 - self.entry)
        return reward / risk if risk > 1e-12 else 0.0


# ============================================================================
# SIGNAL PATHWAY: SMC LIQUIDITY REVERSAL
# ============================================================================

def build_pathway(symbol: str, bundle: dict, combo_name: str, regime: RegimeVector,
                   btc_bias: str, orderbook: dict, market_snap: dict) -> Optional[Candidate]:
    combo = COMBOS[combo_name]
    c_bias = bundle.get(combo["bias"], [])
    c_struct = bundle.get(combo["struct"], [])
    c_exec = bundle.get(combo["exec"], [])
    if len(c_bias) < 30 or len(c_struct) < 30 or len(c_exec) < 30:
        return None

    ind_bias = compute_indicators(c_bias)
    ind_struct = compute_indicators(c_struct)
    ind_exec = compute_indicators(c_exec)
    if not ind_bias or not ind_struct or not ind_exec:
        return None

    # --- HTF bias ---
    bias_closes = [c["c"] for c in c_bias]
    fast, slow = ind_bias["ema_fast"][-1], ind_bias["ema_slow"][-1]
    if fast > slow and bias_closes[-1] > fast:
        htf_dir = "bullish"
    elif fast < slow and bias_closes[-1] < fast:
        htf_dir = "bearish"
    else:
        htf_dir = "neutral"
    if htf_dir == "neutral":
        log.info("%s [%s] no signal: killer=HTF_BIAS_NEUTRAL", symbol, combo_name)
        return None

    pd_zone = premium_discount_zone(c_bias)

    # --- Structure timeframe: liquidity pools + sweep ---
    struct_swings = find_swings(c_struct)
    pools = build_liquidity_pools(struct_swings)
    sweep = detect_sweep(c_struct, pools, htf_dir)
    if not sweep:
        log.info("%s [%s] no signal: killer=NO_SWEEP bias=%s pd=%s", symbol, combo_name, htf_dir, pd_zone["zone"])
        return None

    # --- POI confluence: order blocks + FVGs on structure tf ---
    obs = find_order_blocks(c_struct, ind_struct["atr"], lookback=60)
    fvgs = find_fvgs(c_struct, lookback=60)
    obs = mark_untested(obs, c_struct)
    fvgs = mark_untested(fvgs, c_struct)
    same_dir_zones = [z for z in (obs + fvgs) if z.direction == htf_dir and not z.tested]
    if not same_dir_zones:
        log.info("%s [%s] near-miss: killer=NO_UNTESTED_POI bias=%s sweep_level=%.6g",
                  symbol, combo_name, htf_dir, sweep["level"])
        # not a hard block here (nearest_zone stays None below); MSS can still confirm

    last_price = c_exec[-1]["c"]
    nearest_zone = None
    if same_dir_zones:
        nearest_zone = min(same_dir_zones, key=lambda z: abs(z.mid() - last_price))

    # --- Execution timeframe: MSS confirmation ---
    mss = detect_mss(c_exec, htf_dir)
    if not mss:
        log.info("%s [%s] no signal: killer=NO_MSS bias=%s sweep_level=%.6g poi_found=%s",
                  symbol, combo_name, htf_dir, sweep["level"], bool(same_dir_zones))
        return None

    direction = "LONG" if htf_dir == "bullish" else "SHORT"

    atr_exec = ind_exec["atr"][-1]
    entry = last_price
    if nearest_zone:
        # anchor entry near the POI mid for better R:R, bounded by current price
        entry = nearest_zone.mid() if abs(nearest_zone.mid() - last_price) / last_price < 0.01 else last_price

    sl_buffer = atr_exec * 1.1
    if direction == "LONG":
        sl = min(sweep["level"], entry) - sl_buffer
        risk = entry - sl
        tp1 = entry + risk * 1.5
        tp2 = entry + risk * 2.75
    else:
        sl = max(sweep["level"], entry) + sl_buffer
        risk = sl - entry
        tp1 = entry - risk * 1.5
        tp2 = entry - risk * 2.75

    if risk <= 0:
        return None

    confluences = [f"HTF {combo['bias']} bias: {htf_dir}", f"Liquidity sweep @ {sweep['level']:.6g}",
                   f"Execution MSS confirmed ({combo['exec']})"]
    if nearest_zone:
        confluences.append(f"Untested {nearest_zone.kind} POI ({nearest_zone.direction})")
    confluences.append(f"Price in {pd_zone['zone']} zone (HTF range)")
    if btc_bias != "neutral":
        aligned = (btc_bias == "bullish" and direction == "LONG") or (btc_bias == "bearish" and direction == "SHORT")
        confluences.append(f"BTC regime {'aligned' if aligned else 'diverging'} ({btc_bias})")

    ob_imbalance = orderbook.get("imbalance", 0.0)
    if (direction == "LONG" and ob_imbalance > 0.1) or (direction == "SHORT" and ob_imbalance < -0.1):
        confluences.append(f"Orderbook imbalance supportive ({ob_imbalance:+.2f})")

    cand = Candidate(
        symbol=symbol, direction=direction, style=combo_name,
        entry=entry, sl=sl, tp1=tp1, tp2=tp2,
        confluences=confluences, setup_family="smc_liquidity_reversal",
    )
    return cand


# ============================================================================
# SCORING / CONFIDENCE / GRADING
# ============================================================================

def logistic(x: float) -> float:
    return 1 / (1 + math.exp(-x))


def setup_prior_winrate(state: dict, setup_family: str) -> float:
    hist = state["win_history"]["by_setup"].get(setup_family)
    if not hist or hist.get("n", 0) < 8:
        return 0.5
    return hist["wins"] / hist["n"]


def score_candidate(cand: Candidate, regime: RegimeVector, state: dict, btc_bias: str) -> float:
    score = 0.0
    # Base confluence count
    score += min(len(cand.confluences), 6) * 6.0

    # R:R quality
    rr = cand.rr()
    score += min(rr, 4.0) * 5.0

    # Regime favorability
    score += regime.composite_favorability() * 20.0

    # BTC alignment bonus/penalty (skip for BTC itself)
    if cand.symbol != "BTC" and btc_bias != "neutral":
        aligned = (btc_bias == "bullish" and cand.direction == "LONG") or \
                  (btc_bias == "bearish" and cand.direction == "SHORT")
        score += 6.0 if aligned else -8.0

    # Historical setup-family prior, mapped through logistic to damp extremes
    prior = setup_prior_winrate(state, cand.setup_family)
    score += (logistic((prior - 0.5) * 6) - 0.5) * 20.0

    return max(0.0, min(100.0, score))


def grade_for_confidence(conf: float) -> str:
    if conf >= 85:
        return "A+"
    if conf >= 75:
        return "A"
    if conf >= 65:
        return "B"
    return "C"


def classify_style_duration(style: str) -> str:
    return COMBOS[style]["hold_hint"]


# ============================================================================
# HARD FILTERS / DEDUP / COOLDOWN
# ============================================================================

def passes_hard_filters(symbol: str, market_snap: dict, min_day_vol_usd: float = 750_000,
                         min_oi_usd: float = 500_000) -> tuple[bool, str]:
    snap = market_snap.get(symbol)
    if not snap:
        return False, "no market snapshot"
    # Liquidity gate: pass on EITHER day volume OR open interest clearing its floor,
    # rather than requiring a single high day-volume bar. This matches the more
    # permissive OI-floor approach used elsewhere (e.g. Nyx's $500k OI floor) and
    # avoids over-filtering mid-cap alts that trade in bursts rather than
    # sustained volume (SEI/TIA/INJ/ORDI/ARB/OP/APT class names).
    vol_ok = snap.get("day_vol_usd", 0.0) >= min_day_vol_usd
    oi_ok = snap.get("oi_usd", 0.0) >= min_oi_usd
    if not (vol_ok or oi_ok):
        return False, f"insufficient liquidity (vol=${snap.get('day_vol_usd', 0.0):,.0f}, oi=${snap.get('oi_usd', 0.0):,.0f})"
    if abs(snap.get("funding", 0.0)) > 0.006:
        return False, "extreme funding"
    return True, "ok"


def check_cooldown(state: dict, symbol: str, direction: str) -> bool:
    """Returns True if a new signal is allowed (not in cooldown)."""
    recs = state["recent_by_symbol"].get(symbol, [])
    if not recs:
        return True
    now = int(time.time() * 1000)
    cooldown_ms = COOLDOWN_BARS_15M * TF_MS["15m"]
    for r in reversed(recs):
        if r["dir"] == direction and (now - r["t"]) < cooldown_ms:
            return False
    return True


def is_duplicate(state: dict, cand: Candidate) -> bool:
    recs = state["recent_by_symbol"].get(cand.symbol, [])
    for r in recs:
        if r["dir"] == cand.direction and abs(r["price"] - cand.entry) / cand.entry <= DEDUP_PRICE_TOL_PCT:
            return True
    return False


def concurrent_open_count(state: dict, symbol: str) -> int:
    return sum(1 for rec in state["signals"].values()
               if rec.get("symbol") == symbol and rec.get("status") == "open")


# ============================================================================
# SIZING
# ============================================================================

def position_size_pct(grade: str, style: str) -> float:
    return GRADE_SIZE_TABLE.get((grade, style), 0.25)


# ============================================================================
# TELEGRAM OUTPUT
# ============================================================================

def format_signal_message(cand: Candidate, snapshot: dict) -> str:
    arrow = "🟢 LONG" if cand.direction == "LONG" else "🔴 SHORT"
    risk_pct = position_size_pct(cand.grade, cand.style)
    lines = [
        f"⚡ *ZENITH PRIME SIGNAL* ⚡",
        f"",
        f"*{cand.symbol}-PERP*  {arrow}",
        f"Style: `{cand.style}`  |  Hold: `{classify_style_duration(cand.style)}`",
        f"Setup Grade: *{cand.grade}*  |  Confidence: *{cand.confidence:.1f}%*",
        f"",
        f"Entry: `{cand.entry:.6g}`",
        f"Stop Loss: `{cand.sl:.6g}`",
        f"TP1: `{cand.tp1:.6g}`",
        f"TP2: `{cand.tp2:.6g}`",
        f"R:R (TP2): `{cand.rr():.2f}`",
        f"Suggested Risk: `{risk_pct:.2f}%` of equity",
        f"",
        f"*Confluences:*",
    ]
    for cf in cand.confluences:
        lines.append(f"• {cf}")
    snap = snapshot.get(cand.symbol, {})
    if snap:
        lines.append(f"")
        lines.append(f"Funding: `{snap.get('funding', 0.0) * 100:.4f}%`  |  OI: `${snap.get('oi_usd', 0.0):,.0f}`")
    return "\n".join(lines)


def send_telegram(text: str) -> Optional[int]:
    if not TG_BOT_TOKEN or not TG_CHAT_ID:
        log.info("Telegram not configured; message suppressed:\n%s", text)
        return None
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    payload = json.dumps({
        "chat_id": TG_CHAT_ID, "text": text, "parse_mode": "Markdown",
    }).encode("utf-8")
    req = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            return data.get("result", {}).get("message_id")
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, ValueError) as e:
        log.error("Telegram send failed: %s", e)
        return None


# ============================================================================
# MAIN SCAN
# ============================================================================

def scan_symbol(symbol: str, state: dict, market_snap: dict, btc_bundle: dict,
                 btc_bias: str, btc_strength: float, breadth: float, threshold: float) -> Optional[Candidate]:
    ok, reason = passes_hard_filters(symbol, market_snap)
    if not ok:
        log.info("%s skipped: %s", symbol, reason)
        return None

    bundle = fetch_all_candles(symbol)
    if not bundle or any(len(bundle.get(tf, [])) < 30 for tf in ("15m", "1h", "4h", "1d")):
        log.info("%s skipped: insufficient candle history", symbol)
        return None

    regime = build_regime_vector(state, symbol, bundle, btc_bias, btc_strength, breadth)
    combo_name = select_combo(regime)

    orderbook = analyze_orderbook(symbol)
    cand = build_pathway(symbol, bundle, combo_name, regime, btc_bias, orderbook, market_snap)
    if not cand:
        return None

    if concurrent_open_count(state, symbol) >= MAX_CONCURRENT_PER_SYMBOL:
        log.info("%s skipped: max concurrent signals reached", symbol)
        return None
    if not check_cooldown(state, symbol, cand.direction):
        log.info("%s skipped: cooldown active", symbol)
        return None
    if is_duplicate(state, cand):
        log.info("%s skipped: duplicate of recent signal", symbol)
        return None

    cand.raw_score = score_candidate(cand, regime, state, btc_bias)
    cand.confidence = cand.raw_score
    cand.grade = grade_for_confidence(cand.confidence)

    if cand.confidence < threshold:
        log.info("%s below governor threshold (%.1f < %.1f)", symbol, cand.confidence, threshold)
        return None

    return cand


def record_signal(state: dict, cand: Candidate):
    now = int(time.time() * 1000)
    sid = f"{cand.symbol}-{cand.direction}-{now}"
    state["signals"][sid] = {
        "symbol": cand.symbol, "direction": cand.direction, "style": cand.style,
        "entry": cand.entry, "sl": cand.sl, "tp1": cand.tp1, "tp2": cand.tp2,
        "grade": cand.grade, "confidence": cand.confidence, "setup_family": cand.setup_family,
        "status": "open", "t": now,
    }
    state["recent_by_symbol"].setdefault(cand.symbol, []).append({
        "t": now, "dir": cand.direction, "price": cand.entry, "combo": cand.style,
    })


def run_scan():
    started = time.time()
    log.info("=== Zenith Prime scan started ===")
    state = load_state()
    market_snap = get_market_snapshot()

    btc_bundle = fetch_all_candles("BTC")
    btc_bias, btc_strength = compute_btc_regime(btc_bundle)
    log.info("BTC regime: %s (strength %.2f)", btc_bias, btc_strength)

    # crude breadth proxy: fraction of watchlist above their 1h EMA50
    breadth_hits, breadth_total = 0, 0
    breadth_cache = {}
    for sym in WATCHLIST:
        try:
            c1h = get_candles(sym, "1h", 60)
            if len(c1h) < 55:
                continue
            ind = compute_indicators(c1h)
            if not ind:
                continue
            breadth_total += 1
            if c1h[-1]["c"] > ind["ema_slow"][-1]:
                breadth_hits += 1
            breadth_cache[sym] = c1h
        except Exception as e:
            log.warning("breadth calc failed for %s: %s", sym, e)
    breadth = (breadth_hits / breadth_total) if breadth_total else 0.5

    threshold = governor_adjust_threshold(state)
    log.info("Governor threshold this scan: %.1f", threshold)

    produced = 0
    for symbol in WATCHLIST:
        try:
            cand = scan_symbol(symbol, state, market_snap, btc_bundle, btc_bias, btc_strength, breadth, threshold)
        except Exception as e:
            log.exception("Unhandled error scanning %s: %s", symbol, e)
            continue
        if cand:
            msg = format_signal_message(cand, market_snap)
            send_telegram(msg)
            record_signal(state, cand)
            produced += 1
            log.info("Signal produced: %s %s grade=%s conf=%.1f", cand.symbol, cand.direction, cand.grade, cand.confidence)

    prune_state(state)
    state["last_run_ms"] = int(time.time() * 1000)
    save_state(state)
    log.info("=== Scan complete: %d signal(s) produced in %.1fs ===", produced, time.time() - started)


if __name__ == "__main__":
    run_scan()
