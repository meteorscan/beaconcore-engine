
# KESTREL — Adaptive Smart-Money Signal Engine
# v1.1.1
#
# Maps liquidity and institutional footprints (Order Blocks, Breaker
# Blocks, Fair Value Gaps) across a multi-timeframe stack, waits for a
# liquidity sweep + structure shift, then scores the setup through five
# independent filters (Location, Context, Quality, RR, LTF-confirmation).

from __future__ import annotations

import os
import sys
import json
import math
import re
import time
import signal as os_signal
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

import requests

ENGINE_NAME = "KESTREL"
__version__ = "1.1.1"

# CONFIGURATION

TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN")
TG_CHAT_ID = os.getenv("TG_CHAT_ID")
if not TG_BOT_TOKEN:
    raise RuntimeError("TG_BOT_TOKEN environment variable is required")
if not TG_CHAT_ID:
    raise RuntimeError("TG_CHAT_ID environment variable is required")

STATE_FILE = os.getenv("STATE_FILE", "state.json")
SCAN_WORKERS = int(os.getenv("SCAN_WORKERS", "3"))
HL_BASE_URL = "https://api.hyperliquid.xyz/info"
HL_MIN_INTERVAL_S = float(os.getenv("HL_MIN_INTERVAL_S", "0.15"))

# ── WATCHLIST (unchanged infrastructure) ────────────────────────────────
WATCHLIST = [
    "BTCUSDT", "ETHUSDT", "HYPEUSDT", "ZECUSDT", "NEARUSDT",
    "ONDOUSDT", "SUIUSDT", "PENGUUSDT", "BNBUSDT", "SOLUSDT",
    "TRXUSDT", "BCHUSDT", "DOGEUSDT", "ADAUSDT", "DOTUSDT",
    "TAOUSDT", "AVAXUSDT", "LINKUSDT", "AAVEUSDT", "XRPUSDT",
    "XLMUSDT", "UNIUSDT", "LTCUSDT", "APTUSDT", "PENDLEUSDT",
]

MAJORS = {"BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT", "ADAUSDT", "DOGEUSDT"}

TF_MACRO, TF_HTF, TF_MID, TF_LTF = "1d", "4h", "1h", "15m"
TF_BARS = {TF_MACRO: 120, TF_HTF: 260, TF_MID: 300, TF_LTF: 300}
SCAN_INTERVAL_MIN = 15

# ── INDICATOR LENGTHS ──────────────────────────────────────────────────────
EMA_FAST, EMA_SLOW, EMA_TREND = 21, 50, 200
RSI_LEN, ATR_LEN, ADX_LEN, BB_LEN = 14, 14, 14, 20
BB_MULT = 2.0

# ── ZONE DETECTION ──────────────────────────────────────────────────────────
OB_DISPLACEMENT_ATR_MULT = 1.15      # min body/ATR to qualify as a displacement candle
OB_BOS_LOOKBACK = 25                 # bars scanned back for prior high/low to break
FVG_MIN_GAP_ATR_MULT = 0.12
ZONE_MAX_WIDTH_ATR_MULT = 1.8        # zones wider than this are too "fat" to be tradable POIs
ZONE_LOOKBACK_HTF = 90
ZONE_LOOKBACK_LTF = 80
PIVOT_LEFT_HTF, PIVOT_RIGHT_HTF = 2, 2
PIVOT_LEFT_LTF, PIVOT_RIGHT_LTF = 2, 2
LIQUIDITY_EQ_TOLERANCE_PCT = 0.0018

# ── SWEEP / MSS ──────────────────────────────────────────────────────────
SWEEP_LOOKBACK_MID = 16
SWEEP_MAX_DEPTH_ATR_MULT = 1.10
SWEEP_MIN_WICK_RATIO = 0.35
MSS_LOOKBACK_LTF = 40
MSS_DISPLACEMENT_ATR_MULT = 0.55
MSS_MIN_CLOSE_MARGIN_ATR_MULT = 0.08
BREAKER_SEARCH_BARS = 8

# ── RISK ─────────────────────────────────────────────────────────────────
MIN_RR_FLOOR = 1.5
MIN_RR_TARGET = 2.0
EXT_RR_LEVELS = [2.0, 2.5, 3.0, 4.0, 5.0]
TP2_MIN_RR_DELTA = 0.3
SL_BUFFER_ATR_MIN_MULT = 0.25
SL_BUFFER_ATR_MAX_MULT = 0.85
LIQUIDITY_ROOM_BUFFER_ATR_MULT = 0.25
POI_MAX_DIST_ATR_MULT = 1.4          # max distance entry POI can sit from live price
POI_MAX_PCT_OF_PRICE = 0.02

# ── VOLATILITY / LIQUIDITY GATES ───────────────────────────────────────────
MIN_ATR_PCT = 0.20
MAX_ATR_PCT = 9.0
SPREAD_WARN_PCT = 0.20
SPREAD_SUPPRESS_PCT = 0.45
SPREAD_EXEMPT = MAJORS
MIN_OI_USD = 400_000.0

# ── SCORING / FREQUENCY GOVERNOR ────────────────────────────────────────────
BASE_MIN_CONFIDENCE = 58.0           # 0-100 scale; adaptively nudged by governor
MAX_SIGNALS_PER_SCAN_DEFAULT = 4
MAX_SIGNALS_PER_SCAN_TRENDING = 6
MAX_CONCURRENT_ACTIVE_SIGNALS = 14
MAX_SIGNAL_HISTORY = 1500
COOLDOWN_BARS_LTF = 3                # bars (of TF_LTF) before same symbol/direction can re-fire
PENDING_ENTRY_EXPIRY_BARS = 8         # bars (of TF_LTF) a signal can sit unfilled before it's cancelled --
                                       # 8 * 15m = 2h; the POI is assumed stale/invalidated past this
DUPLICATE_ENTRY_TOLERANCE_PCT = 0.0035

# ── ADAPTIVE GOVERNOR ────────────────────────────────────────────────────
GOVERNOR_LOOKBACK_SIGNALS = 40
GOVERNOR_MAX_SHIFT = 8.0             # max +/- confidence-threshold points the governor may apply
GOVERNOR_TARGET_WINRATE = 0.50

# ── FUNDING / OI / RELATIVE STRENGTH ────────────────────────────────────
FUNDING_EXTREME = 0.0010
FUNDING_CARRY_THRESHOLD = 0.0005
OI_HISTORY_DEPTH = 6
OI_CHANGE_THRESHOLD_PCT = 1.0
RS_TOP_PCTILE, RS_BOTTOM_PCTILE = 0.20, 0.20

# ── SESSION WEIGHTING (UTC hours) ───────────────────────────────────────
SESSION_WINDOWS = {
    "asia":   (0, 8),
    "london": (7, 12),
    "ny":     (12, 21),
    "off":    (21, 24),
}
SESSION_SCORE_BONUS = {"asia": 0.0, "london": 2.0, "ny": 2.5, "off": -1.5}

# HYPERLIQUID API CLIENT

_session = requests.Session()
_req_lock = threading.Lock()
_last_req_ts = 0.0


def _throttle():
    global _last_req_ts
    with _req_lock:
        wait = HL_MIN_INTERVAL_S - (time.time() - _last_req_ts)
        if wait > 0:
            time.sleep(wait)
        _last_req_ts = time.time()


def hl_post(payload: dict, retries: int = 4, timeout: int = 12) -> dict | list | None:
    for attempt in range(retries):
        _throttle()
        try:
            r = _session.post(HL_BASE_URL, json=payload, timeout=timeout)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (429, 502, 503, 504):
                time.sleep(0.5 * (attempt + 1))
                continue
            return None
        except requests.RequestException:
            time.sleep(0.4 * (attempt + 1))
    return None


def hl_coin(symbol: str) -> str:
    return symbol[:-4] if symbol.endswith("USDT") else symbol


def current_bar_open_ms(reference_ms: int, interval: str) -> int:
    mult = {"15m": 15, "1h": 60, "4h": 240, "1d": 1440}[interval] * 60_000
    return (reference_ms // mult) * mult


def filter_closed_candles(candles: list[dict], interval: str, reference_ms: int) -> list[dict]:
    open_ms = current_bar_open_ms(reference_ms, interval)
    return [c for c in candles if c["t"] < open_ms]


def get_candles(symbol: str, interval: str, n: int, reference_ms: int) -> list[dict] | None:
    coin = hl_coin(symbol)
    end = reference_ms
    start = end - n * {"15m": 15, "1h": 60, "4h": 240, "1d": 1440}[interval] * 60_000 * 2
    payload = {
        "type": "candleSnapshot",
        "req": {"coin": coin, "interval": interval, "startTime": start, "endTime": end},
    }
    raw = hl_post(payload)
    if not raw or not isinstance(raw, list):
        return None
    candles = [
        {"t": c["t"], "o": float(c["o"]), "h": float(c["h"]),
         "l": float(c["l"]), "c": float(c["c"]), "v": float(c["v"])}
        for c in raw
    ]
    candles = filter_closed_candles(candles, interval, reference_ms)
    return candles[-n:] if len(candles) >= 5 else None


def fetch_all_candles(symbol: str, reference_ms: int) -> dict[str, list[dict]] | None:
    out = {}
    for tf in (TF_LTF, TF_MID, TF_HTF, TF_MACRO):
        c = get_candles(symbol, tf, TF_BARS[tf], reference_ms)
        if c is None or len(c) < 40:
            return None
        out[tf] = c
    return out


_meta_ctx_cache: dict | None = None
_meta_ctx_lock = threading.Lock()


def get_meta_and_asset_ctxs() -> dict | None:
    global _meta_ctx_cache
    raw = hl_post({"type": "metaAndAssetCtxs"})
    if not raw or not isinstance(raw, list) or len(raw) < 2:
        return _meta_ctx_cache
    universe = raw[0].get("universe", [])
    ctxs = raw[1]
    out = {}
    for meta, ctx in zip(universe, ctxs):
        name = meta.get("name")
        if not name:
            continue
        try:
            out[name] = {
                "funding": float(ctx.get("funding", 0.0)),
                "oi": float(ctx.get("openInterest", 0.0)),
                "mark_px": float(ctx.get("markPx", 0.0)),
                "day_vol_usd": float(ctx.get("dayNtlVlm", 0.0)),
            }
        except (TypeError, ValueError):
            continue
    with _meta_ctx_lock:
        _meta_ctx_cache = out
    return out


def get_l2_spread_pct(symbol: str) -> float | None:
    coin = hl_coin(symbol)
    raw = hl_post({"type": "l2Book", "coin": coin})
    if not raw or "levels" not in raw:
        return None
    try:
        bids, asks = raw["levels"][0], raw["levels"][1]
        if not bids or not asks:
            return None
        best_bid, best_ask = float(bids[0]["px"]), float(asks[0]["px"])
        mid = (best_bid + best_ask) / 2
        return ((best_ask - best_bid) / mid) * 100 if mid > 0 else None
    except (KeyError, IndexError, ValueError, TypeError):
        return None

# MATH / INDICATORS

def safe(v, fb=0.0):
    try:
        if v is None or math.isnan(v) or math.isinf(v):
            return fb
        return v
    except TypeError:
        return fb


def safe_div(a: float, b: float, default: float = 0.0) -> float:
    return a / b if b else default


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
        m = sum(window) / len(window)
        out.append(math.sqrt(sum((x - m) ** 2 for x in window) / len(window)))
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
        rs = safe_div(avg_g, avg_l, 999.0)
        out.append(100 - 100 / (1 + rs) if avg_l > 0 else 100.0)
    return out


def atr_series(candles: list[dict], period: int = ATR_LEN) -> list[float]:
    if not candles:
        return []
    trs = [candles[0]["h"] - candles[0]["l"]]
    for i in range(1, len(candles)):
        h, l, pc = candles[i]["h"], candles[i]["l"], candles[i - 1]["c"]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    out = [trs[0]]
    for i in range(1, len(trs)):
        out.append((out[-1] * (period - 1) + trs[i]) / period)
    return out


def adx_series(candles: list[dict], period: int = ADX_LEN) -> list[float]:
    n = len(candles)
    if n < period + 2:
        return [15.0] * n
    plus_dm, minus_dm, trs = [0.0], [0.0], [candles[0]["h"] - candles[0]["l"]]
    for i in range(1, n):
        up = candles[i]["h"] - candles[i - 1]["h"]
        dn = candles[i - 1]["l"] - candles[i]["l"]
        plus_dm.append(up if (up > dn and up > 0) else 0.0)
        minus_dm.append(dn if (dn > up and dn > 0) else 0.0)
        h, l, pc = candles[i]["h"], candles[i]["l"], candles[i - 1]["c"]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))

    def wilder_smooth(vals):
        out = [sum(vals[:period])]
        for v in vals[period:]:
            out.append(out[-1] - out[-1] / period + v)
        return out

    tr_s = wilder_smooth(trs)
    pdm_s = wilder_smooth(plus_dm)
    mdm_s = wilder_smooth(minus_dm)
    dx = []
    for i in range(len(tr_s)):
        pdi = 100 * safe_div(pdm_s[i], tr_s[i])
        mdi = 100 * safe_div(mdm_s[i], tr_s[i])
        dx.append(100 * safe_div(abs(pdi - mdi), pdi + mdi))
    pad = [dx[0] if dx else 15.0] * (n - len(dx))
    full_dx = pad + dx
    adx = sma(full_dx, period)
    return adx


def bb_width_pct(closes: list[float], period: int = BB_LEN, mult: float = BB_MULT) -> list[float]:
    mid = sma(closes, period)
    sd = stdev(closes, period)
    out = []
    for i in range(len(closes)):
        upper, lower = mid[i] + mult * sd[i], mid[i] - mult * sd[i]
        out.append(safe_div(upper - lower, mid[i]) * 100 if mid[i] else 0.0)
    return out


def percentile_rank(vals: list[float], x: float) -> float:
    if not vals:
        return 0.5
    below = sum(1 for v in vals if v <= x)
    return below / len(vals)


def session_now(reference_ms: int) -> str:
    hour = datetime.fromtimestamp(reference_ms / 1000, tz=timezone.utc).hour
    for name, (start, end) in SESSION_WINDOWS.items():
        if start <= hour < end:
            return name
    return "off"


def compute_returns(candles: list[dict], lookback: int) -> list[float]:
    closes = [c["c"] for c in candles[-lookback:]]
    return [safe_div(closes[i] - closes[i - 1], closes[i - 1]) for i in range(1, len(closes))]


def pearson(a: list[float], b: list[float]) -> float:
    n = min(len(a), len(b))
    if n < 5:
        return 0.0
    a, b = a[-n:], b[-n:]
    ma, mb = sum(a) / n, sum(b) / n
    cov = sum((a[i] - ma) * (b[i] - mb) for i in range(n))
    va = math.sqrt(sum((x - ma) ** 2 for x in a))
    vb = math.sqrt(sum((x - mb) ** 2 for x in b))
    return safe_div(cov, va * vb)

# MARKET STRUCTURE — SWINGS, BOS/CHoCH, BIAS

@dataclass
class Swing:
    index: int
    price: float
    kind: str  # "high" | "low"


def find_swings(candles: list[dict], left: int = 2, right: int = 2) -> list[Swing]:
    out = []
    n = len(candles)
    for i in range(left, n - right):
        wh = [candles[j]["h"] for j in range(i - left, i + right + 1)]
        wl = [candles[j]["l"] for j in range(i - left, i + right + 1)]
        h, l = candles[i]["h"], candles[i]["l"]
        if h == max(wh) and wh.count(h) == 1:
            out.append(Swing(i, h, "high"))
        if l == min(wl) and wl.count(l) == 1:
            out.append(Swing(i, l, "low"))
    return out


@dataclass
class StructureState:
    bias: str            # "bullish" | "bearish" | "neutral"
    range_high: float
    range_low: float
    eq: float             # equilibrium (premium/discount split)


def analyze_structure(candles: list[dict], swings: list[Swing]) -> Optional[StructureState]:
    highs = sorted([s for s in swings if s.kind == "high"], key=lambda s: s.index)
    lows = sorted([s for s in swings if s.kind == "low"], key=lambda s: s.index)
    if len(highs) < 2 or len(lows) < 2:
        return None
    last_h, prev_h = highs[-1], highs[-2]
    last_l, prev_l = lows[-1], lows[-2]

    if last_h.price > prev_h.price and last_l.price > prev_l.price:
        bias = "bullish"
    elif last_h.price < prev_h.price and last_l.price < prev_l.price:
        bias = "bearish"
    else:
        bias = "neutral"

    close = candles[-1]["c"]
    # CHoCH: a decisive close beyond the most recent opposing swing flips bias
    if bias in ("bullish", "neutral") and close < last_l.price:
        bias = "bearish"
    elif bias in ("bearish", "neutral") and close > last_h.price:
        bias = "bullish"

    range_high = max(last_h.price, prev_h.price)
    range_low = min(last_l.price, prev_l.price)
    if range_high <= range_low:
        return None
    return StructureState(bias, range_high, range_low, (range_high + range_low) / 2)


def price_zone(price: float, structure: StructureState) -> str:
    return "premium" if price >= structure.eq else "discount"


# REGIME DETECTION

@dataclass
class Regime:
    label: str            # "trend" | "range" | "reversal" | "volatile"
    direction: str         # "bullish" | "bearish" | "neutral"
    adx: float
    bbw_pctile: float
    atr_pct: float
    strength: float        # 0-1 composite trend strength


def classify_regime(candles_htf: list[dict], candles_mid: list[dict]) -> Regime:
    closes_htf = [c["c"] for c in candles_htf]
    adx_htf = adx_series(candles_htf, ADX_LEN)[-1]
    bbw = bb_width_pct(closes_htf, BB_LEN, BB_MULT)
    bbw_now = bbw[-1]
    bbw_pctile = percentile_rank(bbw[-60:], bbw_now)
    atr_htf = atr_series(candles_htf, ATR_LEN)[-1]
    atr_pct = safe_div(atr_htf, candles_htf[-1]["c"]) * 100

    ema_fast = ema(closes_htf, EMA_FAST)[-1]
    ema_slow = ema(closes_htf, EMA_SLOW)[-1]
    ema_trend = ema(closes_htf, EMA_TREND)[-1] if len(closes_htf) >= EMA_TREND else ema_slow
    price = closes_htf[-1]

    if price > ema_fast > ema_slow > ema_trend:
        direction, align = "bullish", 1.0
    elif price < ema_fast < ema_slow < ema_trend:
        direction, align = "bearish", 1.0
    elif price > ema_slow:
        direction, align = "bullish", 0.5
    elif price < ema_slow:
        direction, align = "bearish", 0.5
    else:
        direction, align = "neutral", 0.0

    swings = find_swings(candles_mid, PIVOT_LEFT_HTF, PIVOT_RIGHT_HTF)
    structure = analyze_structure(candles_mid, swings)
    struct_bias = structure.bias if structure else "neutral"

    strength = min(1.0, (adx_htf / 40.0) * 0.6 + align * 0.4)

    if adx_htf >= 24 and align >= 0.5 and struct_bias == direction:
        label = "trend"
    elif adx_htf < 18 and bbw_pctile < 0.45:
        label = "range"
    elif bbw_pctile > 0.85 or atr_pct > MAX_ATR_PCT * 0.7:
        label = "volatile"
    else:
        label = "reversal"

    return Regime(label, direction, adx_htf, bbw_pctile, atr_pct, strength)

# SMC ZONE ENGINE — ORDER BLOCKS / BREAKER BLOCKS / FAIR VALUE GAPS

@dataclass
class Zone:
    low: float
    high: float
    kind: str          # "demand" | "supply"
    origin: str         # "ob" | "breaker" | "fvg"
    index: int
    displacement_atr: float = 0.0    # displacement move / ATR at formation (quality input)
    vol_ratio: float = 1.0            # formation-candle volume vs local average (quality input)
    mitigated: bool = False           # True once price has traded back through & closed beyond
    flipped: bool = False             # True once a mitigated OB has been reclassified as a breaker

    @property
    def mid(self) -> float:
        return (self.low + self.high) / 2

    @property
    def width(self) -> float:
        return self.high - self.low

    def contains(self, price: float, buf: float = 0.0) -> bool:
        return (self.low - buf) <= price <= (self.high + buf)


def _avg_volume(candles: list[dict], idx: int, window: int = 20) -> float:
    lo = max(0, idx - window)
    seg = candles[lo:idx]
    return (sum(c["v"] for c in seg) / len(seg)) if seg else (candles[idx]["v"] or 1.0)


def find_order_blocks(candles: list[dict], atr_vals: list[float], lookback: int) -> list[Zone]:
    """Last opposite-colour candle immediately before a displacement move
    that breaks prior structure. Requires the displacement candle's body to
    clear OB_DISPLACEMENT_ATR_MULT * ATR and to close beyond the recent
    swing extreme (i.e. this is structurally significant, not just a big
    candle)."""
    zones = []
    n = len(candles)
    start = max(2, n - lookback)
    for i in range(start, n):
        c = candles[i]
        a = atr_vals[i] or 1e-9
        body = c["c"] - c["o"]
        if abs(body) < OB_DISPLACEMENT_ATR_MULT * a:
            continue
        back_lo = max(0, i - OB_BOS_LOOKBACK)
        if body > 0:
            prior_high = max((candles[j]["h"] for j in range(back_lo, i)), default=c["h"])
            if c["c"] <= prior_high:
                continue
            for j in range(i - 1, back_lo - 1, -1):
                ob = candles[j]
                if ob["c"] < ob["o"]:
                    zones.append(Zone(ob["l"], ob["h"], "demand", "ob", j,
                                       displacement_atr=abs(body) / a,
                                       vol_ratio=safe_div(c["v"], _avg_volume(candles, i), 1.0)))
                    break
        else:
            prior_low = min((candles[j]["l"] for j in range(back_lo, i)), default=c["l"])
            if c["c"] >= prior_low:
                continue
            for j in range(i - 1, back_lo - 1, -1):
                ob = candles[j]
                if ob["c"] > ob["o"]:
                    zones.append(Zone(ob["l"], ob["h"], "supply", "ob", j,
                                       displacement_atr=abs(body) / a,
                                       vol_ratio=safe_div(c["v"], _avg_volume(candles, i), 1.0)))
                    break
    # drop zones that are too wide to be a usable POI
    atr_last = atr_vals[-1] or 1e-9
    zones = [z for z in zones if z.width <= ZONE_MAX_WIDTH_ATR_MULT * atr_last]
    return zones[-14:]


def find_fvgs(candles: list[dict], atr_vals: list[float], lookback: int) -> list[Zone]:
    zones = []
    n = len(candles)
    start = max(2, n - lookback)
    for i in range(start, n):
        a, b, c = candles[i - 2], candles[i - 1], candles[i]
        atrv = atr_vals[i] or 1e-9
        if a["h"] < c["l"] and (c["l"] - a["h"]) >= FVG_MIN_GAP_ATR_MULT * atrv:
            zones.append(Zone(a["h"], c["l"], "demand", "fvg", i - 1,
                               displacement_atr=abs(b["c"] - b["o"]) / atrv,
                               vol_ratio=safe_div(b["v"], _avg_volume(candles, i - 1), 1.0)))
        elif a["l"] > c["h"] and (a["l"] - c["h"]) >= FVG_MIN_GAP_ATR_MULT * atrv:
            zones.append(Zone(c["h"], a["l"], "supply", "fvg", i - 1,
                               displacement_atr=abs(b["c"] - b["o"]) / atrv,
                               vol_ratio=safe_div(b["v"], _avg_volume(candles, i - 1), 1.0)))
    return zones[-14:]


def mark_mitigation_and_breakers(zones: list[Zone], candles: list[dict]) -> list[Zone]:
    """Walks forward from each zone's formation bar. The first candle that
    trades back into the zone and CLOSES beyond its far edge mitigates it.
    An Order Block that gets mitigated is immediately reclassified as a
    Breaker Block of the opposite polarity (institutional supply that fails
    becomes new demand, and vice versa) — this is what lets the engine use
    the same swing structure on both sides of a flip instead of hunting for
    a second, unrelated pattern."""
    out = []
    for z in zones:
        zc = Zone(z.low, z.high, z.kind, z.origin, z.index,
                   z.displacement_atr, z.vol_ratio)
        for c in candles[z.index + 1:]:
            touched = c["l"] <= zc.high and c["h"] >= zc.low
            if not touched:
                continue
            closed_through = (c["c"] > zc.high) if zc.kind == "supply" else (c["c"] < zc.low)
            if closed_through:
                zc.mitigated = True
                if zc.origin == "ob":
                    zc.flipped = True
                    zc.kind = "demand" if zc.kind == "supply" else "supply"
                    zc.origin = "breaker"
                break
        out.append(zc)
    return out


def zone_quality(z: Zone) -> float:
    """0-1 composite: displacement strength, formation volume, freshness,
    tightness, and a bonus for breaker-block origin (a breaker carries the
    extra evidence that the level was defended, broken, and is now being
    retested from the other side — a stronger footprint than a raw OB)."""
    disp_score = min(1.0, z.displacement_atr / 2.2)
    vol_score = min(1.0, z.vol_ratio / 1.8)
    fresh_score = 0.0 if z.mitigated and not z.flipped else 1.0
    tight_score = 1.0 - min(1.0, z.width / (z.width + 1e-9)) if z.width <= 0 else \
        max(0.0, 1.0 - (z.width / (max(z.width, 1e-9) * 2.5)))
    tight_score = 0.7  # neutral baseline; refined by caller with ATR context
    origin_bonus = 0.12 if z.origin in ("breaker", "fvg") else 0.0
    q = 0.35 * disp_score + 0.25 * vol_score + 0.20 * fresh_score + 0.20 * tight_score + origin_bonus
    return max(0.0, min(1.0, q))


def cluster_levels(levels: list[float], tol_pct: float = LIQUIDITY_EQ_TOLERANCE_PCT) -> list[tuple[float, int]]:
    if not levels:
        return []
    levels = sorted(levels)
    clusters, cur = [], [levels[0]]
    for lv in levels[1:]:
        if abs(lv - cur[-1]) / max(cur[-1], 1e-9) <= tol_pct:
            cur.append(lv)
        else:
            clusters.append((sum(cur) / len(cur), len(cur)))
            cur = [lv]
    clusters.append((sum(cur) / len(cur), len(cur)))
    return clusters


def build_liquidity_pools(swings: list[Swing], candles_macro: list[dict]) -> dict:
    highs = [s.price for s in swings if s.kind == "high"]
    lows = [s.price for s in swings if s.kind == "low"]
    pools = {"resistance": cluster_levels(highs), "support": cluster_levels(lows)}
    if len(candles_macro) >= 2:
        prev_day = candles_macro[-2]
        pools["resistance"].append((prev_day["h"], 1))
        pools["support"].append((prev_day["l"], 1))
    if len(candles_macro) >= 8:
        week = candles_macro[-8:-1]
        pools["resistance"].append((max(c["h"] for c in week), 1))
        pools["support"].append((min(c["l"] for c in week), 1))
    return pools


def detect_sweep(candles: list[dict], pools: dict, direction: str, atr_vals: list[float],
                  lookback: int) -> Optional[dict]:
    """A sweep: price wicks beyond a liquidity pool (stop-hunt) and closes
    back inside it with a rejection wick — the raw fuel for a reversal."""
    window_start = max(0, len(candles) - lookback)
    targets = pools["support"] if direction == "long" else pools["resistance"]
    best = None
    for i in range(window_start, len(candles)):
        c = candles[i]
        a = atr_vals[i] or 1e-9
        rng = c["h"] - c["l"]
        if rng <= 0:
            continue
        for level, touches in targets:
            if direction == "long":
                swept = c["l"] < level and (level - c["l"]) <= SWEEP_MAX_DEPTH_ATR_MULT * a
                rejected = c["c"] > level
                wick_ratio = safe_div(min(c["o"], c["c"]) - c["l"], rng)
            else:
                swept = c["h"] > level and (c["h"] - level) <= SWEEP_MAX_DEPTH_ATR_MULT * a
                rejected = c["c"] < level
                wick_ratio = safe_div(c["h"] - max(c["o"], c["c"]), rng)
            if swept and rejected and wick_ratio >= SWEEP_MIN_WICK_RATIO:
                cand = {"level": level, "touches": touches, "index": i,
                        "extreme": c["l"] if direction == "long" else c["h"], "t": c["t"]}
                if best is None or cand["index"] >= best["index"]:
                    best = cand
    return best


@dataclass
class MSSEvent:
    direction: str
    impulse_index: int
    swing_price: float


def detect_mss(candles: list[dict], direction: str, after_index: int,
                atr_vals: list[float]) -> Optional[MSSEvent]:
    """Market Structure Shift on the LTF: a displacement close beyond the
    most recent opposing swing, confirming the reversal signalled by the
    sweep is actually taking hold before any entry commits."""
    post = candles[after_index + 1:]
    if len(post) < (PIVOT_LEFT_LTF + PIVOT_RIGHT_LTF + 3):
        return None
    offset = after_index + 1
    pivots = find_swings(post, PIVOT_LEFT_LTF, PIVOT_RIGHT_LTF)
    if direction == "long":
        swing_highs = sorted([p for p in pivots if p.kind == "high"], key=lambda p: p.index)
        if not swing_highs:
            return None
        for idx in range(swing_highs[0].index + 1, len(post)):
            relevant = [p for p in swing_highs if p.index < idx]
            if not relevant:
                continue
            sp = relevant[-1].price
            c = post[idx]
            a = atr_vals[offset + idx] or 1e-9
            disp = (c["c"] - c["o"]) >= MSS_DISPLACEMENT_ATR_MULT * a
            margin = (c["c"] - sp) >= MSS_MIN_CLOSE_MARGIN_ATR_MULT * a
            if c["c"] > sp and disp and margin:
                return MSSEvent("long", offset + idx, sp)
        return None
    else:
        swing_lows = sorted([p for p in pivots if p.kind == "low"], key=lambda p: p.index)
        if not swing_lows:
            return None
        for idx in range(swing_lows[0].index + 1, len(post)):
            relevant = [p for p in swing_lows if p.index < idx]
            if not relevant:
                continue
            sp = relevant[-1].price
            c = post[idx]
            a = atr_vals[offset + idx] or 1e-9
            disp = (c["o"] - c["c"]) >= MSS_DISPLACEMENT_ATR_MULT * a
            margin = (sp - c["c"]) >= MSS_MIN_CLOSE_MARGIN_ATR_MULT * a
            if c["c"] < sp and disp and margin:
                return MSSEvent("short", offset + idx, sp)
        return None


def find_ltf_breaker(candles: list[dict], mss: MSSEvent) -> Optional[Zone]:
    """LTF entry trigger: the last opposite-colour candle before the MSS
    impulse leg — the breaker block that, once retested, is the precision
    entry point ("LTF -> BBs" per the required best practice)."""
    lo = max(0, mss.impulse_index - BREAKER_SEARCH_BARS)
    if mss.direction == "long":
        for j in range(mss.impulse_index - 1, lo - 1, -1):
            c = candles[j]
            if c["c"] < c["o"]:
                return Zone(c["l"], c["h"], "demand", "breaker", j, displacement_atr=1.5, vol_ratio=1.2)
    else:
        for j in range(mss.impulse_index - 1, lo - 1, -1):
            c = candles[j]
            if c["c"] > c["o"]:
                return Zone(c["l"], c["h"], "supply", "breaker", j, displacement_atr=1.5, vol_ratio=1.2)
    return None

# VOLUME PROFILE (TP clipping to real volume-built levels)

def volume_profile(candles: list[dict], bins: int = 24) -> dict:
    hi = max(c["h"] for c in candles)
    lo = min(c["l"] for c in candles)
    if hi <= lo:
        return {"poc": hi, "vah": hi, "val": lo}
    step = (hi - lo) / bins
    buckets = [0.0] * bins
    for c in candles:
        idx = min(bins - 1, max(0, int((((c["h"] + c["l"] + c["c"]) / 3) - lo) / step)))
        buckets[idx] += c["v"]
    poc_idx = max(range(bins), key=lambda i: buckets[i])
    poc = lo + (poc_idx + 0.5) * step
    total = sum(buckets) or 1.0
    target = total * 0.70
    lo_i = hi_i = poc_idx
    acc = buckets[poc_idx]
    while acc < target and (lo_i > 0 or hi_i < bins - 1):
        exp_lo = buckets[lo_i - 1] if lo_i > 0 else -1
        exp_hi = buckets[hi_i + 1] if hi_i < bins - 1 else -1
        if exp_hi >= exp_lo:
            hi_i += 1
            acc += buckets[hi_i]
        else:
            lo_i -= 1
            acc += buckets[lo_i]
    return {"poc": poc, "vah": lo + (hi_i + 1) * step, "val": lo + lo_i * step}


def clip_tp_to_liquidity(entry: float, tp: float, direction: str, pools: dict,
                          vp: dict, zones: list[Zone]) -> float:
    candidates = [lv for lv, _ in (pools["resistance"] if direction == "long" else pools["support"])
                  if (lv > entry if direction == "long" else lv < entry)]
    candidates += [v for v in (vp["poc"], vp["vah"], vp["val"])
                   if (v > entry if direction == "long" else v < entry)]
    for z in zones:
        edge = z.low if direction == "long" else z.high
        wanted_kind = "supply" if direction == "long" else "demand"
        if z.kind == wanted_kind and (edge > entry if direction == "long" else edge < entry):
            candidates.append(edge)
    if not candidates:
        return tp
    nearest = min(candidates, key=lambda lv: abs(lv - tp))
    if abs(nearest - tp) / max(abs(tp - entry), 1e-9) < 0.4:
        return nearest
    return tp


# CANDIDATE SETUP + RISK CONSTRUCTION

@dataclass
class Candidate:
    symbol: str
    direction: str            # "long" | "short"
    pathway: str               # "liquidity_reversal" | "trend_continuation" | "range_reversion"
    entry: float
    sl: float
    tp1: float
    tp2: float
    zone: Zone
    confluences: list[str] = field(default_factory=list)
    atr_val: float = 0.0
    zone_q: float = 0.0
    context_ok: bool = True
    ltf_confirmed: bool = False
    regime: Optional[Regime] = None
    tp2_extended: bool = True   # False when tp2 couldn't clear tp1 by TP2_MIN_RR_DELTA (no room)

    def rr1(self) -> float:
        risk = abs(self.entry - self.sl)
        return safe_div(abs(self.tp1 - self.entry), risk)

    def rr2(self) -> float:
        risk = abs(self.entry - self.sl)
        return safe_div(abs(self.tp2 - self.entry), risk)


def adaptive_sl_buffer(candles: list[dict], atr_val: float, vol_pctile: float) -> float:
    window = candles[-20:]
    if len(window) < 5:
        base = atr_val * SL_BUFFER_ATR_MIN_MULT
    else:
        wicks = []
        for c in window:
            top, bot = max(c["o"], c["c"]), min(c["o"], c["c"])
            wicks.append(max(c["h"] - top, bot - c["l"]))
        avg_wick = sum(wicks) / len(wicks)
        base = max(atr_val * SL_BUFFER_ATR_MIN_MULT, avg_wick * 1.3)
    vol_scale = 1.0 + 0.5 * max(0.0, vol_pctile - 0.5)
    return min(base * vol_scale, atr_val * SL_BUFFER_ATR_MAX_MULT)


def clamp_entry_to_market(entry: float, sl: float, tp1: float, tp2: float,
                           market_price: float, atr_val: float) -> tuple[float, float, float, float]:
    max_dist = min(atr_val * POI_MAX_DIST_ATR_MULT, market_price * POI_MAX_PCT_OF_PRICE)
    dist = entry - market_price
    if abs(dist) <= max_dist or market_price <= 0:
        return entry, sl, tp1, tp2
    target_dist = max_dist if dist > 0 else -max_dist
    shift = target_dist - dist
    return entry + shift, sl + shift, tp1 + shift, tp2 + shift


def room_to_next_opposing_level(entry: float, direction: str, zones: list[Zone],
                                 pools: dict) -> Optional[float]:
    candidates = []
    for z in zones:
        if direction == "long" and z.kind == "supply" and z.low > entry:
            candidates.append(z.low)
        if direction == "short" and z.kind == "demand" and z.high < entry:
            candidates.append(z.high)
    for lv, _ in (pools["resistance"] if direction == "long" else pools["support"]):
        if (lv > entry) if direction == "long" else (lv < entry):
            candidates.append(lv)
    if not candidates:
        return None
    return (min(candidates) - entry) if direction == "long" else (entry - max(candidates))


def build_risk_plan(direction: str, entry: float, invalidation: float, atr_val: float,
                     vol_pctile: float, candles_ltf: list[dict], zones: list[Zone],
                     pools: dict) -> Optional[tuple]:
    buf = adaptive_sl_buffer(candles_ltf, atr_val, vol_pctile)
    sl = invalidation - buf if direction == "long" else invalidation + buf
    risk = abs(entry - sl)
    if risk <= 0:
        return None

    room = room_to_next_opposing_level(entry, direction, zones, pools)

    tp1_rr = MIN_RR_TARGET
    if room is not None:
        wall_room = room - LIQUIDITY_ROOM_BUFFER_ATR_MULT * atr_val
        if wall_room > 0:
            wall_rr = safe_div(wall_room, risk)
            if wall_rr < MIN_RR_TARGET:
                tp1_rr = max(MIN_RR_FLOOR, wall_rr)
    tp1 = entry + tp1_rr * risk if direction == "long" else entry - tp1_rr * risk

    best_rr = MIN_RR_TARGET
    if room is not None:
        usable = room - LIQUIDITY_ROOM_BUFFER_ATR_MULT * atr_val
        for rr in EXT_RR_LEVELS:
            if usable >= rr * risk:
                best_rr = rr
        continuous_rr = safe_div(usable, risk)
        if continuous_rr > best_rr:
            best_rr = min(continuous_rr, EXT_RR_LEVELS[-1])
    tp2 = entry + best_rr * risk if direction == "long" else entry - best_rr * risk
    tp2_extended = (best_rr - tp1_rr) >= TP2_MIN_RR_DELTA

    if direction == "long" and not (sl < entry < tp1 <= tp2):
        return None
    if direction == "short" and not (tp2 <= tp1 < entry < sl):
        return None
    return sl, tp1, tp2, best_rr, tp2_extended

# PATHWAYS

def build_pathway_liquidity_reversal(symbol: str, bundles: dict, regime: Regime,
                                      structure_htf: StructureState,
                                      htf_zones: list[Zone], pools: dict,
                                      atr_ltf_vals: list[float]) -> Optional[Candidate]:
    """Sweep of external liquidity into an HTF demand/supply zone (OB or
    Breaker), LTF Market Structure Shift confirms the reversal is real, and
    the entry is taken on the retest of the LTF breaker block that formed
    the MSS impulse."""
    candles_mid, candles_ltf = bundles[TF_MID], bundles[TF_LTF]
    atr_mid_vals = atr_series(candles_mid, ATR_LEN)
    price = candles_ltf[-1]["c"]
    z_price = price_zone(price, structure_htf)

    for direction in ("long", "short"):
        wanted_kind = "demand" if direction == "long" else "supply"
        relevant_htf = [z for z in htf_zones if z.kind == wanted_kind and not
                         (z.mitigated and not z.flipped)]
        if not relevant_htf:
            continue
        # location filter: only zones on the correct side of equilibrium
        relevant_htf = [z for z in relevant_htf
                         if (z.mid <= structure_htf.eq if direction == "long" else z.mid >= structure_htf.eq)]
        if not relevant_htf:
            continue

        sweep = detect_sweep(candles_mid, pools, direction, atr_mid_vals, SWEEP_LOOKBACK_MID)
        if not sweep:
            continue
        near_zone = min(relevant_htf, key=lambda z: abs(z.mid - sweep["level"]))
        if abs(near_zone.mid - sweep["level"]) > 2.0 * (atr_mid_vals[-1] or 1e-9):
            continue

        sweep_mid_index = sweep["index"]
        sweep_ts = candles_mid[sweep_mid_index]["t"]
        ltf_after = next((i for i, c in enumerate(candles_ltf) if c["t"] >= sweep_ts), None)
        if ltf_after is None:
            continue
        mss = detect_mss(candles_ltf, direction, ltf_after, atr_ltf_vals)
        if not mss:
            continue
        breaker = find_ltf_breaker(candles_ltf, mss)
        if not breaker:
            continue

        entry = breaker.high if direction == "long" else breaker.low
        invalidation = min(sweep["extreme"], breaker.low) if direction == "long" else \
            max(sweep["extreme"], breaker.high)
        atr_ltf = atr_ltf_vals[-1] or 1e-9
        vol_pctile = percentile_rank([abs(c["c"] - c["o"]) for c in candles_ltf[-40:]],
                                      abs(candles_ltf[-1]["c"] - candles_ltf[-1]["o"]))
        plan = build_risk_plan(direction, entry, invalidation, atr_ltf, vol_pctile,
                                candles_ltf, htf_zones, pools)
        if not plan:
            continue
        sl, tp1, tp2, _, tp2_extended = plan

        cand = Candidate(symbol, direction, "liquidity_reversal", entry, sl, tp1, tp2,
                          zone=near_zone, atr_val=atr_ltf, regime=regime)
        cand.tp2_extended = tp2_extended
        cand.confluences = [
            f"liquidity sweep @ {sweep['level']:.4f} ({sweep['touches']}x tested)",
            f"HTF {near_zone.origin} {wanted_kind} zone",
            "LTF MSS confirmed + breaker retest",
            f"{z_price} zone entry" if direction == "long" and z_price == "discount"
            or direction == "short" and z_price == "premium" else f"{z_price} zone",
        ]
        cand.context_ok = regime.label in ("reversal", "range", "volatile")
        cand.ltf_confirmed = True
        return cand
    return None


def build_pathway_trend_continuation(symbol: str, bundles: dict, regime: Regime,
                                      structure_htf: StructureState,
                                      htf_zones: list[Zone], pools: dict,
                                      atr_ltf_vals: list[float]) -> Optional[Candidate]:
    """HTF trend intact; price pulls back into a same-direction HTF Order
    Block / Breaker Block or an unfilled LTF FVG, LTF shows a rejection +
    momentum reset back in trend direction."""
    if regime.label != "trend" or regime.direction not in ("bullish", "bearish"):
        return None
    direction = "long" if regime.direction == "bullish" else "short"
    candles_ltf = bundles[TF_LTF]
    price = candles_ltf[-1]["c"]
    wanted_kind = "demand" if direction == "long" else "supply"

    pool_zones = [z for z in htf_zones if z.kind == wanted_kind and not
                  (z.mitigated and not z.flipped)]
    ltf_fvgs = find_fvgs(candles_ltf, atr_ltf_vals, ZONE_LOOKBACK_LTF)
    pool_zones += [z for z in ltf_fvgs if z.kind == wanted_kind]
    if not pool_zones:
        return None

    atr_ltf = atr_ltf_vals[-1] or 1e-9
    touched = [z for z in pool_zones if z.contains(price, buf=0.35 * atr_ltf)]
    if not touched:
        return None
    zone = max(touched, key=lambda z: zone_quality(z))

    closes = [c["c"] for c in candles_ltf]
    r = rsi(closes, RSI_LEN)[-1]
    momentum_ok = (45 <= r <= 68) if direction == "long" else (32 <= r <= 55)
    if not momentum_ok:
        return None

    entry = zone.high if direction == "long" else zone.low
    invalidation = zone.low if direction == "long" else zone.high
    vol_pctile = percentile_rank([abs(c["c"] - c["o"]) for c in candles_ltf[-40:]],
                                  abs(candles_ltf[-1]["c"] - candles_ltf[-1]["o"]))
    plan = build_risk_plan(direction, entry, invalidation, atr_ltf, vol_pctile,
                            candles_ltf, htf_zones, pools)
    if not plan:
        return None
    sl, tp1, tp2, _, tp2_extended = plan

    cand = Candidate(symbol, direction, "trend_continuation", entry, sl, tp1, tp2,
                      zone=zone, atr_val=atr_ltf, regime=regime)
    cand.tp2_extended = tp2_extended
    cand.confluences = [
        f"HTF trend intact (ADX {regime.adx:.0f}, dir={regime.direction})",
        f"pullback into {zone.origin} {wanted_kind}",
        f"RSI momentum reset ({r:.0f})",
    ]
    cand.context_ok = True
    cand.ltf_confirmed = momentum_ok
    return cand


def build_pathway_range_reversion(symbol: str, bundles: dict, regime: Regime,
                                   structure_htf: StructureState,
                                   htf_zones: list[Zone], pools: dict,
                                   atr_ltf_vals: list[float]) -> Optional[Candidate]:
    """Ranging HTF context: fade the range extreme back toward equilibrium
    using an opposing OB/Breaker at the boundary, sized off the range width
    rather than a trend leg."""
    if regime.label != "range":
        return None
    candles_ltf = bundles[TF_LTF]
    price = candles_ltf[-1]["c"]
    rng_w = structure_htf.range_high - structure_htf.range_low
    if rng_w <= 0:
        return None
    pos = safe_div(price - structure_htf.range_low, rng_w)

    if pos >= 0.80:
        direction, wanted_kind = "short", "supply"
    elif pos <= 0.20:
        direction, wanted_kind = "long", "demand"
    else:
        return None

    pool_zones = [z for z in htf_zones if z.kind == wanted_kind and not
                  (z.mitigated and not z.flipped)]
    if not pool_zones:
        return None
    atr_ltf = atr_ltf_vals[-1] or 1e-9
    near = [z for z in pool_zones if z.contains(price, buf=0.5 * atr_ltf)]
    zone = max(near, key=lambda z: zone_quality(z)) if near else \
        min(pool_zones, key=lambda z: abs(z.mid - price))
    if abs(zone.mid - price) > 1.5 * atr_ltf:
        return None

    entry = zone.high if direction == "long" else zone.low
    invalidation = zone.low if direction == "long" else zone.high
    target = structure_htf.eq
    risk = abs(entry - invalidation)
    if risk <= 0:
        return None
    buf = adaptive_sl_buffer(candles_ltf, atr_ltf, 0.4)
    sl = invalidation - buf if direction == "long" else invalidation + buf
    risk = abs(entry - sl)
    tp1 = target
    tp2 = structure_htf.range_high if direction == "long" else structure_htf.range_low
    if direction == "long" and not (sl < entry < tp1 <= tp2):
        return None
    if direction == "short" and not (tp2 <= tp1 < entry < sl):
        return None
    if safe_div(abs(tp1 - entry), risk) < MIN_RR_FLOOR:
        return None

    rr1_val = safe_div(abs(tp1 - entry), risk)
    rr2_val = safe_div(abs(tp2 - entry), risk)

    cand = Candidate(symbol, direction, "range_reversion", entry, sl, tp1, tp2,
                      zone=zone, atr_val=atr_ltf, regime=regime)
    cand.tp2_extended = (rr2_val - rr1_val) >= TP2_MIN_RR_DELTA
    cand.confluences = [
        f"ranging HTF context (ADX {regime.adx:.0f})",
        f"range extreme fade ({pos*100:.0f}% of range)",
        "target equilibrium / opposite range edge",
    ]
    cand.context_ok = True
    cand.ltf_confirmed = True
    return cand

# FIVE-FILTER FRAMEWORK — Location / Context / Quality / RR / LTF

@dataclass
class FilterResult:
    passed: bool
    reason: str = ""
    location_score: float = 0.0
    context_score: float = 0.0
    quality_score: float = 0.0
    rr_score: float = 0.0
    ltf_score: float = 0.0


def apply_five_filters(cand: Candidate, market_price: float, min_rr_floor: float) -> FilterResult:
    atr_val = cand.atr_val or 1e-9

    dist_atr = abs(cand.zone.mid - market_price) / atr_val
    entry_dist_atr = abs(cand.entry - market_price) / atr_val
    if entry_dist_atr > POI_MAX_DIST_ATR_MULT * 1.35:
        return FilterResult(False, "location: entry too far from live price")
    location_score = max(0.0, 1.0 - min(1.0, dist_atr / (POI_MAX_DIST_ATR_MULT * 1.5)))

    context_map = {
        "liquidity_reversal": {"reversal": 1.0, "volatile": 0.75, "range": 0.7, "trend": 0.35},
        "trend_continuation": {"trend": 1.0, "reversal": 0.3, "range": 0.15, "volatile": 0.25},
        "range_reversion": {"range": 1.0, "reversal": 0.4, "trend": 0.1, "volatile": 0.2},
    }
    context_score = context_map.get(cand.pathway, {}).get(cand.regime.label, 0.3) if cand.regime else 0.3
    if not cand.context_ok or context_score < 0.15:
        return FilterResult(False, "context: regime/pathway mismatch")

    # 3) QUALITY — displacement strength, formation volume, freshness,
    #    tightness of the zone, breaker/FVG confluence.
    q = zone_quality(cand.zone)
    width_pen = 0.0 if cand.zone.width <= ZONE_MAX_WIDTH_ATR_MULT * atr_val else 0.25
    quality_score = max(0.0, q - width_pen)
    if quality_score < 0.30:
        return FilterResult(False, f"quality: zone quality too low ({quality_score:.2f})")

    # 4) RR — dynamic floor: choppier / lower-quality regimes need more
    #    reward per unit risk to be worth taking.
    dyn_floor = min_rr_floor
    if cand.regime and cand.regime.label == "volatile":
        dyn_floor += 0.4
    if quality_score < 0.5:
        dyn_floor += 0.3
    rr1 = cand.rr1()
    if rr1 < dyn_floor:
        return FilterResult(False, f"rr: {rr1:.2f} below dynamic floor {dyn_floor:.2f}")
    rr_score = min(1.0, rr1 / 4.0)

    if not cand.ltf_confirmed:
        return FilterResult(False, "ltf: no lower-timeframe confirmation trigger")
    ltf_score = 1.0 if cand.pathway == "liquidity_reversal" else 0.75

    return FilterResult(True, "ok", location_score, context_score, quality_score, rr_score, ltf_score)


# MARKET BREADTH / RELATIVE STRENGTH / CORRELATION

_breadth_lock = threading.Lock()
_breadth_snapshot: dict[str, str] = {}
_rs_lock = threading.Lock()
_rs_snapshot: dict[str, float] = {}


def reset_market_caches():
    with _breadth_lock:
        _breadth_snapshot.clear()
    with _rs_lock:
        _rs_snapshot.clear()


def record_market_inputs(symbol: str, candles_mid: list[dict]):
    closes = [c["c"] for c in candles_mid]
    if len(closes) < 25:
        return
    change_pct = safe_div(closes[-1] - closes[-25], closes[-25]) * 100
    bias = "up" if closes[-1] > ema(closes, EMA_SLOW)[-1] else "down"
    with _breadth_lock:
        _breadth_snapshot[symbol] = bias
    with _rs_lock:
        _rs_snapshot[symbol] = change_pct


def compute_breadth_pct() -> float:
    with _breadth_lock:
        if not _breadth_snapshot:
            return 0.5
        up = sum(1 for v in _breadth_snapshot.values() if v == "up")
        return up / len(_breadth_snapshot)


def relative_strength_percentile(symbol: str) -> float:
    with _rs_lock:
        if symbol not in _rs_snapshot or len(_rs_snapshot) < 5:
            return 0.5
        return percentile_rank(list(_rs_snapshot.values()), _rs_snapshot[symbol])


def compute_pairwise_correlation(symbols: list[str], bundles: dict[str, dict]) -> dict:
    returns = {}
    for s in symbols:
        b = bundles.get(s)
        if b:
            returns[s] = compute_returns(b[TF_MID], 60)
    matrix = {}
    for i, a in enumerate(symbols):
        for b in symbols[i + 1:]:
            if a in returns and b in returns:
                matrix[(a, b)] = pearson(returns[a], returns[b])
    return matrix


def cluster_by_correlation(symbols: list[str], matrix: dict, threshold: float = 0.75) -> list[set]:
    clusters: list[set] = []
    for s in symbols:
        placed = False
        for cl in clusters:
            if any(matrix.get((min(s, o), max(s, o)), 0.0) >= threshold for o in cl):
                cl.add(s)
                placed = True
                break
        if not placed:
            clusters.append({s})
    return clusters


def deduplicate_correlated(ranked: list[tuple], clusters: list[set]) -> list[tuple]:
    kept, seen_clusters_dir = [], set()
    for symbol, direction, cand, conf, grade in ranked:
        cl = next((c for c in clusters if symbol in c), {symbol})
        key = (frozenset(cl), direction)
        if key in seen_clusters_dir:
            continue
        seen_clusters_dir.add(key)
        kept.append((symbol, direction, cand, conf, grade))
    return kept

# CONFIDENCE SCORING + SETUP GRADE

def compute_confidence(cand: Candidate, fr: FilterResult, symbol: str,
                        market_ctx: dict, reference_ms: int, htf_alignment: float) -> float:
    """Composite 0-100 confidence blending the five filter sub-scores with
    professional-grade auxiliary signals (funding carry, OI trend, relative
    strength, breadth alignment, session weighting, HTF alignment across
    4H+1D) so a single weak secondary input tempers rather than vetoes an
    otherwise strong structural setup."""
    base = (
        0.24 * fr.location_score +
        0.22 * fr.context_score +
        0.24 * fr.quality_score +
        0.16 * fr.rr_score +
        0.14 * fr.ltf_score
    ) * 100

    bonus = 0.0
    ctx = market_ctx.get(symbol, {})
    funding = ctx.get("funding", 0.0)
    if abs(funding) >= FUNDING_CARRY_THRESHOLD:
        favorable = (funding > 0 and cand.direction == "short") or (funding < 0 and cand.direction == "long")
        bonus += 3.0 if favorable else -2.0
    if abs(funding) >= FUNDING_EXTREME:
        bonus -= 2.0  # crowded / squeeze risk

    rs_pct = relative_strength_percentile(symbol)
    if cand.direction == "long" and rs_pct >= (1 - RS_TOP_PCTILE):
        bonus += 3.0
    elif cand.direction == "short" and rs_pct <= RS_BOTTOM_PCTILE:
        bonus += 3.0
    elif cand.direction == "long" and rs_pct <= RS_BOTTOM_PCTILE:
        bonus -= 3.0
    elif cand.direction == "short" and rs_pct >= (1 - RS_TOP_PCTILE):
        bonus -= 3.0

    breadth = compute_breadth_pct()
    if cand.direction == "long":
        bonus += (breadth - 0.5) * 6.0
    else:
        bonus += (0.5 - breadth) * 6.0

    sess = session_now(reference_ms)
    bonus += SESSION_SCORE_BONUS.get(sess, 0.0)

    bonus += (htf_alignment - 0.5) * 8.0

    if cand.zone.origin == "breaker":
        bonus += 2.5  # defended-then-flipped level: extra evidential weight

    return max(0.0, min(100.0, base + bonus))


def grade_for_confidence(confidence: float) -> str:
    if confidence >= 85:
        return "S"
    if confidence >= 75:
        return "A"
    if confidence >= 65:
        return "B"
    return "C"


# ADAPTIVE GOVERNOR — self-tunes the confidence floor from realized winrate

def governor_threshold(state: dict) -> float:
    history = [h for h in state.get("signal_history", []) if h.get("sent") and h.get("result") in ("win", "loss")]
    recent = history[-GOVERNOR_LOOKBACK_SIGNALS:]
    if len(recent) < 12:
        return BASE_MIN_CONFIDENCE
    wins = sum(1 for h in recent if h["result"] == "win")
    winrate = wins / len(recent)
    delta = (GOVERNOR_TARGET_WINRATE - winrate) * 40.0   # underperforming -> raise the bar
    delta = max(-GOVERNOR_MAX_SHIFT, min(GOVERNOR_MAX_SHIFT, delta))
    return BASE_MIN_CONFIDENCE + delta


def dynamic_max_signals(regime_breadth: float, btc_regime: Optional[Regime]) -> int:
    if btc_regime and btc_regime.label == "trend" and (regime_breadth >= 0.65 or regime_breadth <= 0.35):
        return MAX_SIGNALS_PER_SCAN_TRENDING
    return MAX_SIGNALS_PER_SCAN_DEFAULT


def priority_score(cand: Candidate, confidence: float) -> float:
    return confidence + cand.rr2() * 2.0

# STATE MANAGEMENT

def _default_state() -> dict:
    return {
        "active_signals": [],
        "signal_history": [],
        "cooldowns": {},
        "last_summary_date": None,
        "spread_history": {},
    }


def load_state() -> dict:
    p = Path(STATE_FILE)
    if not p.exists():
        return _default_state()
    try:
        data = json.loads(p.read_text())
        base = _default_state()
        base.update(data)
        return base
    except (json.JSONDecodeError, OSError):
        return _default_state()


def save_state(state: dict):
    tmp = Path(STATE_FILE + ".tmp")
    tmp.write_text(json.dumps(state, indent=None, default=str))
    tmp.replace(STATE_FILE)


def prune_state(state: dict, max_days: int = 21):
    cutoff = time.time() * 1000 - max_days * 86_400_000
    state["signal_history"] = [
        h for h in state["signal_history"][-MAX_SIGNAL_HISTORY:]
        if h.get("ts", 0) >= cutoff
    ]


def check_cooldown(state: dict, symbol: str, direction: str, bar_index: int) -> bool:
    key = f"{symbol}:{direction}"
    last = state["cooldowns"].get(key)
    return last is None or (bar_index - last) >= COOLDOWN_BARS_LTF


def update_cooldown(state: dict, symbol: str, direction: str, bar_index: int):
    state["cooldowns"][f"{symbol}:{direction}"] = bar_index


def is_recent_duplicate(state: dict, symbol: str, direction: str, entry: float) -> bool:
    for sig in state["active_signals"]:
        if sig["symbol"] == symbol and sig["direction"] == direction:
            if abs(sig["entry"] - entry) / max(entry, 1e-9) <= DUPLICATE_ENTRY_TOLERANCE_PCT:
                return True
    return False


def has_active_signal(state: dict, symbol: str) -> bool:
    """True if `symbol` already has ANY active signal (either direction).
    This is the one-signal-per-symbol gate -- unlike is_recent_duplicate()
    (which only catches same-direction/near-identical-entry re-fires), this
    blocks opposite-direction signals too."""
    return any(sig["symbol"] == symbol for sig in state["active_signals"])


def count_active(state: dict) -> int:
    return len(state["active_signals"])


def record_signal_history(state: dict, symbol: str, direction: str, pathway: str,
                           confidence: float, grade: str, sent: bool) -> int:
    hist_id = int(time.time() * 1000) + len(state["signal_history"])
    state["signal_history"].append({
        "id": hist_id, "symbol": symbol, "direction": direction, "pathway": pathway,
        "confidence": confidence, "grade": grade, "sent": sent,
        "ts": time.time() * 1000, "result": None,
    })
    return hist_id


def track_signal(state: dict, symbol: str, direction: str, msg_id: int,
                  cand: Candidate, confidence: float, grade: str, bar_index: int, hist_id: int,
                  last_candle_ts: float = 0):
    state["active_signals"].append({
        "symbol": symbol, "direction": direction, "msg_id": msg_id,
        "entry": cand.entry, "sl": cand.sl, "tp1": cand.tp1, "tp2": cand.tp2,
        "pathway": cand.pathway, "confidence": confidence, "grade": grade,
        "bar_index": bar_index, "hist_id": hist_id, "opened_ts": time.time() * 1000,
        "tp1_hit": False,
        # Entry is a resting limit/stop order, not an instant fill. Nothing
        # is "at risk" until price actually trades through cand.entry, so
        # SL/TP checks must not begin until this flips true.
        "entry_filled": False,
        "bars_pending": 0,
        # Only candles CLOSING after this ts get scanned for SL/TP hits.
        # Seeded with the last already-closed LTF candle at entry time, so
        # the next NEW closed candle is the first one checked.
        "last_check_ts": last_candle_ts,
    })


def _r_multiple(sig: dict, price: float) -> float:
    risk = abs(sig["entry"] - sig["sl"])
    if risk <= 0:
        return 0.0
    if sig["direction"] == "long":
        return (price - sig["entry"]) / risk
    return (sig["entry"] - price) / risk


def _update_history_result(state: dict, hist_id: int, result: str):
    for h in state["signal_history"]:
        if h["id"] == hist_id:
            h["result"] = result
            break


def check_active_signals(state: dict, bundles_by_symbol: dict[str, dict[str, list[dict]]]):
    """Scans every LTF candle CLOSED since the signal's last check -- using
    that candle's high/low wick, not just the latest close -- so a SL/TP1/TP2
    level touched intra-candle is never missed between polling runs, however
    far apart those runs are.

    ENTRY FILL: `entry` is a resting limit/stop order, not an instant fill.
    Nothing is at risk until price actually trades through the entry level,
    so SL/TP wicks are ignored for any signal whose entry hasn't printed
    yet -- otherwise a signal whose SL sits closer to market than its entry
    (a common shape for breakout/continuation setups) can be marked a loss
    without ever having been filled. A candle's range is treated as having
    touched entry if lo <= entry <= hi. If entry fills mid-candle and that
    same candle's wick also reaches SL/TP1/TP2, the same conservative
    same-candle-ambiguity handling below still applies. A signal that sits
    unfilled for PENDING_ENTRY_EXPIRY_BARS is cancelled outright (see
    _expire_signal) rather than left pending forever or misjudged as a loss.

    NOTE: SL is never moved/trailed to TP1. It stays at its original level
    for the life of the signal. A return to that original SL after TP1 has
    already been hit is still scored as a managed win (see _close_signal
    call below) purely for bookkeeping purposes -- the stored sl value
    itself is untouched, so there's no risk of a moved SL sitting right on
    top of TP1 and self-triggering the moment TP1 prints.
    """
    still_active = []
    for sig in state["active_signals"]:
        bundle = bundles_by_symbol.get(sig["symbol"])
        candles_ltf = bundle.get(TF_LTF) if bundle else None
        if not candles_ltf:
            still_active.append(sig)
            continue

        direction = sig["direction"]
        last_ts = sig.get("last_check_ts", 0)
        # Only candles that closed strictly after the last check, oldest first.
        new_candles = sorted(
            (c for c in candles_ltf if c["t"] > last_ts),
            key=lambda c: c["t"],
        )
        if not new_candles:
            still_active.append(sig)
            continue

        closed = False
        for c in new_candles:
            hi, lo = c["h"], c["l"]

            if not sig.get("entry_filled", False):
                if not (lo <= sig["entry"] <= hi):
                    # Order still resting -- nothing to check this candle.
                    sig["bars_pending"] = sig.get("bars_pending", 0) + 1
                    if sig["bars_pending"] >= PENDING_ENTRY_EXPIRY_BARS:
                        _expire_signal(state, sig)
                        closed = True
                        break
                    continue
                sig["entry_filled"] = True
                # Fall through: the same candle that fills entry can also
                # wick to SL/TP1/TP2, so it still gets evaluated below.

            hit_sl = lo <= sig["sl"] if direction == "long" else hi >= sig["sl"]
            hit_tp1 = hi >= sig["tp1"] if direction == "long" else lo <= sig["tp1"]
            hit_tp2 = hi >= sig["tp2"] if direction == "long" else lo <= sig["tp2"]

            if not sig["tp1_hit"]:
                if hit_sl and not hit_tp1:
                    _close_signal(state, sig, "loss", sig["sl"])
                    closed = True
                    break
                if hit_tp1:
                    sig["tp1_hit"] = True
                    r1 = _r_multiple(sig, sig["tp1"])
                    react_telegram(sig["msg_id"], "🔥")
                    reply_telegram(sig["msg_id"],
                                    f"🔥 {hl_coin(sig['symbol'])} {sig['direction'].upper()} — "
                                    f"TP1 hit ({r1:+.2f}R) — runner still active toward TP2")
                    # Same candle's wick can also clear TP2 (fast/volatile
                    # move) -- check it immediately rather than waiting for
                    # the next candle.
                    if hit_tp2:
                        _close_signal(state, sig, "win", sig["tp2"])
                        closed = True
                        break
                    if hit_sl:
                        # Wick touched both TP1 and original SL within the
                        # same candle -- can't know which came first intra-bar,
                        # so treat conservatively as SL first (loss) rather
                        # than assuming the more favourable order.
                        _close_signal(state, sig, "loss", sig["sl"])
                        closed = True
                        break
            else:
                if hit_tp2:
                    _close_signal(state, sig, "win", sig["tp2"])
                    closed = True
                    break
                if hit_sl:
                    _close_signal(state, sig, "win", sig["sl"])  # managed win, see docstring
                    closed = True
                    break

        sig["last_check_ts"] = new_candles[-1]["t"]
        if not closed:
            still_active.append(sig)
    state["active_signals"] = still_active


def _close_signal(state: dict, sig: dict, result: str, price: float):
    r = _r_multiple(sig, price)
    emoji = "🏆" if result == "win" else "😭"
    react_telegram(sig["msg_id"], emoji)
    reply_telegram(sig["msg_id"],
                    f"{emoji} {hl_coin(sig['symbol'])} {sig['direction'].upper()} closed — "
                    f"{result.upper()} ({r:+.2f}R)")
    _update_history_result(state, sig["hist_id"], result)


def _expire_signal(state: dict, sig: dict):
    """Cancels a signal whose entry never filled within PENDING_ENTRY_EXPIRY_BARS.
    No position was ever opened, so this is neither a win nor a loss -- it's
    excluded from win-rate stats entirely (governor_threshold and
    build_daily_summary already only count result in ('win', 'loss'))."""
    react_telegram(sig["msg_id"], "⌛")
    reply_telegram(sig["msg_id"],
                    f"⌛ {hl_coin(sig['symbol'])} {sig['direction'].upper()} — entry never filled after "
                    f"{PENDING_ENTRY_EXPIRY_BARS} bars, signal expired (not counted as win/loss)")
    _update_history_result(state, sig["hist_id"], "expired")

# TELEGRAM

TG_API = f"https://api.telegram.org/bot{TG_BOT_TOKEN}"


def fmt_px(v: float) -> str:
    if v >= 1000:
        return f"{v:,.2f}"
    if v >= 1:
        return f"{v:,.4f}"
    return f"{v:.6f}"


def fmt_px_copy(v: float) -> str:
    """Plain numeric formatting with NO thousands separators, so tapping the
    <code> block in Telegram copies a value that pastes straight into an
    exchange order field without needing to strip commas first."""
    if v >= 1000:
        return f"{v:.2f}"
    if v >= 1:
        return f"{v:.4f}"
    return f"{v:.6f}"


def confidence_bar(confidence: float) -> str:
    filled = round(confidence / 10)
    return "🟩" * filled + "⬜" * (10 - filled)


PATHWAY_LABEL = {
    "liquidity_reversal": "Liquidity Reversal",
    "trend_continuation": "Trend Continuation",
    "range_reversion": "Range Mean-Reversion",
}


_PAREN_RE = re.compile(r"\s*\([^)]*\)")

_SHORTEN_MAP = {
    "confirmed + breaker retest": "+ breaker retest",
    "liquidity sweep @": "sweep @",
}


def _shorten_confluence(text: str) -> str:
    text = _PAREN_RE.sub("", text).strip()
    for long, short in _SHORTEN_MAP.items():
        text = text.replace(long, short)
    return text


def format_signal(symbol: str, cand: Candidate, confidence: float, grade: str, rank: int) -> str:
    dir_emoji = "🟢 LONG" if cand.direction == "long" else "🔴 SHORT"
    confluences = "\n".join(f"• {_shorten_confluence(c)}" for c in cand.confluences)
    setup_line = PATHWAY_LABEL.get(cand.pathway, cand.pathway)
    if cand.regime:
        setup_line += f" · {cand.regime.label}/{cand.regime.direction}"

    tp2_line = (f"Take Profit 2: <code>{fmt_px_copy(cand.tp2)}</code>" if cand.tp2_extended
                else "Take Profit 2: <i>no extension room — close full at TP1</i>")

    lines = [
        f"🦅 <b>{ENGINE_NAME}</b> — Adaptive Smart-Money Signal Engine",
        "────────────────────",
        f"#{rank} <b>{hl_coin(symbol)}</b> {dir_emoji}",
        f"Grade: <b>{grade}</b>   Confidence: <b>{confidence:.0f}%</b>",
        confidence_bar(confidence),
        "",
        f"Entry: <code>{fmt_px_copy(cand.entry)}</code>",
        f"Stop Loss: <code>{fmt_px_copy(cand.sl)}</code>",
        f"Take Profit 1: <code>{fmt_px_copy(cand.tp1)}</code>",
        tp2_line,
        "",
        setup_line,
        confluences,
        "",
        f"<i>{ENGINE_NAME} v{__version__} · scan-per-run · not financial advice</i>",
    ]
    return "\n".join(l for l in lines if l is not None)


def send_telegram(text: str) -> Optional[int]:
    try:
        r = _session.post(f"{TG_API}/sendMessage",
                           json={"chat_id": TG_CHAT_ID, "text": text, "parse_mode": "HTML"},
                           timeout=10)
        data = r.json()
        if data.get("ok"):
            return data["result"]["message_id"]
    except (requests.RequestException, KeyError, ValueError):
        pass
    return None


def reply_telegram(msg_id: int, text: str):
    try:
        _session.post(f"{TG_API}/sendMessage",
                       json={"chat_id": TG_CHAT_ID, "text": text,
                             "reply_to_message_id": msg_id, "parse_mode": "HTML"},
                       timeout=10)
    except requests.RequestException:
        pass


def react_telegram(msg_id: int, emoji: str):
    try:
        _session.post(f"{TG_API}/setMessageReaction",
                       json={"chat_id": TG_CHAT_ID, "message_id": msg_id,
                             "reaction": [{"type": "emoji", "emoji": emoji}]},
                       timeout=10)
    except requests.RequestException:
        pass


DAILY_SUMMARY_HOUR_UTC = 8   # send once per day, during the 8am UTC scan window


def build_daily_summary(state: dict) -> str:
    now = time.time() * 1000
    last_24h = [h for h in state["signal_history"] if h.get("sent") and now - h.get("ts", 0) <= 86_400_000]
    resolved = [h for h in last_24h if h.get("result") in ("win", "loss")]
    wins = sum(1 for h in resolved if h["result"] == "win")
    winrate = safe_div(wins, len(resolved)) * 100 if resolved else 0.0
    return (
        f"🦅 <b>{ENGINE_NAME}</b> — Adaptive Smart-Money Signal Engine\n"
        f"────────────────────\n"
        f"📊 <b>24H Summary</b>\n"
        f"Signals sent (24h): {len(last_24h)}\n"
        f"Resolved: {len(resolved)}  Win rate: {winrate:.0f}%\n"
        f"Currently active: {count_active(state)}"
    )


def maybe_send_daily_summary(state: dict):
    now_dt = datetime.now(timezone.utc)
    today_str = now_dt.strftime("%Y-%m-%d")
    if now_dt.hour == DAILY_SUMMARY_HOUR_UTC and state.get("last_summary_date") != today_str:
        send_telegram(build_daily_summary(state))
        state["last_summary_date"] = today_str

# PER-SYMBOL SCAN PIPELINE

def collect_market_inputs(symbol: str, reference_ms: int) -> Optional[tuple]:
    bundles = fetch_all_candles(symbol, reference_ms)
    if bundles is None:
        return None
    record_market_inputs(symbol, bundles[TF_MID])
    return bundles


def scan_symbol(symbol: str, state: dict, bundles: dict, market_ctx: dict,
                 btc_regime: Optional[Regime], bar_index_ltf: int,
                 reference_ms: int, min_confidence: float) -> list[tuple]:
    if bundles is None:
        return []
    if has_active_signal(state, symbol):
        return []
    candles_ltf, candles_mid = bundles[TF_LTF], bundles[TF_MID]
    candles_htf, candles_macro = bundles[TF_HTF], bundles[TF_MACRO]

    atr_htf = atr_series(candles_htf, ATR_LEN)
    atr_ltf_vals = atr_series(candles_ltf, ATR_LEN)
    atr_pct = safe_div(atr_ltf_vals[-1], candles_ltf[-1]["c"]) * 100
    if atr_pct < MIN_ATR_PCT or atr_pct > MAX_ATR_PCT:
        return []

    ctx = market_ctx.get(symbol, {})
    if ctx.get("oi", 0.0) and ctx["oi"] * ctx.get("mark_px", 0.0) < MIN_OI_USD and symbol not in MAJORS:
        return []

    spread = ctx.get("spread_pct")
    if spread is not None and symbol not in SPREAD_EXEMPT:
        if spread >= SPREAD_SUPPRESS_PCT:
            return []

    regime = classify_regime(candles_htf, candles_mid)

    swings_htf = find_swings(candles_htf, PIVOT_LEFT_HTF, PIVOT_RIGHT_HTF)
    structure_htf = analyze_structure(candles_htf, swings_htf)
    if structure_htf is None:
        return []

    htf_zones = find_order_blocks(candles_htf, atr_htf, ZONE_LOOKBACK_HTF)
    htf_zones = mark_mitigation_and_breakers(htf_zones, candles_htf)
    htf_fvgs = find_fvgs(candles_htf, atr_htf, ZONE_LOOKBACK_HTF)
    htf_zones += htf_fvgs
    pools = build_liquidity_pools(swings_htf, candles_macro)

    macro_closes = [c["c"] for c in candles_macro]
    macro_bias = "bullish" if candles_macro[-1]["c"] > ema(macro_closes, min(EMA_SLOW, len(macro_closes) - 1))[-1] else "bearish"
    htf_alignment = 1.0 if macro_bias == regime.direction else (0.3 if regime.direction != "neutral" else 0.5)

    pathways = [
        build_pathway_liquidity_reversal(symbol, bundles, regime, structure_htf, htf_zones, pools, atr_ltf_vals),
        build_pathway_trend_continuation(symbol, bundles, regime, structure_htf, htf_zones, pools, atr_ltf_vals),
        build_pathway_range_reversion(symbol, bundles, regime, structure_htf, htf_zones, pools, atr_ltf_vals),
    ]

    market_price = candles_ltf[-1]["c"]
    vp = volume_profile(candles_mid[-80:])
    results = []
    for cand in pathways:
        if cand is None:
            continue
        if not check_cooldown(state, symbol, cand.direction, bar_index_ltf):
            continue
        if is_recent_duplicate(state, symbol, cand.direction, cand.entry):
            continue

        cand.entry, cand.sl, cand.tp1, cand.tp2 = clamp_entry_to_market(
            cand.entry, cand.sl, cand.tp1, cand.tp2, market_price, cand.atr_val)
        clipped_tp2 = clip_tp_to_liquidity(cand.entry, cand.tp2, cand.direction, pools, vp, htf_zones)
        if cand.direction == "long":
            if clipped_tp2 >= cand.tp1:
                cand.tp2 = clipped_tp2
        else:
            if clipped_tp2 <= cand.tp1:
                cand.tp2 = clipped_tp2
        risk = abs(cand.entry - cand.sl)
        rr1 = safe_div(abs(cand.tp1 - cand.entry), risk)
        rr2 = safe_div(abs(cand.tp2 - cand.entry), risk)
        cand.tp2_extended = (rr2 - rr1) >= TP2_MIN_RR_DELTA
        cand.zone_q = zone_quality(cand.zone)

        fr = apply_five_filters(cand, market_price, MIN_RR_FLOOR)
        if not fr.passed:
            continue

        confidence = compute_confidence(cand, fr, symbol, market_ctx, reference_ms, htf_alignment)
        if confidence < min_confidence:
            continue
        grade = grade_for_confidence(confidence)
        results.append((symbol, cand.direction, cand, confidence, grade))

    if not results:
        return []
    best = max(results, key=lambda t: t[3])
    return [best]


# MAIN EXECUTION FLOW

_shutdown = False


def _handle_sigterm(signum, frame):
    global _shutdown
    _shutdown = True


os_signal.signal(os_signal.SIGTERM, _handle_sigterm)


def main():
    print(f"[{datetime.now(timezone.utc).isoformat()}] {ENGINE_NAME} v{__version__} scan starting…")

    reference_ms = int(time.time() * 1000)
    bar_index_ltf = reference_ms // (15 * 60 * 1000)
    state = load_state()
    prune_state(state)
    reset_market_caches()

    print("[INIT] Fetching market context (metaAndAssetCtxs)…")
    meta_ctx = get_meta_and_asset_ctxs() or {}
    market_ctx = {f"{coin}USDT": data for coin, data in meta_ctx.items()}

    if _shutdown:
        save_state(state)
        sys.exit(0)

    print("[PHASE 1] Collecting candle bundles for all timeframes…")
    bundles_by_symbol: dict[str, dict] = {}
    with ThreadPoolExecutor(max_workers=max(1, SCAN_WORKERS)) as ex:
        futures = {ex.submit(collect_market_inputs, sym, reference_ms): sym for sym in WATCHLIST}
        for fut in as_completed(futures):
            sym = futures[fut]
            try:
                b = fut.result()
                if b is not None:
                    bundles_by_symbol[sym] = b
            except Exception as e:
                print(f"    ERROR fetching {sym}: {e}")

    resolved = [s for s in WATCHLIST if s in bundles_by_symbol]
    print(f"  Resolved {len(resolved)}/{len(WATCHLIST)} symbols")
    if len(resolved) < 10:
        print("  [ABORT] Too few symbols resolved this cycle — skipping to avoid bad breadth reads")
        save_state(state)
        return

    print("[PHASE 1b] Spread checks (majors exempt)…")
    with ThreadPoolExecutor(max_workers=max(1, SCAN_WORKERS)) as ex:
        futures = {ex.submit(get_l2_spread_pct, sym): sym for sym in resolved if sym not in SPREAD_EXEMPT}
        for fut in as_completed(futures):
            sym = futures[fut]
            try:
                spread = fut.result()
                if spread is not None:
                    market_ctx.setdefault(sym, {})["spread_pct"] = spread
            except Exception:
                pass

    print("[INIT] Computing BTC macro regime…")
    btc_regime = None
    btc_bundle = bundles_by_symbol.get("BTCUSDT")
    if btc_bundle:
        try:
            btc_regime = classify_regime(btc_bundle[TF_HTF], btc_bundle[TF_MID])
            print(f"  BTC regime: {btc_regime.label} / {btc_regime.direction} (ADX {btc_regime.adx:.1f})")
        except Exception as e:
            print(f"  [BTC REGIME] failed: {e}")

    try:
        corr_matrix = compute_pairwise_correlation(resolved, bundles_by_symbol)
        corr_clusters = cluster_by_correlation(resolved, corr_matrix)
    except Exception as e:
        print(f"  [CORR] clustering failed, falling back to singletons: {e}")
        corr_clusters = [{s} for s in resolved]

    breadth_pct = compute_breadth_pct()
    min_confidence = governor_threshold(state)
    max_signals = dynamic_max_signals(breadth_pct, btc_regime)
    print(f"  Governor min-confidence: {min_confidence:.1f}  |  Max signals this scan: {max_signals} "
          f"(breadth {breadth_pct*100:.0f}%)")

    if _shutdown:
        save_state(state)
        sys.exit(0)

    print("[PHASE 2] Scanning symbols for setups…")
    pending: list[tuple] = []
    with ThreadPoolExecutor(max_workers=max(1, SCAN_WORKERS)) as ex:
        futures = {
            ex.submit(scan_symbol, sym, state, bundles_by_symbol.get(sym), market_ctx,
                      btc_regime, bar_index_ltf, reference_ms, min_confidence): sym
            for sym in resolved
        }
        for fut in as_completed(futures):
            sym = futures[fut]
            try:
                res = fut.result()
                if res:
                    pending.extend(res)
            except Exception as e:
                print(f"    ERROR scanning {sym}: {e}")

    pending.sort(key=lambda t: priority_score(t[2], t[3]), reverse=True)
    deduped = deduplicate_correlated(pending, corr_clusters)

    room = max(0, MAX_CONCURRENT_ACTIVE_SIGNALS - count_active(state))
    cap = min(max_signals, room)
    top = deduped[:cap]
    dropped = deduped[cap:]

    if dropped:
        for symbol, direction, cand, confidence, grade in dropped:
            record_signal_history(state, symbol, direction, cand.pathway, confidence, grade, sent=False)
        print(f"  Dropped {len(dropped)} lower-priority setup(s) (cap={cap})")

    fired = 0
    for rank, (symbol, direction, cand, confidence, grade) in enumerate(top, start=1):
        msg = format_signal(symbol, cand, confidence, grade, rank)
        msg_id = send_telegram(msg)
        hist_id = record_signal_history(state, symbol, direction, cand.pathway, confidence, grade, sent=True)
        if msg_id:
            update_cooldown(state, symbol, direction, bar_index_ltf)
            track_signal(state, symbol, direction, msg_id, cand, confidence, grade, bar_index_ltf, hist_id,
                         last_candle_ts=bundles_by_symbol[symbol][TF_LTF][-1]["t"])
            print(f"  #{rank} {hl_coin(symbol)} {direction.upper()} [{grade}] conf={confidence:.0f} "
                  f"entry={cand.entry:.4f} sl={cand.sl:.4f} tp1={cand.tp1:.4f} tp2={cand.tp2:.4f}")
            fired += 1
        else:
            print(f"  #{rank} {hl_coin(symbol)} {direction.upper()} — Telegram send failed, not tracked")
        time.sleep(0.4)

    print("[TRACK] Checking active signals against candle wicks since last check…")
    check_active_signals(state, bundles_by_symbol)

    maybe_send_daily_summary(state)
    save_state(state)
    print(f"Scan complete. {fired} signal(s) fired. {count_active(state)} active.")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        try:
            send_telegram(f"🚨 {ENGINE_NAME} crashed: {e}")
        except Exception:
            pass
        raise
    finally:
        _session.close()
