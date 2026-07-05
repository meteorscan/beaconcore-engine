"""
MERIDIAN v1.0.0
--------------------------------------------------------------------
An institutional Smart Money Concept execution engine trading a single,
non-negotiable sequence on Hyperliquid perpetuals, run across two
independent timeframe combos in parallel:

    HTF Bias
      -> HTF Institutional Point of Interest (POI)
        -> HTF Swing Failure Pattern (liquidity sweep)
          -> Execution-TF Market Structure Shift (confirmation)
            -> Breaker / Mitigation Block entry
              -> Structure-based Stop Loss
                -> Minimum 2R Target, dynamically extended to 3R / 4R

Combos:
    H4 / 15m   (original)
    12H / 1h   (added v1.2.0)

Each combo runs its own bias/POI/SFP/MSS pipeline with its own tuned
constants, its own pending-setup and cooldown state, and its own
active-signal tracking, so a symbol can fire on either combo, or both,
independently in the same scan. Telegram signals are tagged with the
combo that produced them, e.g. "(H4/15m)" or "(12H/1h)".

No EMA crossover systems, no RSI/MACD entries, no breakout or pullback
systems, no scoring/voting, no machine learning. One setup, executed
with discipline. Signal quality is prioritized over signal frequency.

Infrastructure (Hyperliquid REST API, hardcoded watchlist, Telegram
delivery + reaction system, cron-per-run scan architecture, state.json
persistence) mirrors the operator's existing engines. The trading model
itself is entirely original to Meridian.

Signal quality controls:
    - Scan results are ranked by a quality metric (TP2 R-multiple, then
      HTF POI confluence quality) before truncation to
      MAX_SIGNALS_PER_SCAN, rather than truncating in whatever order
      symbols happened to resolve in.
    - A sector/correlated-asset diversification cap (MAX_PER_SECTOR)
      stops a single scan from firing all its signal slots into one
      correlated basket (e.g. all L1s).
    - An aggregate outcome / win-rate summary is printed at the end of
      every scan, computed from state["resolved_signals"].
    - The HTF POI zone's confluence quality is carried through the
      pending-setup -> MSS-confirmation handoff, and the SFP detector
      explicitly breaks same-candle ties by zone quality instead of by
      zone list order.
--------------------------------------------------------------------
"""

import os, json, time, math, random, threading, requests, sys, copy
import signal as os_signal
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

__version__ = "1.0.0"
ENGINE_NAME = "Meridian"

# ── CONFIG ──────────────────────────────────────────────────────────
TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN")
TG_CHAT_ID   = os.getenv("TG_CHAT_ID")
if not TG_BOT_TOKEN:
    raise RuntimeError("TG_BOT_TOKEN environment variable is required")
if not TG_CHAT_ID:
    raise RuntimeError("TG_CHAT_ID environment variable is required")

STATE_FILE     = "state.json"
SCAN_WORKERS   = int(os.getenv("SCAN_WORKERS", "2"))
HL_INFO_URL    = "https://api.hyperliquid.xyz/info"

HL_MIN_INTERVAL_S     = float(os.getenv("HL_MIN_INTERVAL_S", "0.18"))
HL_MIN_INTERVAL_MAX_S = float(os.getenv("HL_MIN_INTERVAL_MAX_S", "0.60"))
HL_TF_WORKERS         = int(os.getenv("HL_TF_WORKERS", "1"))

_hl_request_lock    = threading.Lock()
_hl_last_request_ts = 0.0
_hl_min_interval_s  = HL_MIN_INTERVAL_S
_hl_consecutive_successes = 0
_hl_session = requests.Session()
_state_lock = threading.Lock()

# ── HARDCODED WATCHLIST (shared with the operator's other engines) ──
WATCHLIST = [
    "BTCUSDT", "ETHUSDT", "HYPEUSDT", "ZECUSDT", "NEARUSDT",
    "ONDOUSDT", "SUIUSDT", "PENGUUSDT", "BNBUSDT", "SOLUSDT",
    "TRXUSDT", "BCHUSDT", "DOGEUSDT", "ADAUSDT", "DOTUSDT",
    "TAOUSDT", "AVAXUSDT", "LINKUSDT", "AAVEUSDT", "XRPUSDT",
    "XLMUSDT", "UNIUSDT", "LTCUSDT", "APTUSDT", "PENDLEUSDT",
]

# ── SECTOR MAP (used for max-one-per-sector diversification cap) ────
# Mirrors the operator's Nyx Engine sector map 1:1 (same watchlist).
SECTOR_MAP: dict[str, str] = {
    "BTCUSDT":    "btc",
    "ETHUSDT":    "eth",
    "SOLUSDT":    "eth_l1", "AVAXUSDT": "eth_l1", "SUIUSDT": "eth_l1", "APTUSDT": "eth_l1",
    "NEARUSDT":   "eth_l1",
    "BNBUSDT":    "bnb",
    "XRPUSDT":    "payments", "XLMUSDT": "payments", "TRXUSDT": "payments", "LTCUSDT": "payments",
    "DOGEUSDT":   "meme",    "PENGUUSDT": "meme",
    "ADAUSDT":    "layer1_alt", "DOTUSDT": "layer1_alt", "TAOUSDT": "layer1_alt",
    "LINKUSDT":   "defi",    "AAVEUSDT": "defi", "UNIUSDT": "defi",
    "ONDOUSDT":   "defi",    "PENDLEUSDT": "defi",
    "HYPEUSDT":   "hype",
    "ZECUSDT":    "privacy", "BCHUSDT": "privacy",
}

INTERVAL_MS = {
    "15m": 15 * 60 * 1000,
    "1h":  60 * 60 * 1000,
    "4h":  4  * 60 * 60 * 1000,
    "12h": 12 * 60 * 60 * 1000,
    "1d":  24 * 60 * 60 * 1000,
}

N_D   = 400     # ~13 months of daily — PDH/PDL/PWH/PWL/PMH/PML

REACT_TP1 = "🔥"
REACT_TP2 = "🏆"
REACT_SL  = "😭"

STATE_VERSION = 1

# ── STRATEGY CONSTANTS (shared across all combos) ───────────────────
MIN_RR                  = 2.0
EXT_RR_LEVELS           = (2.0, 3.0, 4.0)
LIQUIDITY_ROOM_BUFFER_ATR_MULT = 0.50

MAX_CONCURRENT_ACTIVE_SIGNALS = 10    # global cap across all combos combined
MAX_SIGNALS_PER_SCAN = 2              # global cap across all combos combined
MAX_PER_SECTOR       = 1              # diversification cap — see SECTOR_MAP


# ── COMBO CONFIG ──────────────────────────────────────────────────────
# Each combo runs the full HTF Bias -> POI -> SFP -> MSS -> Breaker
# pipeline independently, with its own tuned constants, its own pending-
# setup/cooldown state, and its own active-signal tracking. A symbol can
# fire on one combo, the other, or both in the same scan.
class Combo:
    __slots__ = (
        "id", "label", "htf_tf", "exec_tf", "n_htf", "n_exec",
        "pivot_left_htf", "pivot_right_htf", "pivot_left_exec", "pivot_right_exec",
        "poi_lookback", "liquidity_tolerance", "poi_confluence_buffer_atr_mult",
        "ob_displacement_atr_mult", "ob_bos_lookback", "fvg_min_gap_atr_mult",
        "sfp_max_sweep_depth_atr_mult", "sfp_min_wick_ratio", "sfp_lookback_htf_bars",
        "mss_max_wait_hours", "mss_displacement_atr_mult", "mss_min_close_margin_atr_mult",
        "breaker_search_bars", "sl_buffer_atr_mult",
        "signal_max_age_hours", "pending_setup_max_age_hours", "cooldown_htf_bars",
    )
    def __init__(self, **kw):
        for k in self.__slots__:
            setattr(self, k, kw[k])


COMBOS: dict[str, Combo] = {
    "h4_15m": Combo(
        id="h4_15m", label="H4/15m", htf_tf="4h", exec_tf="15m",
        n_htf=220, n_exec=300,                      # ~36d H4, ~3.1d M15
        pivot_left_htf=3, pivot_right_htf=3,
        pivot_left_exec=2, pivot_right_exec=2,
        poi_lookback=120,
        liquidity_tolerance=0.0012,                 # 0.12% price tolerance for equal highs/lows
        poi_confluence_buffer_atr_mult=0.30,
        ob_displacement_atr_mult=1.30,
        ob_bos_lookback=3,
        fvg_min_gap_atr_mult=0.05,
        sfp_max_sweep_depth_atr_mult=1.60,           # reject SFPs that sweep too far
        sfp_min_wick_ratio=0.33,
        sfp_lookback_htf_bars=24,                    # ~4 days of H4
        mss_max_wait_hours=30,                       # M15 confirm must arrive within this window
        mss_displacement_atr_mult=1.15,
        mss_min_close_margin_atr_mult=0.08,
        breaker_search_bars=6,
        sl_buffer_atr_mult=0.20,
        signal_max_age_hours=24,                     # max life for a tracked, unresolved signal
        pending_setup_max_age_hours=48,               # drop an unconfirmed HTF SFP after this long
        cooldown_htf_bars=2,
    ),
    "12h_1h": Combo(
        id="12h_1h", label="12H/1h", htf_tf="12h", exec_tf="1h",
        n_htf=100, n_exec=200,                       # ~50d of 12H, ~8.3d of 1h
        pivot_left_htf=3, pivot_right_htf=3,
        pivot_left_exec=2, pivot_right_exec=2,
        poi_lookback=80,                              # 12H bars, deeper HTF -> shorter bar-count lookback
        liquidity_tolerance=0.0015,                   # slightly looser clustering on the coarser TF
        poi_confluence_buffer_atr_mult=0.30,
        ob_displacement_atr_mult=1.30,
        ob_bos_lookback=3,
        fvg_min_gap_atr_mult=0.05,
        sfp_max_sweep_depth_atr_mult=1.60,
        sfp_min_wick_ratio=0.33,
        sfp_lookback_htf_bars=16,                     # 16 * 12h = ~8 days of 12H bars
        mss_max_wait_hours=90,                        # 3x the H4/15m window (12h is 3x slower than 4h)
        mss_displacement_atr_mult=1.15,
        mss_min_close_margin_atr_mult=0.08,
        breaker_search_bars=6,
        sl_buffer_atr_mult=0.20,
        signal_max_age_hours=96,                      # 4x the H4/15m window (1h is 4x slower than 15m)
        pending_setup_max_age_hours=144,               # 3x the H4/15m window
        cooldown_htf_bars=2,                           # 2 * 12h = 24h cooldown between same-direction fires
    ),
}

# Max allowed drift between plan.entry and live market price at fire-time,
# expressed as a fraction of the plan's own risk distance (|entry - sl|).
# e.g. 0.5 = market may be at most 0.5R away from entry before we consider
# the setup stale and refuse to send it.
MAX_ENTRY_DRIFT_R = 0.5

_shutdown = False
def _handle_sigterm(signum, frame):
    global _shutdown
    _shutdown = True
os_signal.signal(os_signal.SIGTERM, _handle_sigterm)
os_signal.signal(os_signal.SIGINT, _handle_sigterm)


# ══════════════════════════════════════════════════════════════════
# HYPERLIQUID API LAYER
# ══════════════════════════════════════════════════════════════════

def hl_coin(symbol: str) -> str:
    return symbol.replace("USDT", "")

def hl_post(payload: dict):
    global _hl_last_request_ts, _hl_min_interval_s, _hl_consecutive_successes
    max_attempts = int(os.getenv("HL_MAX_ATTEMPTS", "6"))
    base_sleep_s = float(os.getenv("HL_BASE_SLEEP_S", "0.75"))
    for attempt in range(max_attempts):
        try:
            with _hl_request_lock:
                now = time.time()
                wait_s = _hl_min_interval_s - (now - _hl_last_request_ts)
                if wait_s > 0:
                    time.sleep(wait_s)
                _hl_last_request_ts = time.time()

            r = _hl_session.post(HL_INFO_URL, json=payload,
                                  headers={"Content-Type": "application/json"}, timeout=15)
            if r.status_code == 429:
                with _hl_request_lock:
                    _hl_min_interval_s = min(HL_MIN_INTERVAL_MAX_S, _hl_min_interval_s * 1.25 + 0.02)
                    _hl_consecutive_successes = 0
                retry_after = r.headers.get("Retry-After")
                try:
                    retry_after_s = float(retry_after) if retry_after is not None else None
                except ValueError:
                    retry_after_s = None
                sleep_s = retry_after_s if retry_after_s is not None else (base_sleep_s * (2 ** attempt))
                sleep_s = min(20.0, max(base_sleep_s, sleep_s)) + random.uniform(0.0, 0.35)
                time.sleep(sleep_s)
                continue
            r.raise_for_status()
            with _hl_request_lock:
                _hl_consecutive_successes += 1
                if _hl_consecutive_successes >= 10:
                    _hl_min_interval_s = HL_MIN_INTERVAL_S
                    _hl_consecutive_successes = 0
                else:
                    _hl_min_interval_s = max(HL_MIN_INTERVAL_S, _hl_min_interval_s - 0.0025)
            return r.json()
        except Exception:
            if attempt == max_attempts - 1:
                raise
            time.sleep(min(20.0, base_sleep_s * (2 ** attempt)) + random.uniform(0.0, 0.25))
    raise RuntimeError("hl_post exhausted all retries (likely persistent 429)")

def current_bar_open_ms(reference_ms: int, interval: str) -> int:
    iv_ms = INTERVAL_MS.get(interval, 3_600_000)
    return (reference_ms // iv_ms) * iv_ms

def filter_closed_candles(candles: list[dict], interval: str, reference_ms: int) -> list[dict]:
    cutoff = current_bar_open_ms(reference_ms, interval)
    return [c for c in candles if c["t"] < cutoff]

def get_candles(symbol: str, interval: str, n: int,
                 start_time_ms: int | None = None,
                 reference_ms: int | None = None) -> list[dict]:
    coin = hl_coin(symbol)
    iv_ms = INTERVAL_MS.get(interval, 3_600_000)
    ref_ms = int(time.time() * 1000) if reference_ms is None else reference_ms
    end_ms = current_bar_open_ms(ref_ms, interval)
    computed_start_ms = start_time_ms if start_time_ms is not None else end_ms - iv_ms * (n + 10)

    payload = {"type": "candleSnapshot", "req": {
        "coin": coin, "interval": interval,
        "startTime": computed_start_ms, "endTime": end_ms,
    }}
    raw = hl_post(payload)
    if raw is None:
        return []
    candles = []
    for c in raw:
        base_v = float(c["v"])
        quote_v = float(c["q"]) if c.get("q") is not None else base_v
        candles.append({"t": int(c["t"]), "o": float(c["o"]), "h": float(c["h"]),
                         "l": float(c["l"]), "c": float(c["c"]), "v": base_v, "qv": quote_v})
    candles = filter_closed_candles(candles, interval, ref_ms)
    return candles[-n:]

MIN_BARS_HTF  = 60
MIN_BARS_EXEC = 60
MIN_BARS_D    = 30

def _required_timeframes() -> dict[str, int]:
    """Union of every timeframe used by any combo (plus daily), mapped to
    the max candle count any combo needs for that timeframe."""
    need: dict[str, int] = {"1d": N_D}
    for combo in COMBOS.values():
        need[combo.htf_tf]  = max(need.get(combo.htf_tf, 0), combo.n_htf)
        need[combo.exec_tf] = max(need.get(combo.exec_tf, 0), combo.n_exec)
    return need

def fetch_all_candles(symbol: str, reference_ms: int | None = None) -> dict[str, list] | None:
    """Fetches every timeframe required across all combos for one symbol
    in parallel, and returns {timeframe: candles}. Returns None if any
    required timeframe comes back too thin to be usable."""
    needed = _required_timeframes()
    results: dict[str, list] = {}
    with ThreadPoolExecutor(max_workers=max(1, HL_TF_WORKERS)) as ex:
        futures = {
            ex.submit(get_candles, symbol, tf, n, None, reference_ms): tf
            for tf, n in needed.items()
        }
        for fut in as_completed(futures):
            tf = futures[fut]
            try:
                results[tf] = fut.result()
            except Exception as e:
                print(f"  [CANDLES] {symbol} {tf} fetch failed: {e} — skipping symbol")
                return None

    for tf in needed:
        if tf not in results:
            return None
        min_bars = MIN_BARS_D if tf == "1d" else (MIN_BARS_HTF if tf in ("4h", "12h") else MIN_BARS_EXEC)
        if len(results[tf]) < min_bars:
            return None
    return results

_meta_cache: dict | None = None
_meta_cache_lock = threading.Lock()
_meta_cache_fetched_at = 0.0
META_CACHE_TTL_S = 55.0

def get_meta_and_asset_ctxs() -> dict | None:
    global _meta_cache, _meta_cache_fetched_at
    with _meta_cache_lock:
        if _meta_cache is not None and (time.time() - _meta_cache_fetched_at) < META_CACHE_TTL_S:
            return _meta_cache
    try:
        data = hl_post({"type": "metaAndAssetCtxs"})
        if data is None:
            return _meta_cache
        universe, asset_ctxs = data[0].get("universe", []), data[1]
        cache = {}
        for i, asset in enumerate(universe):
            name = asset.get("name", "")
            if not name:
                continue
            ctx = asset_ctxs[i]
            cache[name] = {
                "funding": float(ctx["funding"]) if ctx.get("funding") is not None else None,
                "open_interest_coins": float(ctx["openInterest"]) if ctx.get("openInterest") is not None else None,
                "mark_px": float(ctx["markPx"]) if ctx.get("markPx") is not None else None,
            }
        with _meta_cache_lock:
            _meta_cache = cache
            _meta_cache_fetched_at = time.time()
        return _meta_cache
    except Exception as e:
        print(f"  [META CACHE] fetch failed: {e}")
        with _meta_cache_lock:
            return _meta_cache

def get_open_interest_usd(symbol: str) -> float | None:
    cache = get_meta_and_asset_ctxs()
    if not cache:
        return None
    row = cache.get(hl_coin(symbol))
    if not row or row.get("open_interest_coins") is None or row.get("mark_px") is None:
        return None
    return row["open_interest_coins"] * row["mark_px"]


# ══════════════════════════════════════════════════════════════════
# MATH HELPERS
# ══════════════════════════════════════════════════════════════════

def safe_div(a: float, b: float, default: float = 0.0) -> float:
    try:
        return a / b if b else default
    except Exception:
        return default

def atr_series(candles: list[dict], period: int = 14) -> list[float]:
    if len(candles) < 2:
        return [0.0] * len(candles)
    trs = [candles[0]["h"] - candles[0]["l"]]
    for i in range(1, len(candles)):
        h, l, pc = candles[i]["h"], candles[i]["l"], candles[i - 1]["c"]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    out, run = [], None
    for i, tr in enumerate(trs):
        if i < period:
            run = sum(trs[:i + 1]) / (i + 1)
        else:
            run = (run * (period - 1) + tr) / period
        out.append(run)
    return out


# ══════════════════════════════════════════════════════════════════
# STEP 1 — HTF BIAS (H4)
# ══════════════════════════════════════════════════════════════════

class Pivot:
    __slots__ = ("index", "price", "kind")
    def __init__(self, index, price, kind):
        self.index, self.price, self.kind = index, price, kind

def detect_pivots(candles: list[dict], left: int, right: int) -> list[Pivot]:
    pivots = []
    n = len(candles)
    for i in range(left, n - right):
        window_h = [candles[j]["h"] for j in range(i - left, i + right + 1)]
        window_l = [candles[j]["l"] for j in range(i - left, i + right + 1)]
        h, l = candles[i]["h"], candles[i]["l"]
        if h == max(window_h) and window_h.count(h) == 1:
            pivots.append(Pivot(i, h, "high"))
        if l == min(window_l) and window_l.count(l) == 1:
            pivots.append(Pivot(i, l, "low"))
    return pivots

class HTFBias:
    def __init__(self, bias, range_low, range_high, eq, atr_h4):
        self.bias = bias
        self.range_low = range_low
        self.range_high = range_high
        self.eq = eq
        self.atr_h4 = atr_h4

    @property
    def zone(self) -> str:
        return None

def compute_htf_bias(candles_h4: list[dict], combo: Combo) -> HTFBias | None:
    if len(candles_h4) < (combo.pivot_left_htf + combo.pivot_right_htf + 10):
        return None
    pivots = detect_pivots(candles_h4, combo.pivot_left_htf, combo.pivot_right_htf)
    highs = sorted([p for p in pivots if p.kind == "high"], key=lambda p: p.index)
    lows  = sorted([p for p in pivots if p.kind == "low"],  key=lambda p: p.index)
    if len(highs) < 2 or len(lows) < 2:
        return None

    last_high, prev_high = highs[-1], highs[-2]
    last_low,  prev_low  = lows[-1],  lows[-2]

    if last_high.price > prev_high.price and last_low.price > prev_low.price:
        bias = "bullish"
    elif last_high.price < prev_high.price and last_low.price < prev_low.price:
        bias = "bearish"
    else:
        bias = "neutral"

    close = candles_h4[-1]["c"]
    # CHOCH: a decisive close through the most recent opposing swing flips bias.
    if bias in ("bullish", "neutral") and close < last_low.price:
        bias = "bearish"
    elif bias in ("bearish", "neutral") and close > last_high.price:
        bias = "bullish"

    range_high = max(last_high.price, prev_high.price)
    range_low  = min(last_low.price, prev_low.price)
    if range_high <= range_low:
        return None
    eq = (range_high + range_low) / 2.0
    atr_h4 = atr_series(candles_h4, 14)[-1]
    return HTFBias(bias, range_low, range_high, eq, atr_h4)

def price_zone(price: float, htf: HTFBias) -> str:
    return "premium" if price >= htf.eq else "discount"


# ══════════════════════════════════════════════════════════════════
# STEP 2 — INSTITUTIONAL H4 POIs
# ══════════════════════════════════════════════════════════════════

class POIZone:
    def __init__(self, low, high, kind, index, quality=1):
        self.low, self.high = min(low, high), max(low, high)
        self.kind = kind
        self.index = index
        self.quality = quality
        self.mid = (self.low + self.high) / 2.0

    def contains(self, price: float, buf: float = 0.0) -> bool:
        return (self.low - buf) <= price <= (self.high + buf)

def _order_blocks(candles: list[dict], atr_vals: list[float], combo: Combo) -> list[POIZone]:
    zones = []
    n = len(candles)
    lookback = combo.ob_bos_lookback
    for i in range(lookback + 1, n):
        c = candles[i]
        body = c["c"] - c["o"]
        a = atr_vals[i] or 1e-9
        displacement = abs(body) >= combo.ob_displacement_atr_mult * a
        if not displacement:
            continue
        if body > 0:
            prior_high = max(candles[j]["h"] for j in range(max(0, i - lookback), i))
            if c["c"] <= prior_high:
                continue
            for j in range(i - 1, max(-1, i - lookback - 1), -1):
                ob = candles[j]
                if ob["c"] < ob["o"]:
                    zones.append(POIZone(ob["l"], ob["h"], "demand", j))
                    break
        else:
            prior_low = min(candles[j]["l"] for j in range(max(0, i - lookback), i))
            if c["c"] >= prior_low:
                continue
            for j in range(i - 1, max(-1, i - lookback - 1), -1):
                ob = candles[j]
                if ob["c"] > ob["o"]:
                    zones.append(POIZone(ob["l"], ob["h"], "supply", j))
                    break
    return zones

def _fair_value_gaps(candles: list[dict], atr_vals: list[float], combo: Combo) -> list[POIZone]:
    zones = []
    for i in range(1, len(candles) - 1):
        left, right = candles[i - 1], candles[i + 1]
        a = atr_vals[i] or 1e-9
        if left["h"] < right["l"] and (right["l"] - left["h"]) >= combo.fvg_min_gap_atr_mult * a:
            zones.append(POIZone(left["h"], right["l"], "demand", i))
        elif left["l"] > right["h"] and (left["l"] - right["h"]) >= combo.fvg_min_gap_atr_mult * a:
            zones.append(POIZone(right["h"], left["l"], "supply", i))
    return zones

def _equal_liquidity_levels(pivots: list[Pivot], tolerance_pct: float) -> list[tuple]:
    """Clusters equal highs / equal lows into external liquidity pool levels."""
    levels = []
    for kind in ("high", "low"):
        pts = sorted([p for p in pivots if p.kind == kind], key=lambda p: p.price)
        used = [False] * len(pts)
        for i, p in enumerate(pts):
            if used[i]:
                continue
            cluster = [p]
            for j in range(i + 1, len(pts)):
                if used[j]:
                    continue
                if abs(pts[j].price - p.price) / max(p.price, 1e-9) <= tolerance_pct:
                    cluster.append(pts[j])
                    used[j] = True
            if len(cluster) >= 2:
                avg = sum(c.price for c in cluster) / len(cluster)
                levels.append(("EQH" if kind == "high" else "EQL", avg))
    return levels

def _period_hl(candles_d: list[dict], days: int) -> tuple:
    if len(candles_d) < days + 1:
        return None, None
    window = candles_d[-(days + 1):-1]  # exclude the still-forming/last day
    if not window:
        return None, None
    return max(c["h"] for c in window), min(c["l"] for c in window)

def build_liquidity_levels(candles_h4: list[dict], candles_d: list[dict], pivots_h4, combo: Combo) -> list[tuple]:
    levels = list(_equal_liquidity_levels(pivots_h4, combo.liquidity_tolerance))
    pdh, pdl = _period_hl(candles_d, 1)
    pwh, pwl = _period_hl(candles_d, 7)
    pmh, pml = _period_hl(candles_d, 30)
    for label, val in (("PDH", pdh), ("PDL", pdl), ("PWH", pwh), ("PWL", pwl),
                        ("PMH", pmh), ("PML", pml)):
        if val is not None:
            levels.append((label, val))
    return levels

def is_zone_untested(zone: POIZone, candles: list[dict]) -> bool:
    """A POI is fresh only if no candle after its formation has already
    traded back through it and closed beyond, i.e. it hasn't been mitigated."""
    for c in candles[zone.index + 1: -1]:
        if c["l"] <= zone.mid <= c["h"]:
            return False
    return True

def build_poi_zones(candles_h4: list[dict], candles_d: list[dict], htf: HTFBias, combo: Combo) -> list[POIZone]:
    window = candles_h4[-combo.poi_lookback:]
    offset = len(candles_h4) - len(window)
    atr_vals = atr_series(candles_h4, 14)
    pivots_h4 = detect_pivots(window, combo.pivot_left_htf, combo.pivot_right_htf)
    for p in pivots_h4:
        p.index += offset

    raw = _order_blocks(window, atr_vals[offset:], combo) + _fair_value_gaps(window, atr_vals[offset:], combo)
    for z in raw:
        z.index += offset

    liquidity_levels = build_liquidity_levels(candles_h4, candles_d, pivots_h4, combo)
    a = htf.atr_h4 or 1e-9
    buf = combo.poi_confluence_buffer_atr_mult * a

    kept = []
    for z in raw:
        if not is_zone_untested(z, candles_h4):
            continue
        if z.kind == "demand" and price_zone(z.mid, htf) != "discount":
            continue
        if z.kind == "supply" and price_zone(z.mid, htf) != "premium":
            continue
        confluence = sum(1 for _, lvl in liquidity_levels if (z.low - buf) <= lvl <= (z.high + buf))
        z.quality = 1 + confluence
        kept.append(z)

    kept.sort(key=lambda z: (z.quality, z.index), reverse=True)
    return kept[:5]


# ══════════════════════════════════════════════════════════════════
# STEP 3 — H4 SWING FAILURE PATTERN (LIQUIDITY SWEEP)
# ══════════════════════════════════════════════════════════════════

class SFPEvent:
    def __init__(self, direction, zone: POIZone, sweep_extreme, candle_index, candle_time):
        self.direction = direction
        self.zone = zone
        self.sweep_extreme = sweep_extreme
        self.candle_index = candle_index
        self.candle_time = candle_time

def detect_sfp(candles_h4: list[dict], zones: list[POIZone], htf: HTFBias, combo: Combo) -> SFPEvent | None:
    if htf.bias == "neutral" or not zones:
        return None
    atr_vals = atr_series(candles_h4, 14)
    n = len(candles_h4)
    start = max(0, n - combo.sfp_lookback_htf_bars)

    best = None
    for i in range(start, n):
        c = candles_h4[i]
        a = atr_vals[i] or 1e-9
        rng = c["h"] - c["l"]
        if rng <= 0:
            continue
        for z in zones:
            if htf.bias == "bullish" and z.kind == "demand":
                swept = c["l"] < z.low and (z.low - c["l"]) <= combo.sfp_max_sweep_depth_atr_mult * a
                rejected = c["c"] > z.low and c["c"] > c["o"]
                wick_ratio = safe_div(min(c["o"], c["c"]) - c["l"], rng)
                if swept and rejected and wick_ratio >= combo.sfp_min_wick_ratio:
                    cand = SFPEvent("long", z, c["l"], i, c["t"])
                    # Most recent sweep wins; among candidates on the same candle,
                    # explicitly prefer the higher-confluence POI zone rather than
                    # whichever zone happened to be first in the list.
                    if best is None or (cand.candle_index, cand.zone.quality) > \
                                        (best.candle_index, best.zone.quality):
                        best = cand
            elif htf.bias == "bearish" and z.kind == "supply":
                swept = c["h"] > z.high and (c["h"] - z.high) <= combo.sfp_max_sweep_depth_atr_mult * a
                rejected = c["c"] < z.high and c["c"] < c["o"]
                wick_ratio = safe_div(c["h"] - max(c["o"], c["c"]), rng)
                if swept and rejected and wick_ratio >= combo.sfp_min_wick_ratio:
                    cand = SFPEvent("short", z, c["h"], i, c["t"])
                    if best is None or (cand.candle_index, cand.zone.quality) > \
                                        (best.candle_index, best.zone.quality):
                        best = cand
    return best


# ══════════════════════════════════════════════════════════════════
# STEP 4 — M15 MARKET STRUCTURE SHIFT (CONFIRMATION)
# ══════════════════════════════════════════════════════════════════

class MSSEvent:
    def __init__(self, direction, impulse_index, swing_price, confirm_time):
        self.direction = direction
        self.impulse_index = impulse_index
        self.swing_price = swing_price
        self.confirm_time = confirm_time

def detect_mss(candles_exec: list[dict], direction: str, sweep_time_ms: int, combo: Combo) -> MSSEvent | None:
    post = [(i, c) for i, c in enumerate(candles_exec) if c["t"] > sweep_time_ms]
    if len(post) < (combo.pivot_left_exec + combo.pivot_right_exec + 3):
        return None
    post_candles = [c for _, c in post]
    offset = post[0][0]
    atr_vals = atr_series(candles_exec, 14)
    pivots = detect_pivots(post_candles, combo.pivot_left_exec, combo.pivot_right_exec)

    if direction == "long":
        swing_highs = sorted([p for p in pivots if p.kind == "high"], key=lambda p: p.index)
        if not swing_highs:
            return None
        for idx in range(swing_highs[0].index + 1, len(post_candles)):
            relevant_highs = [p for p in swing_highs if p.index < idx]
            if not relevant_highs:
                continue
            swing_price = relevant_highs[-1].price
            c = post_candles[idx]
            a = atr_vals[offset + idx] or 1e-9
            displacement = (c["c"] - c["o"]) >= combo.mss_displacement_atr_mult * a
            margin = c["c"] - swing_price >= combo.mss_min_close_margin_atr_mult * a
            if c["c"] > swing_price and displacement and margin:
                return MSSEvent("long", offset + idx, swing_price, c["t"])
        return None
    else:
        swing_lows = sorted([p for p in pivots if p.kind == "low"], key=lambda p: p.index)
        if not swing_lows:
            return None
        for idx in range(swing_lows[0].index + 1, len(post_candles)):
            relevant_lows = [p for p in swing_lows if p.index < idx]
            if not relevant_lows:
                continue
            swing_price = relevant_lows[-1].price
            c = post_candles[idx]
            a = atr_vals[offset + idx] or 1e-9
            displacement = (c["o"] - c["c"]) >= combo.mss_displacement_atr_mult * a
            margin = swing_price - c["c"] >= combo.mss_min_close_margin_atr_mult * a
            if c["c"] < swing_price and displacement and margin:
                return MSSEvent("short", offset + idx, swing_price, c["t"])
        return None


# ══════════════════════════════════════════════════════════════════
# STEP 5 — BREAKER ENTRY, STRUCTURE STOP, DYNAMIC TARGETS
# ══════════════════════════════════════════════════════════════════

class TradePlan:
    def __init__(self, entry, sl, tp1, tp2, r_multiple_tp2, breaker_low, breaker_high):
        self.entry, self.sl, self.tp1, self.tp2 = entry, sl, tp1, tp2
        self.r_multiple_tp2 = r_multiple_tp2
        self.breaker_low, self.breaker_high = breaker_low, breaker_high

def find_breaker_block(candles_exec: list[dict], mss: MSSEvent, combo: Combo) -> POIZone | None:
    lo = max(0, mss.impulse_index - combo.breaker_search_bars)
    if mss.direction == "long":
        for j in range(mss.impulse_index - 1, lo - 1, -1):
            c = candles_exec[j]
            if c["c"] < c["o"]:
                return POIZone(c["l"], c["h"], "demand", j)
    else:
        for j in range(mss.impulse_index - 1, lo - 1, -1):
            c = candles_exec[j]
            if c["c"] > c["o"]:
                return POIZone(c["l"], c["h"], "supply", j)
    return None

def room_to_next_opposing_level(entry: float, direction: str, sfp: SFPEvent,
                                 zones: list[POIZone], liquidity_levels: list[tuple]) -> float | None:
    candidates = []
    for z in zones:
        if direction == "long" and z.kind == "supply" and z.low > entry:
            candidates.append(z.low)
        if direction == "short" and z.kind == "demand" and z.high < entry:
            candidates.append(z.high)
    for _, lvl in liquidity_levels:
        if direction == "long" and lvl > entry:
            candidates.append(lvl)
        if direction == "short" and lvl < entry:
            candidates.append(lvl)
    if not candidates:
        return None
    return min(candidates) - entry if direction == "long" else entry - max(candidates)

def build_trade_plan(direction: str, sfp: SFPEvent, breaker: POIZone,
                      candles_h4: list[dict], atr_h4: float,
                      zones: list[POIZone], liquidity_levels: list[tuple],
                      combo: Combo) -> TradePlan | None:
    buf = combo.sl_buffer_atr_mult * (atr_h4 or 1e-9)
    if direction == "long":
        entry = breaker.high
        sl = sfp.sweep_extreme - buf
    else:
        entry = breaker.low
        sl = sfp.sweep_extreme + buf

    risk = abs(entry - sl)
    if risk <= 0:
        return None

    tp1 = entry + MIN_RR * risk if direction == "long" else entry - MIN_RR * risk

    room = room_to_next_opposing_level(entry, direction, sfp, zones, liquidity_levels)
    best_rr = MIN_RR
    if room is not None:
        usable_room = room - LIQUIDITY_ROOM_BUFFER_ATR_MULT * (atr_h4 or 1e-9)
        for rr in EXT_RR_LEVELS:
            if usable_room >= rr * risk:
                best_rr = rr
    tp2 = entry + best_rr * risk if direction == "long" else entry - best_rr * risk

    if direction == "long" and not (sl < entry < tp1 <= tp2):
        return None
    if direction == "short" and not (tp2 <= tp1 < entry < sl):
        return None

    return TradePlan(entry, sl, tp1, tp2, best_rr, breaker.low, breaker.high)


# ══════════════════════════════════════════════════════════════════
# STATE PERSISTENCE
# ══════════════════════════════════════════════════════════════════

def load_state() -> dict:
    fresh = {"_version": STATE_VERSION, "pending_setups": {}, "active_signals": [],
             "resolved_signals": [], "signal_cooldowns": {}, "signal_history": []}
    for path in (STATE_FILE, STATE_FILE + ".bak"):
        if Path(path).exists():
            try:
                s = json.loads(Path(path).read_text())
                if s.get("_version", 1) != STATE_VERSION:
                    print(f"[STATE] Schema version mismatch in {path}. Starting fresh.")
                    continue
                for k, v in fresh.items():
                    s.setdefault(k, v if not isinstance(v, (dict, list)) else type(v)())
                if path != STATE_FILE:
                    print(f"[STATE] Loaded from backup {path}")
                return s
            except Exception as e:
                print(f"[STATE] Failed to load {path}: {e}")
    print("[STATE] Starting fresh — no valid state file found")
    return fresh

def save_state(state: dict):
    with _state_lock:
        state_copy = copy.deepcopy(state)
    tmp_path = STATE_FILE + ".tmp"
    Path(tmp_path).write_text(json.dumps(state_copy, indent=2))
    os.replace(tmp_path, STATE_FILE)
    try:
        import shutil
        shutil.copy2(STATE_FILE, STATE_FILE + ".bak")
    except Exception:
        pass


# ══════════════════════════════════════════════════════════════════
# TELEGRAM
# ══════════════════════════════════════════════════════════════════

def _sanitize_error(e: Exception) -> str:
    import re
    msg = str(e)
    if "bot" in msg and "/" in msg:
        msg = re.sub(r'https?://[^\s]+', '[URL]', msg)
    return f"{e.__class__.__name__}: {msg[:200]}"

def send_telegram(text: str) -> int | None:
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    for attempt in range(3):
        try:
            r = requests.post(url, json={"chat_id": TG_CHAT_ID, "text": text, "parse_mode": "HTML"}, timeout=10)
            r.raise_for_status()
            return r.json()["result"]["message_id"]
        except Exception as e:
            if attempt == 2:
                print(f"[TG ERROR] {_sanitize_error(e)}")
            time.sleep(2)
    return None

def react_to_message(message_id: int, emoji: str):
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/setMessageReaction"
    try:
        r = requests.post(url, json={"chat_id": TG_CHAT_ID, "message_id": message_id,
                                      "reaction": [{"type": "emoji", "emoji": emoji}], "is_big": False}, timeout=10)
        r.raise_for_status()
        print(f"  [REACT] {emoji} -> msg_id {message_id}")
    except Exception as e:
        print(f"  [REACT ERROR] msg_id {message_id}: {_sanitize_error(e)}")

def fmt_px(v: float) -> str:
    if v >= 1000: return f"{v:,.2f}"
    if v >= 1:    return f"{v:,.4f}"
    return f"{v:.6f}"

def format_signal(symbol: str, direction: str, plan: TradePlan, htf: HTFBias, zone_kind: str,
                   combo: Combo, market_price: float | None = None) -> str:
    coin = hl_coin(symbol)
    arrow = "▲ LONG" if direction == "long" else "▼ SHORT"
    emoji = "🟢" if direction == "long" else "🔴"
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    risk = abs(plan.entry - plan.sl)
    htf_label = combo.htf_tf.upper() if combo.htf_tf != "12h" else "12H"
    exec_label = combo.exec_tf
    price_line = ""
    if market_price is not None:
        dist = market_price - plan.entry
        dist_pct = safe_div(dist, plan.entry) * 100
        sign = "+" if dist >= 0 else "-"
        price_line = (
            f"Market price: <code>{fmt_px(market_price)}</code> "
            f"({sign}{fmt_px(abs(dist))} / {sign}{abs(dist_pct):.2f}% from entry)\n"
        )
    return (
        f"{emoji} <b>{ENGINE_NAME} v{__version__}</b>\n"
        f"<b>{coin} — {arrow} ({combo.label})</b>\n\n"
        f"HTF Bias ({htf_label}): {htf.bias.upper()} | Zone swept: {zone_kind.upper()}\n"
        f"Setup: {htf_label} SFP -> {exec_label} MSS -> Breaker Entry\n\n"
        f"{price_line}"
        f"Entry (breaker zone): <code>{fmt_px(plan.entry)}</code>\n"
        f"Stop Loss (structure): <code>{fmt_px(plan.sl)}</code>\n"
        f"TP1 (2R): <code>{fmt_px(plan.tp1)}</code>\n"
        f"Risk distance: {fmt_px(risk)}\n\n"
        f"{ts}"
    )


# ══════════════════════════════════════════════════════════════════
# SIGNAL LIFECYCLE: PENDING SETUPS -> ACTIVE SIGNALS -> RESOLUTION
# ══════════════════════════════════════════════════════════════════

def check_cooldown(state: dict, symbol: str, direction: str, bar_index_htf: int, combo: Combo) -> bool:
    active = state.get("active_signals", [])
    active_count = sum(1 for s in active if not s.get("resolved", False))
    if active_count >= MAX_CONCURRENT_ACTIVE_SIGNALS:
        return False
    # Exclusivity is per (symbol, combo): an active H4/15m signal on a
    # symbol does not block a 12H/1h signal on the same symbol, or vice versa.
    for s in active:
        if s.get("symbol") == symbol and s.get("combo") == combo.id and not s.get("resolved", False):
            return False
    key = f"{combo.id}:{symbol}_{direction}"
    last_bar = state.get("signal_cooldowns", {}).get(key)
    if last_bar is not None and (bar_index_htf - last_bar) < combo.cooldown_htf_bars:
        return False
    return True

def update_cooldown(state: dict, symbol: str, direction: str, bar_index_htf: int, combo: Combo):
    state.setdefault("signal_cooldowns", {})[f"{combo.id}:{symbol}_{direction}"] = bar_index_htf

def process_symbol(symbol: str, combo: Combo, state: dict, bundle: tuple, reference_ms: int,
                    bar_index_htf: int, bar_index_exec: int) -> dict | None:
    candles_exec, candles_htf, candles_d = bundle
    coin = hl_coin(symbol)
    tag = f"{coin}/{combo.label}"

    pending = state.setdefault("pending_setups", {}).setdefault(combo.id, {})
    setup = pending.get(symbol)

    # -- Advance an existing pending HTF SFP toward exec-TF confirmation --
    if setup is not None:
        age_hours = (reference_ms - setup["sfp_time"]) / 3_600_000.0
        if age_hours > combo.pending_setup_max_age_hours:
            print(f"    {tag}: pending setup expired ({age_hours:.1f}h) — dropping")
            del pending[symbol]
            setup = None

    if setup is not None:
        mss = detect_mss(candles_exec, setup["direction"], setup["sfp_time"], combo)
        if mss is None:
            print(f"    {tag}: SFP pending, no {combo.exec_tf} MSS yet")
            return None

        zone = POIZone(setup["zone_low"], setup["zone_high"],
                        "demand" if setup["direction"] == "long" else "supply", 0,
                        quality=setup.get("zone_quality", 1))
        sfp = SFPEvent(setup["direction"], zone, setup["sweep_extreme"], 0, setup["sfp_time"])
        breaker = find_breaker_block(candles_exec, mss, combo)
        if breaker is None:
            print(f"    {tag}: MSS confirmed but no breaker candle found — dropping setup")
            del pending[symbol]
            return None

        htf = HTFBias(setup["direction"] == "long" and "bullish" or "bearish",
                      setup["range_low"], setup["range_high"], setup["eq"], setup["atr_h4"])
        zones = build_poi_zones(candles_htf, candles_d, htf, combo)
        liquidity_levels = build_liquidity_levels(
            candles_htf, candles_d,
            detect_pivots(candles_htf[-combo.poi_lookback:], combo.pivot_left_htf, combo.pivot_right_htf),
            combo)

        plan = build_trade_plan(setup["direction"], sfp, breaker, candles_htf, setup["atr_h4"],
                                 zones, liquidity_levels, combo)
        del pending[symbol]
        if plan is None:
            print(f"    {tag}: MSS confirmed but trade plan failed validation")
            return None

        if not check_cooldown(state, symbol, setup["direction"], bar_index_htf, combo):
            print(f"    {tag}: setup confirmed but suppressed by cooldown / concurrency limit")
            return None

        print(f"    MERIDIAN SIGNAL [{combo.label}]: {coin} {setup['direction'].upper()} "
              f"entry={fmt_px(plan.entry)} sl={fmt_px(plan.sl)} "
              f"tp1={fmt_px(plan.tp1)} tp2={fmt_px(plan.tp2)} ({plan.r_multiple_tp2:.0f}R)")
        return {"symbol": symbol, "direction": setup["direction"], "plan": plan,
                "zone_kind": zone.kind, "zone_quality": zone.quality,
                "bar_index_exec": bar_index_exec, "combo": combo}

    # -- No pending setup: look for a fresh HTF bias -> POI -> SFP sequence --
    htf = compute_htf_bias(candles_htf, combo)
    if htf is None or htf.bias == "neutral":
        print(f"    {tag}: no {combo.htf_tf} bias")
        return None

    zones = build_poi_zones(candles_htf, candles_d, htf, combo)
    if not zones:
        print(f"    {tag}: no qualified {combo.htf_tf} POI in {price_zone(candles_htf[-1]['c'], htf)}")
        return None

    sfp = detect_sfp(candles_htf, zones, htf, combo)
    if sfp is None:
        print(f"    {tag}: {htf.bias} bias, no valid {combo.htf_tf} SFP yet")
        return None

    already_active = any(s.get("symbol") == symbol and s.get("combo") == combo.id
                          and not s.get("resolved", False)
                          for s in state.get("active_signals", []))
    if already_active:
        return None

    pending[symbol] = {
        "direction": sfp.direction, "zone_low": sfp.zone.low, "zone_high": sfp.zone.high,
        "zone_quality": sfp.zone.quality,
        "sweep_extreme": sfp.sweep_extreme, "sfp_time": sfp.candle_time,
        "range_low": htf.range_low, "range_high": htf.range_high, "eq": htf.eq,
        "atr_h4": htf.atr_h4,
    }
    print(f"    {tag}: {combo.htf_tf} SFP detected ({sfp.direction.upper()}) — "
          f"awaiting {combo.exec_tf} MSS confirmation")
    return None

def track_signal(state: dict, symbol: str, direction: str, msg_id: int,
                  plan: TradePlan, bar_index_exec: int, combo: Combo):
    exec_iv_ms = INTERVAL_MS[combo.exec_tf]
    state.setdefault("active_signals", []).append({
        "symbol": symbol, "direction": direction, "msg_id": msg_id,
        "combo": combo.id, "exec_tf": combo.exec_tf,
        "signal_max_age_hours": combo.signal_max_age_hours,
        "bar_index": bar_index_exec, "signal_bar_time": bar_index_exec * exec_iv_ms,
        "entry": plan.entry, "tp1": plan.tp1, "tp2": plan.tp2, "sl": plan.sl,
        "tp1_hit": False, "resolved": False,
    })

def check_active_signals(state: dict, reference_ms: int):
    signals = list(state.get("active_signals", []))
    if not signals:
        return
    still_active = []
    for sig in signals:
        if sig.get("resolved", False):
            continue

        # Legacy signals (pre-multi-combo) default to the H4/15m combo's
        # execution timeframe and max age so in-flight tracking survives upgrade.
        exec_tf = sig.get("exec_tf", "15m")
        max_age_hours = sig.get("signal_max_age_hours", COMBOS["h4_15m"].signal_max_age_hours)
        signal_bar_time_ms = sig.get("signal_bar_time")
        age_hours = (reference_ms - signal_bar_time_ms) / 3_600_000.0 if signal_bar_time_ms else 0.0
        if age_hours > max_age_hours:
            print(f"  [TRACK] {sig['symbol']} ({sig.get('combo', 'h4_15m')}) expired "
                  f"after {age_hours:.1f}h — dropping")
            state.setdefault("resolved_signals", []).append(
                {"symbol": sig["symbol"], "direction": sig.get("direction", ""),
                 "combo": sig.get("combo", "h4_15m"),
                 "outcome": "expired", "resolved_at": int(time.time())})
            continue

        symbol, direction, msg_id = sig["symbol"], sig["direction"], sig["msg_id"]
        tp1, tp2, sl = sig["tp1"], sig["tp2"], sig["sl"]
        tp1_hit = sig.get("tp1_hit", False)
        last_ts = sig.get("last_processed_candle_ts", signal_bar_time_ms or 0)
        exec_n = COMBOS.get(sig.get("combo", "h4_15m"), COMBOS["h4_15m"]).n_exec

        try:
            candles = get_candles(symbol, exec_tf, exec_n, start_time_ms=signal_bar_time_ms, reference_ms=reference_ms)
        except Exception as e:
            print(f"  [TRACK] candle fetch failed for {symbol}: {e}")
            still_active.append(sig)
            continue
        new_candles = [c for c in candles if c["t"] > last_ts]
        if not new_candles:
            still_active.append(sig)
            continue

        def resolve(outcome):
            state.setdefault("resolved_signals", []).append(
                {"symbol": symbol, "direction": direction, "outcome": outcome, "resolved_at": int(time.time())})
            sig["resolved"] = True

        for c in new_candles:
            last_ts = c["t"]
            if direction == "long":
                if not tp1_hit:
                    if c["h"] >= tp1 and c["l"] <= sl:
                        if abs(sl - c["o"]) < abs(tp1 - c["o"]):
                            react_to_message(msg_id, REACT_SL); resolve("sl"); break
                        react_to_message(msg_id, REACT_TP1); tp1_hit = True; sig["tp1_hit"] = True
                    elif c["h"] >= tp1:
                        react_to_message(msg_id, REACT_TP1); tp1_hit = True; sig["tp1_hit"] = True
                    elif c["l"] <= sl:
                        react_to_message(msg_id, REACT_SL); resolve("sl"); break
                if tp1_hit and c["h"] >= tp2:
                    react_to_message(msg_id, REACT_TP2); resolve("tp2"); break
                if tp1_hit and not sig.get("resolved", False) and c["l"] <= sl:
                    resolve("tp1"); break
            else:
                if not tp1_hit:
                    if c["l"] <= tp1 and c["h"] >= sl:
                        if abs(sl - c["o"]) < abs(tp1 - c["o"]):
                            react_to_message(msg_id, REACT_SL); resolve("sl"); break
                        react_to_message(msg_id, REACT_TP1); tp1_hit = True; sig["tp1_hit"] = True
                    elif c["l"] <= tp1:
                        react_to_message(msg_id, REACT_TP1); tp1_hit = True; sig["tp1_hit"] = True
                    elif c["h"] >= sl:
                        react_to_message(msg_id, REACT_SL); resolve("sl"); break
                if tp1_hit and c["l"] <= tp2:
                    react_to_message(msg_id, REACT_TP2); resolve("tp2"); break
                if tp1_hit and not sig.get("resolved", False) and c["h"] >= sl:
                    resolve("tp1"); break

        if not sig.get("resolved", False):
            sig["last_processed_candle_ts"] = last_ts
            still_active.append(sig)

    state["active_signals"] = still_active


def get_outcome_summary(state: dict) -> str:
    """
    Aggregate win-rate summary computed from state["resolved_signals"],
    which check_active_signals() has already been populating all along.

    Outcome semantics (see resolve() in check_active_signals):
      "tp2"     -> full win (TP1 then TP2)
      "tp1"     -> partial win (TP1 secured, later stopped out at/after BE)
      "sl"      -> loss (stopped before TP1)
      "expired" -> no outcome reached before max age — excluded from win rate
    """
    resolved = state.get("resolved_signals", [])
    if not resolved:
        return "[OUTCOMES] No resolved signals tracked yet."

    counts = {"tp2": 0, "tp1": 0, "sl": 0, "expired": 0}
    per_combo: dict[str, dict[str, int]] = {}
    for r in resolved:
        outcome = r.get("outcome", "expired")
        counts[outcome] = counts.get(outcome, 0) + 1
        combo_id = r.get("combo", "unknown")
        bucket = per_combo.setdefault(combo_id, {"tp2": 0, "tp1": 0, "sl": 0, "expired": 0})
        bucket[outcome] = bucket.get(outcome, 0) + 1

    wins = counts["tp2"] + counts["tp1"]
    losses = counts["sl"]
    decided = wins + losses
    win_rate = (wins / decided * 100) if decided else 0.0

    lines = [
        f"[OUTCOMES] {wins}W / {losses}L ({win_rate:.1f}%) — "
        f"{counts['tp2']} full TP2, {counts['tp1']} partial TP1, "
        f"{counts['expired']} expired — {len(resolved)} resolved total"
    ]
    for combo_id, bucket in per_combo.items():
        c_wins = bucket["tp2"] + bucket["tp1"]
        c_losses = bucket["sl"]
        c_decided = c_wins + c_losses
        c_rate = (c_wins / c_decided * 100) if c_decided else 0.0
        label = COMBOS[combo_id].label if combo_id in COMBOS else combo_id
        lines.append(f"           {label}: {c_wins}W / {c_losses}L ({c_rate:.1f}%)")
    return "\n".join(lines)


# ══════════════════════════════════════════════════════════════════
# MAIN SCAN LOOP
# ══════════════════════════════════════════════════════════════════

def main():
    print(f"[{datetime.now(timezone.utc).isoformat()}] {ENGINE_NAME} v{__version__} starting…")
    print(f"Watchlist ({len(WATCHLIST)} pairs): {[hl_coin(s) for s in WATCHLIST]}")
    print(f"Combos: {', '.join(c.label for c in COMBOS.values())}")

    reference_ms = int(time.time() * 1000)
    bar_index_by_tf = {tf: reference_ms // iv for tf, iv in INTERVAL_MS.items()}
    state = load_state()

    print("[TRACK] Checking active signals…")
    check_active_signals(state, reference_ms)
    save_state(state)

    print("[INIT] Fetching market context…")
    get_meta_and_asset_ctxs()

    if _shutdown:
        save_state(state); sys.exit(0)

    print("[PHASE 1] Fetching candles…")
    bundles: dict[str, dict[str, list]] = {}
    with ThreadPoolExecutor(max_workers=max(1, SCAN_WORKERS)) as ex:
        futures = {ex.submit(fetch_all_candles, sym, reference_ms): sym for sym in WATCHLIST}
        for fut in as_completed(futures):
            sym = futures[fut]
            try:
                data = fut.result()
                if data is not None:
                    bundles[sym] = data
            except Exception as e:
                print(f"    ERROR fetching {sym}: {e}")

    if _shutdown:
        save_state(state); sys.exit(0)

    print("[PHASE 2] Running SMC sequence per symbol per combo…")
    results = []
    for sym in WATCHLIST:
        candles_by_tf = bundles.get(sym)
        if candles_by_tf is None:
            print(f"    Skipping {hl_coin(sym)}: insufficient candles")
            continue
        try:
            oi_usd = get_open_interest_usd(sym)
            if oi_usd is not None and oi_usd < 500_000:
                print(f"    Skipping {hl_coin(sym)}: OI too low (${oi_usd:,.0f})")
                continue
        except Exception as e:
            print(f"    ERROR checking OI for {sym}: {e}")
            continue

        for combo in COMBOS.values():
            candles_exec = candles_by_tf.get(combo.exec_tf)
            candles_htf  = candles_by_tf.get(combo.htf_tf)
            candles_d    = candles_by_tf.get("1d")
            if not candles_exec or not candles_htf or not candles_d:
                continue
            bundle = (candles_exec, candles_htf, candles_d)
            try:
                res = process_symbol(sym, combo, state, bundle, reference_ms,
                                      bar_index_by_tf[combo.htf_tf], bar_index_by_tf[combo.exec_tf])
                if res:
                    results.append(res)
            except Exception as e:
                print(f"    ERROR processing {sym} [{combo.label}]: {e}")

    # -- Rank before truncating: previously the top MAX_SIGNALS_PER_SCAN
    #    results were whichever symbols happened to resolve first out of
    #    process_symbol(), which has nothing to do with setup quality.
    #    Rank by TP2 R-multiple first (bigger reward-to-risk = better setup),
    #    then by HTF POI confluence quality as a tiebreak. --
    results.sort(key=lambda r: (r["plan"].r_multiple_tp2, r["zone_quality"]), reverse=True)

    # -- Diversification cap: don't let one correlated basket (e.g. every
    #    L1 alt) eat every signal slot in the scan, mirroring Nyx's
    #    MAX_PER_SECTOR cap. Applied after ranking, so within a sector the
    #    highest-quality setup is still the one that gets through. --
    diversified = []
    sector_used: dict[str, int] = {}
    for res in results:
        if len(diversified) >= MAX_SIGNALS_PER_SCAN:
            break
        sector = SECTOR_MAP.get(res["symbol"], "other")
        if sector_used.get(sector, 0) >= MAX_PER_SECTOR:
            print(f"  [SKIP] {hl_coin(res['symbol'])} {res['direction'].upper()} "
                  f"[{res['combo'].label}] — sector '{sector}' cap reached ({MAX_PER_SECTOR})")
            continue
        diversified.append(res)
        sector_used[sector] = sector_used.get(sector, 0) + 1
    results = diversified

    signals_fired = 0
    for res in results:
        symbol, direction, plan, combo = res["symbol"], res["direction"], res["plan"], res["combo"]
        coin = hl_coin(symbol)
        mark_cache = get_meta_and_asset_ctxs() or {}
        market_price = (mark_cache.get(coin) or {}).get("mark_px")

        # -- Staleness guard: refuse to fire if we can't confirm the live
        #    price is still close to the planned entry. A missing price is
        #    treated as a failure, not silently ignored. --
        if market_price is None:
            print(f"  [SKIP] {coin} {direction.upper()} [{combo.label}] — could not fetch live mark price, "
                  f"refusing to send a signal with unverified entry")
            continue

        risk = abs(plan.entry - plan.sl)
        drift = abs(market_price - plan.entry)
        drift_r = safe_div(drift, risk)
        if drift_r > MAX_ENTRY_DRIFT_R:
            print(f"  [SKIP] {coin} {direction.upper()} [{combo.label}] — entry stale: "
                  f"market={fmt_px(market_price)} is {drift_r:.2f}R from entry={fmt_px(plan.entry)} "
                  f"(max {MAX_ENTRY_DRIFT_R}R)")
            continue

        msg = format_signal(symbol, direction, plan,
                             HTFBias(direction == "long" and "bullish" or "bearish", 0, 0, 0, 0),
                             res["zone_kind"], combo, market_price)
        msg_id = send_telegram(msg)
        if msg_id:
            update_cooldown(state, symbol, direction, bar_index_by_tf[combo.htf_tf], combo)
            track_signal(state, symbol, direction, msg_id, plan, res["bar_index_exec"], combo)
            print(f"  [FIRED] {hl_coin(symbol)} {direction.upper()} [{combo.label}] "
                  f"TP1={fmt_px(plan.tp1)} TP2={fmt_px(plan.tp2)} SL={fmt_px(plan.sl)}")
            signals_fired += 1
        else:
            print(f"  [TG FAIL] {hl_coin(symbol)} {direction.upper()} [{combo.label}] — Telegram send failed")
        time.sleep(0.5)

    save_state(state)
    print(f"Scan complete. {signals_fired} signal(s) fired.")
    print(get_outcome_summary(state))

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        try:
            send_telegram(f"🚨 {ENGINE_NAME} crashed: {e}")
        except Exception:
            pass
        raise
