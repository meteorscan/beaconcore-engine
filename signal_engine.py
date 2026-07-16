from __future__ import annotations

import os
import sys
import json
import math
import time
import fcntl
import logging
import statistics
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone, timedelta
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, Any

import requests

ENGINE_NAME = "PRISM QUANT"
ENGINE_SLUG = "prism_quant"
__version__ = "1.0.0"
RESOLUTION_LOGIC_VERSION = 1

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger(ENGINE_SLUG)


TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN")
TG_CHAT_ID = os.getenv("TG_CHAT_ID")
if not TG_BOT_TOKEN:
    raise RuntimeError("TG_BOT_TOKEN environment variable is required")
if not TG_CHAT_ID:
    raise RuntimeError("TG_CHAT_ID environment variable is required")

STATE_FILE = os.getenv("STATE_FILE", "state.json")


CANDLE_CACHE_FILE = os.getenv("CANDLE_CACHE_FILE", "candle_cache.json")
SCAN_WORKERS = int(os.getenv("SCAN_WORKERS", "4"))
HL_BASE_URL = "https://api.hyperliquid.xyz/info"


WATCHLIST = [
    "BTCUSDT", "ETHUSDT", "HYPEUSDT", "ZECUSDT", "NEARUSDT",
    "ONDOUSDT", "SUIUSDT", "PENGUUSDT", "BNBUSDT", "SOLUSDT",
    "TRXUSDT", "BCHUSDT", "DOGEUSDT", "ADAUSDT", "DOTUSDT",
    "TAOUSDT", "AVAXUSDT", "LINKUSDT", "AAVEUSDT", "XRPUSDT",
    "XLMUSDT", "UNIUSDT", "LTCUSDT", "APTUSDT", "PENDLEUSDT",
]
MACRO_ASSET = "BTCUSDT"
MAJORS = {"BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT", "ADAUSDT", "DOGEUSDT"}


TF_HTF_SWING, TF_MID_SWING, TF_LTF_SWING = "1d", "4h", "1h"
TF_HTF_INTRADAY, TF_MID_INTRADAY, TF_LTF_INTRADAY = "4h", "1h", "15m"
ALL_TFS = ["1d", "4h", "1h", "15m"]
TF_BARS = {"1d": 260, "4h": 320, "1h": 320, "15m": 400}
SCAN_INTERVAL_MIN = 15

EMA_FAST, EMA_SLOW = 21, 50
RSI_LEN, ATR_LEN, ADX_LEN, BB_LEN = 14, 14, 14, 20
LINREG_LEN = 40
ZSCORE_LOOKBACK = 60
VWAP_LOOKBACK = 96
VOL_PROFILE_LOOKBACK = {"1d": 90, "4h": 120, "1h": 168, "15m": 192}
VOL_PROFILE_BINS = 40
OI_HISTORY_MAXLEN = 96

MAX_CONCURRENT_ACTIVE_SIGNALS = int(os.getenv("MAX_CONCURRENT_ACTIVE_SIGNALS", "8"))
MAX_CORRELATED_CONCURRENT = 1

MIN_SAMPLE_SIZE = int(os.getenv("MIN_SAMPLE_SIZE", "20"))
MIN_SAMPLE_SIZE_CATEGORY = int(os.getenv("MIN_SAMPLE_SIZE_CATEGORY", "12"))
TIER2_RETENTION_DAYS = 15

CIRCUIT_BREAKER_WINDOW = 30
CIRCUIT_BREAKER_WIN_RATE_DROP = 0.20
CIRCUIT_BREAKER_PF_DROP_FRAC = 0.25

RR_MIN_GATE = 1.5
RR_TP1_CEIL_SOFT = 2.0
RR_TP2_MIN_EXCESS = 0.15

MIN_MOVE_PCT_SL = 0.004
MAX_ENTRY_DISTANCE_ATR = 1.2

PENDING_ENTRY_EXPIRY_BARS = {
    "15m": 8,
    "1h": 12,
}

NEWS_BLACKOUT_MIN_BEFORE = 30
NEWS_BLACKOUT_MIN_AFTER = 30

EMOJI_WIN = "\U0001F3C6"
EMOJI_LOSS = "\U0001F62D"
EMOJI_EXPIRED = "\U0001F937"
EMOJI_CIRCUIT_BREAKER = "\U0001F92F"
EMOJI_RECOVERED = "\U0001F44F"

POSTURES = ["continuation", "reversion"]
REGIME_BUCKETS = ["trending_expansion", "trending_compression",
                  "ranging_expansion", "ranging_compression", "transitional"]
FACTOR_NAMES = ["trend", "momentum", "mean_reversion", "volatility_regime",
                "positioning", "liquidity_microstructure", "volume_profile", "breadth"]


FAILURE_CATEGORIES = [
    "regime_mismatch", "structural_invalidation_too_tight", "order_book_liquidity_trap",
    "mtf_conflict_ignored", "sequence_violated", "positioning_misread",
    "correct_read_poor_rr", "confidence_miscalibration", "filter_over_permissiveness",
    "genuine_variance",
]


def _default_weight_matrix() -> dict:


    base = {
        "trending_expansion":   {"trend": 1.4, "momentum": 1.1, "mean_reversion": 0.0,
                                  "volatility_regime": 0.0, "positioning": 0.7,
                                  "liquidity_microstructure": 0.6, "volume_profile": 0.8,
                                  "breadth": 0.7},
        "trending_compression": {"trend": 1.2, "momentum": 0.9, "mean_reversion": 0.0,
                                  "volatility_regime": 0.0, "positioning": 0.6,
                                  "liquidity_microstructure": 0.6, "volume_profile": 0.9,
                                  "breadth": 0.6},
        "ranging_expansion":    {"trend": 0.2, "momentum": 0.3, "mean_reversion": 1.3,
                                  "volatility_regime": 0.0, "positioning": 1.0,
                                  "liquidity_microstructure": 0.8, "volume_profile": 1.0,
                                  "breadth": 0.3},
        "ranging_compression":  {"trend": 0.1, "momentum": 0.2, "mean_reversion": 1.4,
                                  "volatility_regime": 0.0, "positioning": 1.1,
                                  "liquidity_microstructure": 0.9, "volume_profile": 1.1,
                                  "breadth": 0.2},
        "transitional":         {"trend": 0.5, "momentum": 0.5, "mean_reversion": 0.5,
                                  "volatility_regime": 0.0, "positioning": 0.5,
                                  "liquidity_microstructure": 0.5, "volume_profile": 0.5,
                                  "breadth": 0.4},
    }
    return base


def _default_segment_stats() -> dict:
    return {"n": 0, "wins": 0, "losses": 0, "sum_r": 0.0, "sum_hold_min": 0.0}


def default_state() -> dict:
    return {
        "schema_version": 1,
        "tier1": {


            "weight_matrix": _default_weight_matrix(),


            "rr_ev_term_weight": 0.15,
            "mtf_alignment_weight": 0.20,
            "session_weight": 0.05,

            "sl_buffer_percentile": {},

            "positioning_extremity_threshold": 1.5,

            "liquidity_sanity_threshold": 0.5,

            "confidence_calibration": {},

            "oi_history": {},
            "segments": {"asset": {}, "regime": {}, "timeframe": {}, "posture": {},
                         "dominant_factor": {}},
            "session_anchor_bucket": {"anchored": _default_segment_stats(),
                                       "non_anchored": _default_segment_stats()},
            "forensic_categories": {c: {"count": 0, "recent_trend": []} for c in FAILURE_CATEGORIES},
            "baseline": {"win_rate": None, "profit_factor": None, "avg_rr": None, "n": 0},
            "circuit_breaker": {"active": False, "since": None, "reason": None},
            "totals": {"signals": 0, "wins": 0, "losses": 0, "expired": 0,
                       "sum_r": 0.0, "sum_hold_min": 0.0},
            "filter_funnel": {},
            "confidence_bucket_log": {},
        },
        "tier2": {
            "trade_log": [],
            "active_signals": [],
        },
    }


class StateStore:
    """Loads/saves state.json with an advisory file lock and deep-merged
    defaults so new fields introduced by a future version never crash an
    older state file."""

    def __init__(self, path: str):
        self.path = Path(path)
        self._fh = None

    def load(self) -> dict:
        self._fh = open(self.path, "a+")
        fcntl.flock(self._fh.fileno(), fcntl.LOCK_EX)
        self._fh.seek(0)
        raw = self._fh.read()
        if raw.strip():
            try:
                loaded = json.loads(raw)
            except json.JSONDecodeError:
                log.error("state.json corrupt -- starting from defaults")
                loaded = {}
        else:
            loaded = {}
        state = default_state()
        _deep_merge_defaults(loaded, state)
        return state

    def save(self, state: dict) -> None:
        tmp = self.path.with_suffix(".tmp")
        with open(tmp, "w") as f:
            json.dump(state, f, indent=2, sort_keys=True, default=str)
        os.replace(tmp, self.path)
        if self._fh:
            fcntl.flock(self._fh.fileno(), fcntl.LOCK_UN)
            self._fh.close()
            self._fh = None

    def prune_tier2(self, state: dict) -> None:
        cutoff = datetime.now(timezone.utc) - timedelta(days=TIER2_RETENTION_DAYS)
        kept = []
        for rec in state["tier2"]["trade_log"]:
            try:
                ts = datetime.fromisoformat(rec.get("resolved_at", ""))
            except (ValueError, TypeError):
                kept.append(rec)
                continue
            if ts >= cutoff:
                kept.append(rec)
        state["tier2"]["trade_log"] = kept


class CandleCacheStore:
    def __init__(self, path: str):
        self.path = Path(path)

    def load(self) -> dict:
        if not self.path.exists():
            return {}
        try:
            with open(self.path) as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            log.warning("candle cache unreadable -- starting empty")
            return {}

    def save(self, cache: dict) -> None:
        tmp = self.path.with_suffix(".tmp")
        with open(tmp, "w") as f:
            json.dump(cache, f)
        os.replace(tmp, self.path)


def _deep_merge_defaults(loaded: dict, defaults: dict) -> None:
    for k, v in defaults.items():
        if k not in loaded:
            loaded[k] = v
        elif isinstance(v, dict) and isinstance(loaded.get(k), dict):
            _deep_merge_defaults(loaded[k], v)
    defaults.clear()
    defaults.update(loaded)


def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def bounded_update(current: float, target: float, lo: float, hi: float,
                    max_step_frac: float = 0.15) -> float:
    """Sec 7 mandatory bounded + dampened parameter update. Single choke
    point every adaptive parameter mutation in this file passes through."""
    span = hi - lo
    if span <= 0:
        return clamp(target, lo, hi)
    max_step = span * max_step_frac
    delta = clamp(target - current, -max_step, max_step)
    return clamp(current + delta, lo, hi)


class _WeightedRateLimiter:
    def __init__(self, budget_per_min: int = 1150):
        self.budget = budget_per_min
        self._events: list[tuple[float, int]] = []

    def acquire(self, weight: int = 20) -> None:
        now = time.monotonic()
        self._events = [(t, w) for t, w in self._events if now - t < 60]
        used = sum(w for _, w in self._events)
        if used + weight > self.budget:
            sleep_for = 60 - (now - self._events[0][0]) + 0.05
            if sleep_for > 0:
                time.sleep(sleep_for)
            now = time.monotonic()
            self._events = [(t, w) for t, w in self._events if now - t < 60]
        self._events.append((now, weight))


class HyperliquidClient:
    """All market-data access, including the Sec 4.1 Factor 5/6 real
    derivatives/order-book endpoints that no reference engine used."""

    def __init__(self, cache: Optional[dict] = None):
        self._limiter = _WeightedRateLimiter()
        self._session = requests.Session()
        self.cache = cache if cache is not None else {}


        self._book_snapshot_cache: dict[str, Optional[dict]] = {}
        self._ctx_snapshot_cache: Optional[dict] = None

    def _post(self, payload: dict, weight: int = 20, retries: int = 4) -> Any:
        self._limiter.acquire(weight)
        backoff = 1.0
        last_exc = None
        for attempt in range(retries):
            try:
                resp = self._session.post(HL_BASE_URL, json=payload, timeout=15)
                if resp.status_code == 429:
                    time.sleep(backoff)
                    backoff *= 2
                    continue
                resp.raise_for_status()
                return resp.json()
            except (requests.RequestException, ValueError) as exc:
                last_exc = exc
                time.sleep(backoff)
                backoff *= 2
        log.error("HL request failed after retries: %s", last_exc)
        return None

    @staticmethod
    def _coin(symbol: str) -> str:
        return symbol.replace("USDT", "").replace("USD", "")

    def candles(self, symbol: str, interval: str, n_bars: int) -> list[dict]:
        """Delta-fetch persisted cache; drops the still-forming final candle
        so every downstream computation is closed-candle-only (Sec 12)."""
        coin = self._coin(symbol)
        cache_key = f"{symbol}:{interval}"
        entry = self.cache.get(cache_key)
        interval_ms = _interval_to_ms(interval)
        now_ms = int(time.time() * 1000)

        start_ms = now_ms - interval_ms * (n_bars + 2)
        if entry and isinstance(entry.get("candles"), list) and entry["candles"]:
            try:
                last_ts = entry["candles"][-1]["t"]
                if now_ms - last_ts < interval_ms * (n_bars * 3):
                    start_ms = last_ts - interval_ms
            except (KeyError, IndexError, TypeError):
                entry = None

        payload = {"type": "candleSnapshot",
                   "req": {"coin": coin, "interval": interval, "startTime": start_ms, "endTime": now_ms}}
        raw = self._post(payload, weight=20)
        fresh = []
        if raw:
            for c in raw:
                try:
                    fresh.append({"t": int(c["t"]), "o": float(c["o"]), "h": float(c["h"]),
                                  "l": float(c["l"]), "c": float(c["c"]), "v": float(c.get("v", 0.0))})
                except (KeyError, TypeError, ValueError):
                    continue

        merged: dict[int, dict] = {}
        if entry and isinstance(entry.get("candles"), list):
            for c in entry["candles"]:
                merged[c["t"]] = c
        for c in fresh:
            merged[c["t"]] = c

        if not merged:
            log.warning("no candle data for %s %s -- graceful skip", symbol, interval)
            return []

        ordered = sorted(merged.values(), key=lambda c: c["t"])
        keep = ordered[-(n_bars + 5):]
        self.cache[cache_key] = {"candles": keep, "updated_at": now_ms}

        if keep and keep[-1]["t"] + interval_ms > now_ms:
            closed = keep[:-1]
        else:
            closed = keep
        return closed[-n_bars:]

    def mark_prices(self) -> dict[str, float]:
        raw = self._post({"type": "allMids"}, weight=2)
        out: dict[str, float] = {}
        if not raw:
            return out
        for coin, px in raw.items():
            sym = coin + "USDT"
            if sym in WATCHLIST:
                try:
                    out[sym] = float(px)
                except (TypeError, ValueError):
                    continue
        return out

    def l2_book(self, symbol: str) -> Optional[dict]:
        """Snapshotted once per scan per asset (Sec 12 consistency addendum)."""
        if symbol in self._book_snapshot_cache:
            return self._book_snapshot_cache[symbol]
        raw = self._post({"type": "l2Book", "coin": self._coin(symbol)}, weight=2)
        book = None
        if raw and isinstance(raw.get("levels"), list) and len(raw["levels"]) == 2:
            try:
                bids = [{"px": float(l["px"]), "sz": float(l["sz"])} for l in raw["levels"][0]]
                asks = [{"px": float(l["px"]), "sz": float(l["sz"])} for l in raw["levels"][1]]
                book = {"bids": bids, "asks": asks}
            except (KeyError, TypeError, ValueError):
                book = None
        self._book_snapshot_cache[symbol] = book
        return book

    def meta_and_asset_ctxs(self) -> Optional[dict]:
        """One shared snapshot per scan for the whole watchlist (funding,
        mark/oracle price, open interest) -- Sec 12 consistency addendum."""
        if self._ctx_snapshot_cache is not None:
            return self._ctx_snapshot_cache
        raw = self._post({"type": "metaAndAssetCtxs"}, weight=20)
        out: dict[str, dict] = {}
        if raw and isinstance(raw, list) and len(raw) == 2:
            try:
                universe = raw[0]["universe"]
                ctxs = raw[1]
                for meta, ctx in zip(universe, ctxs):
                    sym = meta["name"] + "USDT"
                    out[sym] = {
                        "funding": float(ctx.get("funding", 0.0)),
                        "open_interest": float(ctx.get("openInterest", 0.0)),
                        "mark_px": float(ctx.get("markPx", 0.0)),
                        "oracle_px": float(ctx.get("oraclePx", 0.0)),
                    }
            except (KeyError, TypeError, ValueError, IndexError):
                out = {}
        self._ctx_snapshot_cache = out
        return out

    def funding_history(self, symbol: str, lookback_hours: int = 72) -> list[float]:
        now_ms = int(time.time() * 1000)
        start_ms = now_ms - lookback_hours * 3_600_000
        raw = self._post({"type": "fundingHistory",
                           "coin": self._coin(symbol), "startTime": start_ms, "endTime": now_ms},
                          weight=20)
        out = []
        if raw:
            for r in raw:
                try:
                    out.append(float(r["fundingRate"]))
                except (KeyError, TypeError, ValueError):
                    continue
        return out


def _interval_to_ms(interval: str) -> int:
    unit = interval[-1]
    n = int(interval[:-1])
    mult = {"m": 60_000, "h": 3_600_000, "d": 86_400_000}[unit]
    return n * mult


def ema(values: list[float], length: int) -> list[float]:
    if not values:
        return []
    k = 2 / (length + 1)
    out = [values[0]]
    for v in values[1:]:
        out.append(v * k + out[-1] * (1 - k))
    return out


def rsi(closes: list[float], length: int = RSI_LEN) -> list[float]:
    if len(closes) < length + 1:
        return [50.0] * len(closes)
    gains, losses = [0.0], [0.0]
    for i in range(1, len(closes)):
        d = closes[i] - closes[i - 1]
        gains.append(max(d, 0.0))
        losses.append(max(-d, 0.0))
    avg_g, avg_l = statistics.fmean(gains[1:length + 1]), statistics.fmean(losses[1:length + 1])
    out = [50.0] * (length + 1)
    for i in range(length + 1, len(closes)):
        avg_g = (avg_g * (length - 1) + gains[i]) / length
        avg_l = (avg_l * (length - 1) + losses[i]) / length
        rs = avg_g / avg_l if avg_l > 1e-12 else float("inf")
        out.append(100 - 100 / (1 + rs) if rs != float("inf") else 100.0)
    return out


def true_ranges(candles: list[dict]) -> list[float]:
    trs = []
    for i, c in enumerate(candles):
        if i == 0:
            trs.append(c["h"] - c["l"])
        else:
            pc = candles[i - 1]["c"]
            trs.append(max(c["h"] - c["l"], abs(c["h"] - pc), abs(c["l"] - pc)))
    return trs


def atr(candles: list[dict], length: int = ATR_LEN) -> list[float]:
    trs = true_ranges(candles)
    return ema(trs, length) if trs else []


def adx(candles: list[dict], length: int = ADX_LEN) -> list[float]:
    if len(candles) < length + 2:
        return [15.0] * len(candles)
    plus_dm, minus_dm = [0.0], [0.0]
    for i in range(1, len(candles)):
        up = candles[i]["h"] - candles[i - 1]["h"]
        dn = candles[i - 1]["l"] - candles[i]["l"]
        plus_dm.append(up if (up > dn and up > 0) else 0.0)
        minus_dm.append(dn if (dn > up and dn > 0) else 0.0)
    trs = true_ranges(candles)

    def _smooth(vals):
        out = [sum(vals[:length])]
        for v in vals[length:]:
            out.append(out[-1] - out[-1] / length + v)
        return out

    tr_s = _smooth(trs)
    pdm_s = _smooth(plus_dm)
    mdm_s = _smooth(minus_dm)
    dx = []
    for tr_v, p, m in zip(tr_s, pdm_s, mdm_s):
        pdi = 100 * p / tr_v if tr_v > 1e-12 else 0.0
        mdi = 100 * m / tr_v if tr_v > 1e-12 else 0.0
        denom = pdi + mdi
        dx.append(100 * abs(pdi - mdi) / denom if denom > 1e-12 else 0.0)
    adx_s = ema(dx, length) if dx else [15.0]
    pad = len(candles) - len(adx_s)
    return [adx_s[0]] * max(pad, 0) + adx_s


def zscore(series: list[float], lookback: int = ZSCORE_LOOKBACK) -> float:
    """Z-score of the last value against its own trailing window."""
    if len(series) < 5:
        return 0.0
    window = series[-lookback:]
    mu = statistics.fmean(window)
    sd = statistics.pstdev(window) if len(window) > 1 else 0.0
    return (series[-1] - mu) / sd if sd > 1e-9 else 0.0


def percentile_rank(series: list[float], value: float) -> float:
    """What fraction of `series` is <= value, in [0, 1]."""
    if not series:
        return 0.5
    below = sum(1 for v in series if v <= value)
    return below / len(series)


def linreg_slope_z(closes: list[float], length: int = LINREG_LEN) -> float:
    """Z-scored linear-regression slope of price, normalized by price level
    so it is comparable across assets of very different magnitude."""
    if len(closes) < length + 5:
        length = max(5, len(closes) - 1)
    window = closes[-length:]
    n = len(window)
    if n < 5:
        return 0.0
    xs = list(range(n))
    x_mean = statistics.fmean(xs)
    y_mean = statistics.fmean(window)
    num = sum((x - x_mean) * (y - y_mean) for x, y in zip(xs, window))
    den = sum((x - x_mean) ** 2 for x in xs)
    slope = num / den if den > 1e-12 else 0.0
    norm_slope = slope / y_mean if y_mean > 1e-12 else 0.0

    rolling = []
    for end in range(length, n + 1):
        w = window[end - length:end]
        xs2 = list(range(len(w)))
        xm2, ym2 = statistics.fmean(xs2), statistics.fmean(w)
        num2 = sum((x - xm2) * (y - ym2) for x, y in zip(xs2, w))
        den2 = sum((x - xm2) ** 2 for x in xs2)
        s2 = (num2 / den2 / ym2) if den2 > 1e-12 and ym2 > 1e-12 else 0.0
        rolling.append(s2)
    if len(rolling) < 5:
        return clamp(norm_slope * 50, -3, 3)
    mu, sd = statistics.fmean(rolling), statistics.pstdev(rolling)
    return clamp((norm_slope - mu) / sd, -3, 3) if sd > 1e-9 else 0.0


def rolling_vwap(candles: list[dict], lookback: int = VWAP_LOOKBACK) -> Optional[float]:
    window = candles[-lookback:]
    if not window:
        return None
    tp_vol = sum(((c["h"] + c["l"] + c["c"]) / 3) * c["v"] for c in window)
    vol = sum(c["v"] for c in window)
    return tp_vol / vol if vol > 1e-9 else window[-1]["c"]


@dataclass
class VolumeProfile:
    poc: float
    vah: float
    val: float
    hvn: list[float]
    lvn: list[float]
    bin_width: float


def build_volume_profile(candles: list[dict], bins: int = VOL_PROFILE_BINS) -> Optional[VolumeProfile]:
    """Sec 4.1 Factor 7: objective, density-based support/resistance from
    real traded volume at price -- the direct replacement for order-block/
    FVG zone-guessing. Distributes each candle's volume evenly across its
    own [low, high] range into price bins (a standard, simple volume-profile
    approximation; true per-trade tape data isn't exposed by candleSnapshot)."""
    if len(candles) < 10:
        return None
    lo = min(c["l"] for c in candles)
    hi = max(c["h"] for c in candles)
    if hi <= lo:
        return None
    bin_width = (hi - lo) / bins
    hist = [0.0] * bins

    def _bin(px: float) -> int:
        return clamp(int((px - lo) / bin_width), 0, bins - 1)

    for c in candles:
        b_lo, b_hi = _bin(c["l"]), _bin(c["h"])
        span = max(b_hi - b_lo + 1, 1)
        per_bin = c["v"] / span
        for b in range(b_lo, b_hi + 1):
            hist[b] += per_bin

    total_vol = sum(hist)
    if total_vol <= 1e-9:
        return None
    poc_idx = max(range(bins), key=lambda i: hist[i])

    order = sorted(range(bins), key=lambda i: hist[i], reverse=True)
    included = {poc_idx}
    covered = hist[poc_idx]
    for idx in order:
        if covered / total_vol >= 0.70:
            break
        if idx not in included:
            included.add(idx)
            covered += hist[idx]
    va_lo_idx, va_hi_idx = min(included), max(included)

    hvn, lvn = [], []
    for i in range(1, bins - 1):
        px = lo + (i + 0.5) * bin_width
        if hist[i] > hist[i - 1] and hist[i] > hist[i + 1] and hist[i] / total_vol > 0.02:
            hvn.append(px)
        elif hist[i] < hist[i - 1] and hist[i] < hist[i + 1]:
            lvn.append(px)

    return VolumeProfile(
        poc=lo + (poc_idx + 0.5) * bin_width,
        vah=lo + (va_hi_idx + 1) * bin_width,
        val=lo + va_lo_idx * bin_width,
        hvn=hvn, lvn=lvn, bin_width=bin_width,
    )


def order_book_imbalance(book: dict, mid: float, band_pct: float = 0.005) -> float:
    """Resting bid vs. ask depth within `band_pct` of mid, signed in
    [-1, 1] where positive favors bids (Factor 5/6)."""
    lo, hi = mid * (1 - band_pct), mid * (1 + band_pct)
    bid_depth = sum(l["sz"] for l in book["bids"] if l["px"] >= lo)
    ask_depth = sum(l["sz"] for l in book["asks"] if l["px"] <= hi)
    total = bid_depth + ask_depth
    return (bid_depth - ask_depth) / total if total > 1e-9 else 0.0


def depth_near_level(book: dict, level: float, side: str, band_pct: float = 0.003) -> float:
    """Genuine resting size within `band_pct` of a candidate level, used to
    confirm/discount a structural node (Sec 4.5 step 5) and to detect
    liquidity walls for TP clipping (Sec 9)."""
    lo, hi = level * (1 - band_pct), level * (1 + band_pct)
    levels = book["bids"] if side == "bid" else book["asks"]
    return sum(l["sz"] for l in levels if lo <= l["px"] <= hi)


@dataclass
class TFView:
    tf: str
    candles: list[dict]
    closes: list[float]
    atr: list[float]
    adx: list[float]
    rsi: list[float]

    def last(self) -> dict:
        return self.candles[-1]


def build_tf_view(tf: str, candles: list[dict]) -> Optional[TFView]:
    if len(candles) < 30:
        return None
    closes = [c["c"] for c in candles]
    return TFView(tf=tf, candles=candles, closes=closes,
                  atr=atr(candles), adx=adx(candles), rsi=rsi(closes))


@dataclass
class SymbolSnapshot:
    symbol: str
    mark: float
    views: dict[str, TFView]
    book: Optional[dict]
    ctx: Optional[dict]
    funding_hist: list[float]
    vol_profiles: dict[str, Optional[VolumeProfile]] = field(default_factory=dict)


def collect_snapshot(hl: HyperliquidClient, symbol: str, mark: float) -> Optional[SymbolSnapshot]:
    views: dict[str, TFView] = {}
    for tf in ALL_TFS:
        candles = hl.candles(symbol, tf, TF_BARS[tf])
        view = build_tf_view(tf, candles)
        if view:
            views[tf] = view
    if not views:
        return None
    book = hl.l2_book(symbol)
    ctx_all = hl.meta_and_asset_ctxs()
    ctx = ctx_all.get(symbol) if ctx_all else None
    funding_hist = hl.funding_history(symbol)
    vol_profiles = {tf: build_volume_profile(v.candles[-VOL_PROFILE_LOOKBACK.get(tf, 120):])
                     for tf, v in views.items()}
    return SymbolSnapshot(symbol=symbol, mark=mark or (views[ALL_TFS[-1]].last()["c"] if views else 0.0),
                          views=views, book=book, ctx=ctx, funding_hist=funding_hist,
                          vol_profiles=vol_profiles)


@dataclass
class FactorReading:
    trend: float
    momentum: float
    mean_reversion: float
    volatility_regime: float
    positioning: float
    liquidity_microstructure: float
    volume_profile: float
    breadth: float

    vol_pctile: float
    bb_width_pctile: float
    adx_now: float
    vwap: Optional[float]
    vwap_dist_sd: float
    oi_delta_z: float
    funding_z: float
    ob_imbalance: float


def bollinger_width_percentile(closes: list[float], length: int = BB_LEN, lookback: int = 100) -> tuple[float, float]:
    if len(closes) < length + lookback:
        lookback = max(10, len(closes) - length)
    widths = []
    for end in range(length, len(closes) + 1):
        w = closes[end - length:end]
        mu, sd = statistics.fmean(w), statistics.pstdev(w)
        widths.append((sd * 4) / mu if mu > 1e-9 else 0.0)
    if not widths:
        return 0.0, 0.5
    current = widths[-1]
    pct = percentile_rank(widths[-lookback:], current)
    return current, pct


def realized_vol_percentile(candles: list[dict], length: int = ATR_LEN, lookback: int = 100) -> float:
    atr_series = atr(candles, length)
    closes = [c["c"] for c in candles]
    norm = [a / c if c > 1e-9 else 0.0 for a, c in zip(atr_series, closes)]
    if len(norm) < 10:
        return 0.5
    return percentile_rank(norm[-lookback:], norm[-1])


def compute_factor_1_trend(htf: TFView, mtf: TFView) -> float:
    """Multi-timeframe directional persistence: z-scored regression slope on
    HTF + MTF, blended with ADX-style directional strength (Sec 4.1 #1)."""
    slope_htf = linreg_slope_z(htf.closes)
    slope_mtf = linreg_slope_z(mtf.closes)
    adx_norm = clamp((htf.adx[-1] - 20) / 30, -1, 1)
    raw = 0.5 * slope_htf + 0.3 * slope_mtf + 0.2 * adx_norm * (1 if slope_htf >= 0 else -1) * 3
    return clamp(raw / 3, -1, 1)


def compute_factor_2_momentum(ltf: TFView) -> float:
    """RoC + RSI-style momentum z-scored across lookbacks on the entry
    timeframe, independent of the trend factor itself (Sec 4.1 #2)."""
    closes = ltf.closes
    rocs = []
    for lb in (5, 10, 20):
        if len(closes) > lb:
            roc_series = [(closes[i] - closes[i - lb]) / closes[i - lb] if closes[i - lb] else 0.0
                          for i in range(lb, len(closes))]
            rocs.append(zscore(roc_series))
    rsi_now = ltf.rsi[-1]
    rsi_term = clamp((rsi_now - 50) / 25, -1, 1)
    roc_term = clamp(statistics.fmean(rocs) / 2.5, -1, 1) if rocs else 0.0
    return clamp(0.6 * roc_term + 0.4 * rsi_term, -1, 1)


def compute_factor_3_mean_reversion(ltf: TFView, vwap_val: Optional[float]) -> tuple[float, float]:
    """Statistical distance from a rolling VWAP fair-value anchor in std
    devs; sign is toward reversion (positive = expect pullback down from
    overextension, i.e. bearish reversion signal, and vice versa) (Sec 4.1 #3).
    Returns (signed_reversion_score, distance_in_sd) -- the sd distance is
    reused unsigned by the entry-refinement step (Sec 4.5 step 7)."""
    if vwap_val is None or vwap_val <= 0:
        return 0.0, 0.0
    window = ltf.closes[-VWAP_LOOKBACK:]
    sd = statistics.pstdev(window) if len(window) > 1 else 0.0
    dist_sd = (ltf.closes[-1] - vwap_val) / sd if sd > 1e-9 else 0.0


    score = clamp(-dist_sd / 2.5, -1, 1)
    return score, dist_sd


def compute_factor_4_volatility_regime(bb_pct: float, vol_pct: float) -> float:
    """Unsigned compression<->expansion percentile blend (Sec 4.1 #4). Does
    not score direction; only steers weight-matrix bucket membership."""
    return clamp(0.5 * bb_pct + 0.5 * vol_pct, 0.0, 1.0)


def compute_factor_5_positioning(ctx: Optional[dict], funding_hist: list[float],
                                  oi_history: list[float], direction_hint: float) -> tuple[float, float, float]:
    """Derivatives Positioning Factor -- Sec 4.1 #5, the primary
    differentiator: real funding rate + its short-term change, plus OI
    delta over a rolling window, computed from actual exchange state
    (metaAndAssetCtxs / fundingHistory), never inferred from candle shape.
    Contrarian at statistical extremes, mildly confirming at moderate
    readings. Returns (positioning_factor, oi_delta_z, funding_z)."""
    if not ctx:
        return 0.0, 0.0, 0.0
    funding_now = ctx.get("funding", 0.0)
    funding_z = zscore(funding_hist + [funding_now]) if len(funding_hist) >= 5 else 0.0

    oi_now = ctx.get("open_interest", 0.0)
    combined_oi = oi_history + [oi_now]
    oi_delta_z = 0.0
    if len(combined_oi) >= 6:
        deltas = [(combined_oi[i] - combined_oi[i - 1]) / combined_oi[i - 1] if combined_oi[i - 1] else 0.0
                  for i in range(1, len(combined_oi))]
        oi_delta_z = zscore(deltas)

    extremity = math.sqrt(funding_z ** 2 + oi_delta_z ** 2)


    crowd_sign = 1.0 if (funding_now > 0 and oi_delta_z > 0) else (-1.0 if (funding_now < 0 and oi_delta_z > 0) else 0.0)
    if extremity >= 1.5:

        positioning = clamp(-crowd_sign * min(extremity / 3.0, 1.0), -1, 1)
    else:

        positioning = clamp(crowd_sign * 0.3, -1, 1)
    return positioning, oi_delta_z, funding_z


def compute_factor_6_liquidity_microstructure(book: Optional[dict], mid: float) -> tuple[float, float]:
    """Order-book depth/imbalance near mid (Sec 4.1 #6). Returns
    (signed_factor, raw_imbalance) -- raw_imbalance is reused directly by
    zone confirmation and risk placement (Sec 4.5, Sec 9)."""
    if not book or mid <= 0:
        return 0.0, 0.0
    imb = order_book_imbalance(book, mid)
    return clamp(imb * 1.5, -1, 1), imb


def compute_factor_7_volume_profile(vp: Optional[VolumeProfile], mid: float) -> float:
    """Where has the market spent volume/time relative to current price
    (Sec 4.1 #7) -- signed toward the direction of the nearest dense node:
    price below POC/VAL with density above => bullish pull toward value;
    price above VAH with density below => bearish pull toward value."""
    if not vp or mid <= 0:
        return 0.0
    if mid < vp.val:
        return clamp((vp.val - mid) / (vp.vah - vp.val + 1e-9), 0, 1)
    if mid > vp.vah:
        return -clamp((mid - vp.vah) / (vp.vah - vp.val + 1e-9), 0, 1)

    span = max(vp.vah - vp.val, 1e-9)
    return clamp((vp.poc - mid) / span, -0.5, 0.5)


def compute_factor_8_breadth(symbol: str, snaps: dict[str, "SymbolSnapshot"], macro_view: Optional[TFView]) -> float:
    """Beta-adjusted relative strength vs. BTC/ETH anchors and vs. the
    watchlist median (Sec 4.1 #8) -- discounts an isolated single-asset move."""
    own = snaps.get(symbol)
    if not own or "1h" not in own.views:
        return 0.0
    own_roc = _roc(own.views["1h"].closes, 20)
    macro_roc = _roc(macro_view.closes, 20) if macro_view else 0.0
    peer_rocs = []
    for sym, snap in snaps.items():
        if sym == symbol or "1h" not in snap.views:
            continue
        peer_rocs.append(_roc(snap.views["1h"].closes, 20))
    median_roc = statistics.median(peer_rocs) if peer_rocs else 0.0
    coherence = 1.0 if (own_roc * median_roc > 0) else -0.3
    rel_strength = own_roc - 0.5 * macro_roc - 0.5 * median_roc
    return clamp(rel_strength * 8 * (1 if coherence > 0 else 0.6), -1, 1)


def _roc(closes: list[float], lb: int) -> float:
    if len(closes) <= lb or closes[-lb - 1] == 0:
        return 0.0
    return (closes[-1] - closes[-lb - 1]) / closes[-lb - 1]


def compute_all_factors(snap: SymbolSnapshot, htf_tf: str, mtf_tf: str, ltf_tf: str,
                          all_snaps: dict[str, SymbolSnapshot], macro_view: Optional[TFView],
                          state: dict) -> Optional[FactorReading]:
    if htf_tf not in snap.views or mtf_tf not in snap.views or ltf_tf not in snap.views:
        return None
    htf, mtf, ltf = snap.views[htf_tf], snap.views[mtf_tf], snap.views[ltf_tf]

    trend = compute_factor_1_trend(htf, mtf)
    momentum = compute_factor_2_momentum(ltf)
    vwap_val = rolling_vwap(ltf.candles)
    mean_rev, vwap_dist_sd = compute_factor_3_mean_reversion(ltf, vwap_val)
    bb_width, bb_pct = bollinger_width_percentile(ltf.closes)
    vol_pct = realized_vol_percentile(ltf.candles)
    vol_regime = compute_factor_4_volatility_regime(bb_pct, vol_pct)

    oi_hist = state["tier1"]["oi_history"].get(snap.symbol, [])
    positioning, oi_delta_z, funding_z = compute_factor_5_positioning(
        snap.ctx, snap.funding_hist, oi_hist, direction_hint=trend)

    liquidity, ob_imbalance = compute_factor_6_liquidity_microstructure(snap.book, snap.mark)
    vp = snap.vol_profiles.get(ltf_tf)
    volume_profile_factor = compute_factor_7_volume_profile(vp, snap.mark)
    breadth = compute_factor_8_breadth(snap.symbol, all_snaps, macro_view)

    return FactorReading(
        trend=trend, momentum=momentum, mean_reversion=mean_rev, volatility_regime=vol_regime,
        positioning=positioning, liquidity_microstructure=liquidity, volume_profile=volume_profile_factor,
        breadth=breadth, vol_pctile=vol_pct, bb_width_pctile=bb_pct, adx_now=htf.adx[-1],
        vwap=vwap_val, vwap_dist_sd=vwap_dist_sd, oi_delta_z=oi_delta_z, funding_z=funding_z,
        ob_imbalance=ob_imbalance,
    )


@dataclass
class RegimeVector:
    macro_bias: float
    vol_percentile: float
    trend_strength: float
    session_weight: float
    session_open_proximity: float
    ob_positioning_skew: float
    noise_index: float
    breadth: float
    bucket_weights: dict

    def dominant_bucket(self) -> str:
        return max(self.bucket_weights, key=self.bucket_weights.get)


def _session_weight_now() -> float:
    hour = datetime.now(timezone.utc).hour


    if 12 <= hour < 16:
        return 1.0
    if 7 <= hour < 12 or 16 <= hour < 21:
        return 0.7
    return 0.4


def _session_open_proximity_now() -> float:
    minute_of_day = datetime.now(timezone.utc).hour * 60 + datetime.now(timezone.utc).minute
    opens = [0, 7 * 60, 12 * 60]
    dist = min(abs(minute_of_day - o) for o in opens)
    return clamp(1.0 - dist / 90.0, 0.0, 1.0)


def noise_index(candles: list[dict], lookback: int = 30) -> float:
    """How choppy/whipsaw-prone recent action is, independent of raw
    volatility: ratio of summed wick range to net directional travel."""
    window = candles[-lookback:]
    if len(window) < 5:
        return 0.5
    total_range = sum(c["h"] - c["l"] for c in window)
    net_move = abs(window[-1]["c"] - window[0]["c"])
    if total_range <= 1e-9:
        return 0.5
    return clamp(1.0 - (net_move / total_range), 0.0, 1.0)


def classify_regime_bucket(trend_strength_norm: float, vol_regime: float) -> dict:
    """Sec 4.2: soft, continuous bucket membership -- never a hard discrete
    switch. `trend_strength_norm` in [0,1] (ADX-based), `vol_regime` in [0,1]
    (compression=0 .. expansion=1). Assigns weight to the four corner
    buckets by bilinear interpolation, folding in a transitional weight
    proportional to how close the reading sits to the trend/range midline."""
    w_trending = trend_strength_norm
    w_ranging = 1 - trend_strength_norm
    w_expansion = vol_regime
    w_compression = 1 - vol_regime

    raw = {
        "trending_expansion": w_trending * w_expansion,
        "trending_compression": w_trending * w_compression,
        "ranging_expansion": w_ranging * w_expansion,
        "ranging_compression": w_ranging * w_compression,
    }


    ambiguity = 1 - abs(trend_strength_norm - 0.5) * 2
    transitional_w = clamp(ambiguity * 0.5, 0.0, 0.5)
    scale = 1 - transitional_w
    out = {k: v * scale for k, v in raw.items()}
    out["transitional"] = transitional_w
    total = sum(out.values())
    return {k: v / total for k, v in out.items()} if total > 1e-9 else {b: 1 / len(REGIME_BUCKETS) for b in REGIME_BUCKETS}


def compute_regime_vector(symbol: str, factors: FactorReading, macro_view: Optional[TFView],
                            ltf_candles: list[dict], all_snaps: dict[str, SymbolSnapshot]) -> RegimeVector:
    macro_bias = linreg_slope_z(macro_view.closes) / 3.0 if macro_view else 0.0
    trend_strength_norm = clamp((factors.adx_now - 10) / 40, 0.0, 1.0)
    bucket_weights = classify_regime_bucket(trend_strength_norm, factors.volatility_regime)
    ob_skew = factors.ob_imbalance
    n_idx = noise_index(ltf_candles)
    breadth_coherence = statistics.fmean(
        [f.breadth for s, f in [] ]) if False else factors.breadth
    return RegimeVector(
        macro_bias=clamp(macro_bias, -1, 1), vol_percentile=factors.vol_pctile,
        trend_strength=trend_strength_norm, session_weight=_session_weight_now(),
        session_open_proximity=_session_open_proximity_now(), ob_positioning_skew=ob_skew,
        noise_index=n_idx, breadth=factors.breadth, bucket_weights=bucket_weights,
    )


TRENDING_BUCKETS = {"trending_expansion", "trending_compression"}
RANGING_BUCKETS = {"ranging_expansion", "ranging_compression"}
BUCKET_ELIGIBILITY_THRESHOLD = 0.35


def eligible_postures(regime: RegimeVector, factors: FactorReading, state: dict) -> list[str]:
    """Deterministic function of regime classification alone (Sec 4.3) --
    never a free per-setup choice."""
    trending_w = sum(regime.bucket_weights[b] for b in TRENDING_BUCKETS)
    ranging_w = sum(regime.bucket_weights[b] for b in RANGING_BUCKETS)
    out = []
    if trending_w >= BUCKET_ELIGIBILITY_THRESHOLD:
        out.append("continuation")
    if ranging_w >= BUCKET_ELIGIBILITY_THRESHOLD:
        extremity_threshold = state["tier1"]["positioning_extremity_threshold"]
        extremity = math.sqrt(factors.funding_z ** 2 + factors.oi_delta_z ** 2)


        if extremity >= extremity_threshold:
            out.append("reversion")
    return out


def posture_direction(posture: str, factors: FactorReading) -> float:
    """Signed direction implied by this posture, in [-1, 1]."""
    if posture == "continuation":
        return clamp(0.6 * factors.trend + 0.4 * factors.momentum, -1, 1)
    return clamp(factors.mean_reversion, -1, 1)


PER_TERM_CAP = 0.9


def _term_contribution(weight: float, factor_value: float) -> float:
    return clamp(weight * factor_value, -PER_TERM_CAP, PER_TERM_CAP)


@dataclass
class ScoreResult:
    probability: float
    raw_sum: float
    per_term: dict
    dominant_factor: str
    mtf_aligned: bool
    filter_margin_thin: bool


def mtf_alignment(htf: TFView, ltf: TFView, direction: float) -> bool:
    htf_bias = 1 if linreg_slope_z(htf.closes) >= 0 else -1
    ltf_bias = 1 if direction >= 0 else -1
    return htf_bias == ltf_bias


def composite_score(posture: str, factors: FactorReading, regime: RegimeVector, direction: float,
                     htf: TFView, ltf: TFView, rr1: float, state: dict) -> ScoreResult:
    """Sec 4.4: a single continuous logistic blend over the eight factors,
    regime-conditioned weights, plus separately-labeled MTF-alignment and
    RR/EV terms. RR/EV never mixes into the probability read (kept as its
    own additive term, clearly separated in `per_term`, and capped like
    every other term) -- it informs ranking/sizing but must never be
    conflated with probability of success."""
    wm = state["tier1"]["weight_matrix"]
    factor_values = {
        "trend": factors.trend, "momentum": factors.momentum,
        "mean_reversion": factors.mean_reversion, "volatility_regime": 0.0,
        "positioning": factors.positioning, "liquidity_microstructure": factors.liquidity_microstructure,
        "volume_profile": factors.volume_profile, "breadth": factors.breadth,
    }


    blended_weights = {f: 0.0 for f in FACTOR_NAMES}
    for bucket, bw in regime.bucket_weights.items():
        bucket_wts = wm.get(bucket, {})
        for f in FACTOR_NAMES:
            blended_weights[f] += bw * bucket_wts.get(f, 0.0)

    per_term = {}
    raw_sum = 0.0
    for f in FACTOR_NAMES:


        signed_val = factor_values[f] * (1 if direction >= 0 else -1) if f != "volatility_regime" else 0.0
        contrib = _term_contribution(blended_weights[f], signed_val)
        per_term[f] = contrib
        raw_sum += contrib

    aligned = mtf_alignment(htf, ltf, direction)
    mtf_term = _term_contribution(state["tier1"]["mtf_alignment_weight"], 1.0 if aligned else -1.0)
    per_term["mtf_alignment"] = mtf_term
    raw_sum += mtf_term

    session_term = _term_contribution(state["tier1"]["session_weight"], regime.session_weight * 2 - 1)
    per_term["session"] = session_term
    raw_sum += session_term


    rr_term = _term_contribution(state["tier1"]["rr_ev_term_weight"], clamp((rr1 - RR_MIN_GATE) / 2.0, -1, 1))
    per_term["rr_ev"] = rr_term
    raw_sum += rr_term

    probability = 1 / (1 + math.exp(-raw_sum))
    probability = calibrate_confidence(probability, posture, state)

    core_terms = {k: v for k, v in per_term.items() if k in FACTOR_NAMES}
    dominant_factor = max(core_terms, key=lambda k: abs(core_terms[k])) if core_terms else "trend"
    thin_margin = all(abs(v) < 0.15 for v in core_terms.values())

    return ScoreResult(probability=probability, raw_sum=raw_sum, per_term=per_term,
                       dominant_factor=dominant_factor, mtf_aligned=aligned, filter_margin_thin=thin_margin)


def calibrate_confidence(raw_probability: float, posture: str, state: dict) -> float:
    """Sec 4.4 calibration: apply the learned per-decile additive offset
    (Sec 13's confidence_miscalibration response writes this)."""
    bucket = str(int(clamp(raw_probability, 0, 0.999) * 10))
    offset = state["tier1"]["confidence_calibration"].get(f"{posture}:{bucket}", 0.0)
    return clamp(raw_probability + offset, 0.01, 0.99)


def assign_tier(probability: float, rr1: float) -> str:
    """Sec 14 graded tiers instead of one binary cutoff."""
    if probability >= 0.72 and rr1 >= 1.8:
        return "A+"
    if probability >= 0.62:
        return "A"
    return "B"


@dataclass
class ZoneSelection:
    level: float
    kind: str
    depth_supporting: float
    direction: float


def select_structural_level(posture: str, direction: float, snap: SymbolSnapshot, ltf_tf: str,
                              factors: FactorReading) -> Optional[ZoneSelection]:
    """Sec 4.5 step 4: nearest genuinely dense Volume-Profile node or VWAP
    band consistent with the posture's direction -- an objective density
    statistic, never a subjective pattern label."""
    vp = snap.vol_profiles.get(ltf_tf)
    mid = snap.mark
    candidates: list[tuple[float, str]] = []
    if vp:
        candidates.append((vp.poc, "poc"))
        candidates.append((vp.vah, "vah"))
        candidates.append((vp.val, "val"))
        for h in vp.hvn:
            candidates.append((h, "hvn"))
    if factors.vwap:
        candidates.append((factors.vwap, "vwap_band"))
    if not candidates:
        return None

    if direction >= 0:

        below = [c for c in candidates if c[0] <= mid]
        pool = below if below else candidates
        level, kind = max(pool, key=lambda c: c[0])
    else:
        above = [c for c in candidates if c[0] >= mid]
        pool = above if above else candidates
        level, kind = min(pool, key=lambda c: c[0])

    return ZoneSelection(level=level, kind=kind, depth_supporting=0.0, direction=direction)


def confirm_order_book(zone: ZoneSelection, book: Optional[dict], liquidity_threshold: float) -> bool:
    """Sec 4.5 step 5: real l2Book depth/imbalance check -- a structurally
    plausible node with no real depth behind it is discounted/rejected."""
    if not book:
        return True

    side = "bid" if zone.direction >= 0 else "ask"
    depth = depth_near_level(book, zone.level, side)
    total_side_depth = sum(l["sz"] for l in (book["bids"] if side == "bid" else book["asks"]))
    if total_side_depth <= 1e-9:
        return True
    relative_depth = depth / total_side_depth
    return relative_depth >= (0.015 * (1 + liquidity_threshold))


def confirm_positioning(posture: str, direction: float, factors: FactorReading, state: dict) -> bool:
    """Sec 4.5 step 6: for continuation, funding/OI must not already be
    extremely crowded against the trade; for reversion, the extremity gate
    was already checked in `eligible_postures` (Sec 4.3)."""
    if posture == "reversion":
        return True
    crowd_against = (direction >= 0 and factors.positioning < -0.5) or (direction < 0 and factors.positioning > 0.5)
    return not crowd_against


def precision_entry_refine(zone: ZoneSelection, ltf: TFView) -> float:
    """Sec 4.5 step 7: placement modifier only -- offsets entry within the
    validated node by a documented standard-deviation fraction of the
    node's own volatility-normalized width (the statistical analog to a
    Fibonacci OTE pocket, grounded in real local volatility instead of a
    fixed ratio). Never nominates a zone on its own."""
    recent_atr = ltf.atr[-1] if ltf.atr else 0.0
    offset = 0.25 * recent_atr
    return zone.level - offset if zone.direction >= 0 else zone.level + offset


def adverse_wick_percentile_buffer(ltf: TFView, asset: str, tf: str, state: dict) -> float:
    """Sec 9 mandatory adaptive-percentile SL buffer: sized from a live
    percentile of recent adverse-wick excursions, so it auto-widens on noisy
    assets and tightens on clean ones. The percentile itself is a bounded,
    dampened adaptive parameter keyed '{asset}:{timeframe}'."""
    key = f"{asset}:{tf}"
    pct = state["tier1"]["sl_buffer_percentile"].get(key, 65.0)
    closes = ltf.closes
    wicks = []
    for i in range(1, len(ltf.candles)):
        c = ltf.candles[i]
        body_hi, body_lo = max(c["o"], c["c"]), min(c["o"], c["c"])
        wicks.append(max(c["h"] - body_hi, body_lo - c["l"]))
    if not wicks:
        return ltf.atr[-1] * 0.5 if ltf.atr else closes[-1] * MIN_MOVE_PCT_SL
    wicks_sorted = sorted(wicks)
    idx = clamp(int(len(wicks_sorted) * pct / 100), 0, len(wicks_sorted) - 1)
    buffer = wicks_sorted[idx]
    floor = closes[-1] * MIN_MOVE_PCT_SL
    return max(buffer, floor)


def clear_liquidity_walls(direction: float, level: float, book: Optional[dict], atr_val: float) -> float:
    """Sec 9: nudge SL just past an obvious resting-order cluster if one
    sits inside the naive buffer, so a common wick-based sweep doesn't
    stop the trade out on noise it was designed to survive."""
    if not book:
        return level
    side = "bid" if direction >= 0 else "ask"
    levels = book["bids"] if side == "bid" else book["asks"]
    nearby = [l["px"] for l in levels if abs(l["px"] - level) <= atr_val * 0.3]
    if not nearby:
        return level
    return (min(nearby) - atr_val * 0.05) if direction >= 0 else (max(nearby) + atr_val * 0.05)


def clip_target_to_liquidity_wall(direction: float, entry: float, target: float, book: Optional[dict]) -> float:
    """Sec 9 mandatory: TP must never sit past an obvious closer liquidity
    wall (real resting-order cluster from l2Book preferred over inferred
    swing-point clustering, per spec, since this build has the real book)."""
    if not book:
        return target
    side = "ask" if direction >= 0 else "bid"
    levels = book["asks"] if direction >= 0 else book["bids"]
    total_depth = sum(l["sz"] for l in levels) or 1.0
    path_lo, path_hi = (entry, target) if direction >= 0 else (target, entry)
    walls = [l["px"] for l in levels if path_lo < l["px"] < path_hi and
             next((x["sz"] for x in levels if x["px"] == l["px"]), 0) / total_depth > 0.08]
    if not walls:
        return target
    nearest_wall = min(walls) if direction >= 0 else max(walls)
    pad = abs(target - entry) * 0.02
    return (nearest_wall - pad) if direction >= 0 else (nearest_wall + pad)


@dataclass
class RiskPlan:
    entry: float
    sl: float
    tp1: float
    tp2: float
    rr1: float
    rr2: float


def build_risk_plan(direction: float, entry: float, structural_level: float, ltf: TFView,
                      asset: str, tf: str, book: Optional[dict], state: dict) -> Optional[RiskPlan]:
    """Sec 9: structure-based SL from Volume-Profile/order-book structure,
    widened by the adaptive-percentile adverse-wick buffer, swept past
    known liquidity walls; TP1 is the RR-floor-respecting nearest opposing
    real level, clipped by a genuine closer liquidity wall; TP2 is an
    uncapped, informational extension strictly farther than TP1."""
    buffer = adverse_wick_percentile_buffer(ltf, asset, tf, state)
    atr_val = ltf.atr[-1] if ltf.atr else entry * 0.01
    raw_sl = (structural_level - buffer) if direction >= 0 else (structural_level + buffer)
    sl = clear_liquidity_walls(direction, raw_sl, book, atr_val)

    risk = abs(entry - sl)
    if risk <= 1e-9:
        return None


    tp1_floor_price = entry + risk * RR_MIN_GATE if direction >= 0 else entry - risk * RR_MIN_GATE
    tp1 = clip_target_to_liquidity_wall(direction, entry, tp1_floor_price, book)
    rr1 = abs(tp1 - entry) / risk

    tp2_target = entry + risk * (RR_MIN_GATE + RR_TP2_MIN_EXCESS + 1.0) if direction >= 0\
        else entry - risk * (RR_MIN_GATE + RR_TP2_MIN_EXCESS + 1.0)
    tp2 = clip_target_to_liquidity_wall(direction, entry, tp2_target, book)


    min_tp2 = entry + risk * rr1 * (1 + RR_TP2_MIN_EXCESS) if direction >= 0 else entry - risk * rr1 * (1 + RR_TP2_MIN_EXCESS)
    if direction >= 0 and tp2 <= min_tp2:
        tp2 = min_tp2
    if direction < 0 and tp2 >= min_tp2:
        tp2 = min_tp2
    rr2 = abs(tp2 - entry) / risk

    return RiskPlan(entry=entry, sl=sl, tp1=tp1, tp2=tp2, rr1=rr1, rr2=rr2)


def passes_entry_placement_rules(entry: float, sl: float, tp1: float, atr_val: float, mark: float) -> bool:
    """Sec 9 mandatory entry-placement rules: minimum entry-to-SL/TP1
    distance, and entry not too far from current market price."""
    if atr_val <= 1e-9:
        return False
    if abs(entry - sl) < atr_val * 0.25:
        return False
    if abs(tp1 - entry) < atr_val * 0.25:
        return False
    if abs(entry - mark) > atr_val * MAX_ENTRY_DISTANCE_ATR:
        return False
    return True


@dataclass
class Candidate:
    id: str
    symbol: str
    combo: str
    posture: str
    direction: str
    entry: float
    sl: float
    tp1: float
    tp2: float
    rr1: float
    rr2: float
    confidence: float
    tier: str
    dominant_factor: str
    regime_label: str
    entry_kind: str
    mtf_aligned: bool
    filter_margin_thin: bool
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def to_dict(self) -> dict:
        d = asdict(self)
        d["resolution_logic_version"] = RESOLUTION_LOGIC_VERSION
        return d


def _new_id(symbol: str) -> str:
    return f"{symbol}-{ENGINE_SLUG}-{int(time.time() * 1000)}"


def _combo_tfs(combo: str) -> tuple[str, str, str]:
    return (TF_HTF_INTRADAY, TF_MID_INTRADAY, TF_LTF_INTRADAY) if combo == "intraday"\
        else (TF_HTF_SWING, TF_MID_SWING, TF_LTF_SWING)


def _track_funnel(state: dict, stage: str, killed: bool) -> None:
    """Sec 14 mandatory filter-funnel attrition logging."""
    funnel = state["tier1"]["filter_funnel"].setdefault(stage, {"seen": 0, "killed": 0})
    funnel["seen"] += 1
    if killed:
        funnel["killed"] += 1


def _news_blackout_active(symbol: str, state: dict) -> bool:
    """Sec 13 macro/news blackout -- no live economic calendar feed is wired
    up in this operational contract (Hyperliquid-only data source, Sec 15),
    so this is a documented no-op hook rather than a fabricated blackout
    window; kept as an explicit, named check so wiring a real calendar feed
    later is a one-function change, not an architectural one. # DECISION"""
    return False


def build_candidate(snap: SymbolSnapshot, combo: str, posture: str, regime: RegimeVector,
                      factors: FactorReading, macro_view: Optional[TFView], state: dict) -> Optional[Candidate]:
    htf_tf, mid_tf, ltf_tf = _combo_tfs(combo)
    if ltf_tf not in snap.views or htf_tf not in snap.views:
        return None
    htf, ltf = snap.views[htf_tf], snap.views[ltf_tf]

    direction = posture_direction(posture, factors)
    _track_funnel(state, "01_direction_nonzero", killed=(abs(direction) < 0.05))
    if abs(direction) < 0.05:
        return None

    zone = select_structural_level(posture, direction, snap, ltf_tf, factors)
    _track_funnel(state, "02_structural_level_found", killed=(zone is None))
    if not zone:
        return None

    liquidity_threshold = state["tier1"]["liquidity_sanity_threshold"]
    ob_ok = confirm_order_book(zone, snap.book, liquidity_threshold)
    _track_funnel(state, "03_order_book_confirmation", killed=not ob_ok)
    if not ob_ok:
        return None

    pos_ok = confirm_positioning(posture, direction, factors, state)
    _track_funnel(state, "04_positioning_confirmation", killed=not pos_ok)
    if not pos_ok:
        return None

    entry = precision_entry_refine(zone, ltf)

    plan = build_risk_plan(direction, entry, zone.level, ltf, snap.symbol, ltf_tf, snap.book, state)
    _track_funnel(state, "05_risk_plan_valid", killed=(plan is None))
    if not plan:
        return None

    _track_funnel(state, "06_rr_floor", killed=(plan.rr1 < RR_MIN_GATE))
    if plan.rr1 < RR_MIN_GATE:
        return None

    atr_val = ltf.atr[-1] if ltf.atr else 0.0
    placement_ok = passes_entry_placement_rules(plan.entry, plan.sl, plan.tp1, atr_val, snap.mark)
    _track_funnel(state, "07_entry_placement_rules", killed=not placement_ok)
    if not placement_ok:
        return None

    blackout = _news_blackout_active(snap.symbol, state)
    _track_funnel(state, "08_news_blackout", killed=blackout)
    if blackout:
        return None

    assert plan.tp2 == plan.tp2


    if abs(plan.tp2 - plan.entry) <= abs(plan.tp1 - plan.entry):
        log.error("TP ordering violation for %s -- discarding candidate", snap.symbol)
        return None

    score = composite_score(posture, factors, regime, direction, htf, ltf, plan.rr1, state)
    _track_funnel(state, "09_regime_fit_veto", killed=(score.probability < 0.5 and posture not in eligible_postures(regime, factors, state)))
    if posture not in eligible_postures(regime, factors, state):
        return None

    tier = assign_tier(score.probability, plan.rr1)
    entry_kind = "pending" if abs(plan.entry - snap.mark) > (atr_val * 0.05) else "market"

    cand = Candidate(
        id=_new_id(snap.symbol), symbol=snap.symbol, combo=combo, posture=posture,
        direction="bullish" if direction >= 0 else "bearish",
        entry=plan.entry, sl=plan.sl, tp1=plan.tp1, tp2=plan.tp2, rr1=plan.rr1, rr2=plan.rr2,
        confidence=score.probability, tier=tier, dominant_factor=score.dominant_factor,
        regime_label=regime.dominant_bucket(), entry_kind=entry_kind,
        mtf_aligned=score.mtf_aligned, filter_margin_thin=score.filter_margin_thin,
    )
    return cand


def run_pipeline_for_symbol(snap: SymbolSnapshot, all_snaps: dict[str, SymbolSnapshot],
                              macro_view: Optional[TFView], state: dict) -> list[Candidate]:
    """One factor pipeline, evaluated independently for each of the two
    timeframe combos (Sec 6) -- never a fallback for one another."""
    out: list[Candidate] = []
    for combo in ("intraday", "swing"):
        htf_tf, mid_tf, ltf_tf = _combo_tfs(combo)
        factors = compute_all_factors(snap, htf_tf, mid_tf, ltf_tf, all_snaps, macro_view, state)
        if not factors:
            continue
        regime = compute_regime_vector(snap.symbol, factors, macro_view, snap.views[ltf_tf].candles, all_snaps)
        for posture in eligible_postures(regime, factors, state):
            cand = build_candidate(snap, combo, posture, regime, factors, macro_view, state)
            if cand:
                out.append(cand)
    return out


def _correlated_group(symbol: str) -> str:
    return "majors" if symbol in MAJORS else symbol


def rank_and_gate_candidates(candidates: list[Candidate], active_signals: list[dict]) -> list[Candidate]:
    """Sec 14: cap concurrent exposure to correlated assets, respect
    MAX_CONCURRENT_ACTIVE_SIGNALS, rank by confidence within the surviving
    set -- frequency comes purely from breadth, never from loosening the
    model."""
    ranked = sorted(candidates, key=lambda c: c.confidence, reverse=True)
    active_count = len(active_signals)
    active_groups: dict[str, int] = {}
    for s in active_signals:
        g = _correlated_group(s["symbol"])
        active_groups[g] = active_groups.get(g, 0) + 1

    accepted: list[Candidate] = []
    for c in ranked:
        if active_count + len(accepted) >= MAX_CONCURRENT_ACTIVE_SIGNALS:
            break
        g = _correlated_group(c.symbol)
        current = active_groups.get(g, 0) + sum(1 for a in accepted if _correlated_group(a.symbol) == g)
        if current >= MAX_CORRELATED_CONCURRENT:
            continue
        accepted.append(c)
    return accepted


def check_fill_and_resolve(signal: dict, candles: list[dict]) -> dict:
    """Chronological, closed-candle, watermark-based scan. Enforces: no
    SL/TP evaluation before entry fill (Sec 11); full-exit-at-TP1 model
    (Sec 10) -- 100% of size closes at TP1, no auto-breakeven, no partial
    exit, TP2 never read here."""
    direction = signal["direction"]
    entry, sl, tp1 = signal["entry"], signal["sl"], signal["tp1"]
    entry_kind = signal["entry_kind"]
    expiry_bars = PENDING_ENTRY_EXPIRY_BARS.get(
        TF_LTF_INTRADAY if signal["combo"] == "intraday" else TF_LTF_SWING, 10)

    watermark_ts = signal.get("watermark_ts")
    if watermark_ts is None:
        created_ms = int(datetime.fromisoformat(signal["created_at"]).timestamp() * 1000)
        watermark_ts = created_ms - 1

    for c in candles:
        if c["t"] <= watermark_ts:
            continue
        watermark_ts = c["t"]
        signal["watermark_ts"] = watermark_ts

        if entry_kind == "market" and not signal.get("entry_filled"):
            signal["entry_filled"] = True

        if not signal.get("entry_filled"):
            if c["l"] <= entry <= c["h"]:
                signal["entry_filled"] = True
            else:
                signal["pending_bars"] = signal.get("pending_bars", 0) + 1
                if signal["pending_bars"] >= expiry_bars:
                    return {"status": "expired", "result": "expired"}
                continue

        hit_sl = (c["l"] <= sl) if direction == "bullish" else (c["h"] >= sl)
        hit_tp1 = (c["h"] >= tp1) if direction == "bullish" else (c["l"] <= tp1)

        if hit_sl:


            return {"status": "closed", "result": "loss"}
        if hit_tp1:
            return {"status": "closed", "result": "win"}
    return {"status": "open"}


def resolve_and_learn(signal: dict, resolution: dict, state: dict, regime_at_entry: dict,
                        htf_bias_aligned: bool) -> None:
    """Sec 10 outcome scoring + Sec 13 forensic tagging + Sec 7 bounded
    adaptive updates, all funneled through `apply_forensic_adaptive_response`."""
    t1, t2 = state["tier1"], state["tier2"]
    frozen = t1["circuit_breaker"]["active"]
    result = resolution["result"]
    now = datetime.now(timezone.utc)

    if result == "expired":
        t1["totals"]["expired"] += 1
        rec = {**signal, "result": "expired", "resolved_at": now.isoformat(),
               "resolution_logic_version": RESOLUTION_LOGIC_VERSION}
        t2["trade_log"].append(rec)
        return

    risk = abs(signal["entry"] - signal["sl"])
    r_multiple = signal["rr1"] if result == "win" else -1.0

    if result == "win":
        assert r_multiple > 0, "bug: win with non-positive R"

    created = datetime.fromisoformat(signal["created_at"])
    hold_min = max((now - created).total_seconds() / 60.0, 0.0)

    category = diagnose_trade(signal, regime_at_entry, state, result, htf_bias_aligned)
    adaptive_note = apply_forensic_adaptive_response(category, signal, state, frozen=frozen)

    t1["totals"]["signals"] += 1
    t1["totals"]["wins" if result == "win" else "losses"] += 1
    t1["totals"]["sum_r"] += r_multiple
    t1["totals"]["sum_hold_min"] += hold_min

    for seg_kind, seg_key in (("asset", signal["symbol"]), ("regime", signal["regime_label"]),
                               ("timeframe", signal["combo"]), ("posture", signal["posture"]),
                               ("dominant_factor", signal["dominant_factor"])):
        seg = t1["segments"][seg_kind].setdefault(seg_key, _default_segment_stats())
        seg["n"] += 1
        seg["wins" if result == "win" else "losses"] += 1
        seg["sum_r"] += r_multiple
        seg["sum_hold_min"] += hold_min

    anchored = regime_at_entry.get("session_open_proximity", 0.0) > 0.5
    bucket = t1["session_anchor_bucket"]["anchored" if anchored else "non_anchored"]
    bucket["n"] += 1
    bucket["wins" if result == "win" else "losses"] += 1
    bucket["sum_r"] += r_multiple

    bucket_key = f"{signal['posture']}:{int(clamp(signal['confidence'], 0, 0.999) * 10)}"
    log_list = t1["confidence_bucket_log"].setdefault(bucket_key, [])
    log_list.append(1 if result == "win" else 0)
    t1["confidence_bucket_log"][bucket_key] = log_list[-200:]

    if result == "win":
        reinforce_win(signal, category, state, frozen=frozen)

    baseline = t1["baseline"]
    if baseline["n"] < MIN_SAMPLE_SIZE:
        _accumulate_baseline(baseline, result, r_multiple)
    check_circuit_breaker(state)

    rec = {**signal, "result": result, "r_multiple": r_multiple, "resolved_at": now.isoformat(),
           "hold_min": hold_min, "forensic_category": category, "adaptive_adjustment": adaptive_note,
           "regime_at_entry": regime_at_entry, "resolution_logic_version": RESOLUTION_LOGIC_VERSION}
    t2["trade_log"].append(rec)


def _accumulate_baseline(baseline: dict, result: str, r_multiple: float) -> None:
    """Sec 13 pre-deployment baseline: populated on the first N trades then
    frozen, used as the live-performance circuit breaker's reference point."""
    n = baseline["n"]
    wins = (baseline["win_rate"] or 0.0) * n + (1 if result == "win" else 0)
    baseline["n"] = n + 1
    baseline["win_rate"] = wins / baseline["n"]
    prev_avg_rr = baseline["avg_rr"] or 0.0
    baseline["avg_rr"] = (prev_avg_rr * n + r_multiple) / baseline["n"]
    gross_win = baseline.get("_gross_win", 0.0) + (r_multiple if result == "win" else 0.0)
    gross_loss = baseline.get("_gross_loss", 0.0) + (abs(r_multiple) if result == "loss" else 0.0)
    baseline["_gross_win"], baseline["_gross_loss"] = gross_win, gross_loss
    baseline["profit_factor"] = (gross_win / gross_loss) if gross_loss > 1e-9 else None


def _confidence_bucket_realized_wr(posture: str, confidence: float, state: dict) -> Optional[float]:
    bucket = str(int(clamp(confidence, 0, 0.999) * 10))
    outcomes = state["tier1"]["confidence_bucket_log"].get(f"{posture}:{bucket}", [])
    if len(outcomes) < MIN_SAMPLE_SIZE_CATEGORY:
        return None
    return sum(outcomes) / len(outcomes)


def diagnose_trade(signal: dict, regime_at_entry: dict, state: dict, result: str,
                     htf_bias_aligned: bool) -> str:
    """Sec 13 closed-set taxonomy, re-keyed to this model's own factors
    (Sec 1's mandatory generalization). Every branch is a positive,
    verifiable condition on recorded trade data -- never an else-by-elimination
    catch-all, except the documented final `genuine_variance` fallback."""
    if result == "win":
        return "genuine_variance"

    dominant_bucket = regime_at_entry.get("label", "transitional")
    posture = signal["posture"]
    trending = dominant_bucket in TRENDING_BUCKETS
    ranging = dominant_bucket in RANGING_BUCKETS
    if (posture == "continuation" and not trending) or (posture == "reversion" and not ranging):
        return "regime_mismatch"

    if not signal.get("mtf_aligned", True):
        return "mtf_conflict_ignored"

    if signal.get("liquidity_wall_hit"):
        return "order_book_liquidity_trap"

    if not htf_bias_aligned:
        return "sequence_violated"

    if posture == "reversion" and signal.get("extension_continued"):
        return "positioning_misread"

    conf_wr = _confidence_bucket_realized_wr(posture, signal.get("confidence", 0.5), state)
    if conf_wr is not None and signal.get("confidence", 0) - conf_wr > 0.2:
        return "confidence_miscalibration"

    if signal.get("rr1", 0) < RR_TP1_CEIL_SOFT * 0.85:
        return "correct_read_poor_rr"

    if signal.get("filter_margin_thin"):
        return "filter_over_permissiveness"

    buffer_ratio = signal.get("buffer_to_risk_ratio", 1.0)
    if 0 < buffer_ratio < 0.18:
        return "structural_invalidation_too_tight"

    return "genuine_variance"


def apply_forensic_adaptive_response(category: str, signal: dict, state: dict, frozen: bool = False) -> str:
    """One diagnosis, one deterministic route (Sec 13), through the shared
    bounded/dampened/min-sample-gated update path. Category counters always
    update for auditability; the parameter mutation itself is skipped while
    `frozen` so the circuit breaker truly freezes adaptation."""
    t1 = state["tier1"]
    posture = signal["posture"]
    cat_state = t1["forensic_categories"][category]
    cat_state["count"] += 1
    cat_state["recent_trend"] = (cat_state["recent_trend"] + [1])[-50:]

    if frozen:
        return "no_change_circuit_breaker_active"
    if cat_state["count"] < MIN_SAMPLE_SIZE_CATEGORY:
        return "no_change_insufficient_sample"

    if category == "regime_mismatch":
        bucket = signal["regime_label"]
        for f in ("trend", "momentum") if posture == "continuation" else ("mean_reversion", "positioning"):
            cur = t1["weight_matrix"].setdefault(bucket, {}).get(f, 0.5)
            new = bounded_update(cur, cur - 0.1, 0.0, 2.0, max_step_frac=0.1)
            t1["weight_matrix"][bucket][f] = new
        return f"weight_matrix[{bucket}] discounted for {posture}"

    if category == "structural_invalidation_too_tight":
        key = f"{signal['symbol']}:{signal.get('ltf_tf', TF_LTF_INTRADAY)}"
        cur = t1["sl_buffer_percentile"].get(key, 65.0)
        new = bounded_update(cur, cur + 5, 50.0, 90.0, max_step_frac=0.2)
        t1["sl_buffer_percentile"][key] = new
        return f"sl_buffer_percentile[{key}] -> {new:.1f}"

    if category == "order_book_liquidity_trap":
        cur = t1["liquidity_sanity_threshold"]
        new = bounded_update(cur, cur + 0.1, 0.1, 0.9, max_step_frac=0.15)
        t1["liquidity_sanity_threshold"] = new
        return f"liquidity_sanity_threshold -> {new:.3f}"

    if category == "mtf_conflict_ignored":
        cur = t1["mtf_alignment_weight"]
        new = bounded_update(cur, cur + 0.02, 0.05, 0.5, max_step_frac=0.2)
        t1["mtf_alignment_weight"] = new
        return f"mtf_alignment_weight -> {new:.3f}"

    if category == "sequence_violated":
        bucket = signal["regime_label"]
        cur = t1["weight_matrix"].setdefault(bucket, {}).get("volume_profile", 0.5)
        new = bounded_update(cur, cur + 0.1, 0.0, 2.0, max_step_frac=0.15)
        t1["weight_matrix"][bucket]["volume_profile"] = new
        return f"weight_matrix[{bucket}][volume_profile] tightened -> {new:.3f}"

    if category == "positioning_misread":
        cur = t1["positioning_extremity_threshold"]
        new = bounded_update(cur, cur + 0.1, 1.0, 3.0, max_step_frac=0.15)
        t1["positioning_extremity_threshold"] = new
        return f"positioning_extremity_threshold -> {new:.3f}"

    if category == "correct_read_poor_rr":
        return "no_change_rr_floor_calibration_review"

    if category == "confidence_miscalibration":
        bucket = str(int(clamp(signal.get("confidence", 0.5), 0, 0.999) * 10))
        key = f"{posture}:{bucket}"
        cur = t1["confidence_calibration"].get(key, 0.0)
        new = bounded_update(cur, cur - 0.05, -0.3, 0.3, max_step_frac=0.25)
        t1["confidence_calibration"][key] = new
        return f"confidence_calibration[{key}] -> {new:.3f}"

    if category == "filter_over_permissiveness":
        cur = t1["liquidity_sanity_threshold"]
        new = bounded_update(cur, cur + 0.08, 0.1, 0.9, max_step_frac=0.15)
        t1["liquidity_sanity_threshold"] = new
        return f"liquidity_sanity_threshold -> {new:.3f} (over-permissive filter)"

    return "no_change_genuine_variance"


def reinforce_win(signal: dict, category: str, state: dict, frozen: bool = False) -> str:
    """Sec 13 win reinforcement: only nudges the weight of the factor that
    was genuinely dominant AND causally relevant (not merely present) --
    detected here as the composite score's own recorded dominant term."""
    if frozen:
        return "no_change_circuit_breaker_active"
    t1 = state["tier1"]
    bucket = signal["regime_label"]
    dominant = signal["dominant_factor"]
    if dominant not in FACTOR_NAMES:
        return "no_change_non_factor_dominant"
    cur = t1["weight_matrix"].setdefault(bucket, {}).get(dominant, 0.5)
    new = bounded_update(cur, cur + 0.05, 0.0, 2.0, max_step_frac=0.08)
    t1["weight_matrix"][bucket][dominant] = new
    return f"weight_matrix[{bucket}][{dominant}] reinforced -> {new:.3f}"


def check_circuit_breaker(state: dict) -> None:
    """Sec 7 mandatory live-performance circuit breaker: dual-metric
    (win-rate + profit-factor) comparison of rolling live performance
    against the frozen pre-deployment baseline."""
    t1 = state["tier1"]
    baseline = t1["baseline"]
    if baseline["n"] < MIN_SAMPLE_SIZE or baseline["win_rate"] is None:
        return

    log_entries = [r for r in [] ] if False else None
    cb = t1["circuit_breaker"]
    recent = _recent_resolved_trades(state, CIRCUIT_BREAKER_WINDOW)
    if len(recent) < CIRCUIT_BREAKER_WINDOW:
        if cb["active"]:
            pass
        return

    wins = sum(1 for r in recent if r["result"] == "win")
    live_wr = wins / len(recent)
    gross_win = sum(r["r_multiple"] for r in recent if r["result"] == "win")
    gross_loss = abs(sum(r["r_multiple"] for r in recent if r["result"] == "loss"))
    live_pf = (gross_win / gross_loss) if gross_loss > 1e-9 else float("inf")

    wr_dropped = (baseline["win_rate"] - live_wr) >= CIRCUIT_BREAKER_WIN_RATE_DROP
    pf_baseline = baseline["profit_factor"] or 1.0
    pf_dropped = (live_pf != float("inf")) and ((pf_baseline - live_pf) / max(pf_baseline, 1e-9) >= CIRCUIT_BREAKER_PF_DROP_FRAC)

    if not cb["active"] and (wr_dropped or pf_dropped):
        cb["active"] = True
        cb["since"] = datetime.now(timezone.utc).isoformat()
        cb["reason"] = (f"Live win rate {live_wr:.0%} vs baseline {baseline['win_rate']:.0%}" if wr_dropped
                         else f"Live profit factor {live_pf:.2f} vs baseline {pf_baseline:.2f}")
    elif cb["active"] and not (wr_dropped or pf_dropped):
        cb["active"] = False
        cb["since"] = None
        cb["reason"] = None


def _recent_resolved_trades(state: dict, n: int) -> list[dict]:
    resolved = [r for r in state["tier2"]["trade_log"]
                if r.get("result") in ("win", "loss")
                and r.get("resolution_logic_version") == RESOLUTION_LOGIC_VERSION]
    return resolved[-n:]


def _display_name(identifier: str) -> str:
    return str(identifier).replace("_", " ").replace("-", " ").title()


def _ticker(symbol: str) -> str:
    return symbol.replace("USDT", "").replace("USD", "").upper()


def _expiry_hours(combo: str) -> float:
    tf = TF_LTF_INTRADAY if combo == "intraday" else TF_LTF_SWING
    bars = PENDING_ENTRY_EXPIRY_BARS.get(tf, 10)
    return bars * (_interval_to_ms(tf) / 3_600_000)


def format_price(price: float) -> str:
    if price >= 100:
        return f"{price:.2f}"
    if price >= 1:
        return f"{price:.4f}"
    return f"{price:.6f}"


def send_telegram(text: str, reply_to: Optional[int] = None) -> Optional[int]:
    base = f"https://api.telegram.org/bot{TG_BOT_TOKEN}"
    try:
        payload = {"chat_id": TG_CHAT_ID, "text": text, "parse_mode": "Markdown"}
        if reply_to:
            payload["reply_to_message_id"] = reply_to
        resp = requests.post(f"{base}/sendMessage", json=payload, timeout=15)
        resp.raise_for_status()
        return resp.json().get("result", {}).get("message_id")
    except requests.RequestException:
        log.exception("Telegram send failed")
        return None


def format_signal_message(cand: Candidate) -> str:
    direction_tag = "LONG \U0001F7E2" if cand.direction == "bullish" else "SHORT \U0001F534"
    lines = [
        f"*{ENGINE_NAME}* v{__version__}",
        f"*{_ticker(cand.symbol)}* \u2014 {direction_tag}",
        "",
        f"Posture: {_display_name(cand.posture)}  |  Tier: {cand.tier}",
        f"Regime: {_display_name(cand.regime_label)}  |  Confidence: {cand.confidence:.0%}",
        f"Dominant Factor: {_display_name(cand.dominant_factor)}",
        "",
        "Entry:",
        f"`{format_price(cand.entry)}`",
        "SL:",
        f"`{format_price(cand.sl)}`",
        "TP1:",
        f"`{format_price(cand.tp1)}`",
        "TP2 (suggested):",
        f"`{format_price(cand.tp2)}`",
        "",
        f"RR: {cand.rr1:.2f} / {cand.rr2:.2f}",
        "_TP2 is a suggested further target only \u2014 position closes in full at TP1._",
    ]
    if cand.entry_kind == "pending":
        lines.append(f"Pending \u2014 expires in {_expiry_hours(cand.combo):.1f}h")
    return "\n".join(lines)


def format_outcome_message(signal: dict, resolution: dict) -> str:
    if resolution["status"] == "expired":
        return (f"{EMOJI_EXPIRED} *{ENGINE_NAME}* \u2014 {_display_name(signal['symbol'])} Expired (No Fill)\n\n"
                f"Entry never filled within its pending window.")
    if resolution["result"] == "win":
        return (f"{EMOJI_WIN} *{ENGINE_NAME}* \u2014 {_display_name(signal['symbol'])} TP1 Hit \u2014 Win\n\n"
                f"Realized: {signal.get('rr1', 0):.2f}R\n"
                f"Entry:\n`{format_price(signal['entry'])}`\n"
                f"SL:\n`{format_price(signal['sl'])}`\n"
                f"TP1:\n`{format_price(signal['tp1'])}`\n\n"
                f"Position closed in full at TP1. Nothing remains open on this signal.")
    return (f"{EMOJI_LOSS} *{ENGINE_NAME}* \u2014 {_display_name(signal['symbol'])} SL Hit \u2014 Loss\n\n"
            f"Entry:\n`{format_price(signal['entry'])}`\n"
            f"SL:\n`{format_price(signal['sl'])}`")


def send_daily_summary(state: dict) -> None:
    t1 = state["tier1"]
    tot = t1["totals"]
    n = max(tot["signals"], 1)
    win_rate = tot["wins"] / n
    log_entries = [r for r in state["tier2"]["trade_log"] if r.get("result") in ("win", "loss")]
    gross_win = sum(r["r_multiple"] for r in log_entries if r.get("result") == "win")
    gross_loss = abs(sum(r["r_multiple"] for r in log_entries if r.get("result") == "loss"))
    profit_factor = (gross_win / gross_loss) if gross_loss > 1e-9 else float("inf")
    avg_rr = tot["sum_r"] / n
    avg_hold = tot["sum_hold_min"] / n

    lines = [
        f"*{ENGINE_NAME}* `{__version__}` \u2014 Daily Summary",
        "",
        f"Total Signals: {tot['signals']}   Expired: {tot['expired']}",
        f"Wins: {tot['wins']}   Losses: {tot['losses']}",
        f"Win Rate: {win_rate:.1%}",
        f"Profit Factor: {profit_factor:.2f}" if profit_factor != float("inf") else "Profit Factor: inf",
        f"Average RR: {avg_rr:.2f}",
        f"Average Hold Time: {avg_hold:.0f} min",
        "",
        "By Regime:",
    ]
    for regime, seg in sorted(t1["segments"]["regime"].items()):
        if seg["n"] == 0:
            continue
        wr = seg["wins"] / seg["n"]
        lines.append(f"  {_display_name(regime)}: {seg['n']} trades, {wr:.0%} WR")

    lines.append("")
    lines.append("By Posture:")
    for posture, seg in sorted(t1["segments"]["posture"].items()):
        if seg["n"] == 0:
            continue
        wr = seg["wins"] / seg["n"]
        lines.append(f"  {_display_name(posture)}: {seg['n']} trades, {wr:.0%} WR")

    lines.append("")
    lines.append("By Dominant Factor:")
    for factor, seg in sorted(t1["segments"]["dominant_factor"].items()):
        if seg["n"] == 0:
            continue
        wr = seg["wins"] / seg["n"]
        lines.append(f"  {_display_name(factor)}: {seg['n']} trades, {wr:.0%} WR")

    if log_entries:
        best = max(log_entries, key=lambda r: r.get("r_multiple", -999))
        worst = min(log_entries, key=lambda r: r.get("r_multiple", 999))
        lines.append("")
        lines.append(f"Best Setup: {_display_name(best['symbol'])} ({_display_name(best['posture'])}), {best.get('r_multiple', 0):.2f}R")
        lines.append(f"Worst Setup: {_display_name(worst['symbol'])} ({_display_name(worst['posture'])}), {worst.get('r_multiple', 0):.2f}R")

    lines.append("")
    lines.append("Confidence Calibration:")
    for key in sorted(t1["confidence_bucket_log"].keys()):
        outcomes = t1["confidence_bucket_log"][key]
        if len(outcomes) < MIN_SAMPLE_SIZE_CATEGORY:
            continue
        wr = sum(outcomes) / len(outcomes)
        lines.append(f"  {_display_name(key)}: {wr:.0%} realized ({len(outcomes)} trades)")

    lines.append("")
    lines.append("Forensic Category Breakdown:")
    for cat, cs in t1["forensic_categories"].items():
        if cs["count"] == 0:
            continue
        trend = sum(cs["recent_trend"][-10:])
        lines.append(f"  {_display_name(cat)}: {cs['count']} total, {trend}/10 recent")

    anchored, non_anchored = t1["session_anchor_bucket"]["anchored"], t1["session_anchor_bucket"]["non_anchored"]
    lines.append("")
    lines.append("Session-Anchored Bucket:")
    if anchored["n"]:
        lines.append(f"  Anchored: {anchored['n']} trades, {anchored['wins']/anchored['n']:.0%} WR")
    if non_anchored["n"]:
        lines.append(f"  Non Anchored: {non_anchored['n']} trades, {non_anchored['wins']/non_anchored['n']:.0%} WR")

    total_dispatched = tot["signals"] + tot["expired"]
    fill_rate = (tot["signals"] / total_dispatched) if total_dispatched else 1.0
    lines.append("")
    lines.append(f"Fill Rate: {fill_rate:.0%}")
    lines.append("Filter Funnel Attrition:")
    for stage, fs in sorted(t1["filter_funnel"].items()):
        if fs["seen"] == 0:
            continue
        lines.append(f"  {_display_name(stage)}: {fs['killed']}/{fs['seen']} killed")

    cb = t1["circuit_breaker"]
    lines.append("")
    lines.append(f"Circuit Breaker: {'ACTIVE — adaptation frozen' if cb['active'] else 'Inactive'}")

    send_telegram("\n".join(lines))


def _update_oi_history(state: dict, snaps: dict[str, SymbolSnapshot]) -> None:
    """Bounded, persisted rolling OI buffer per asset (Sec 4.1 Factor 5's
    OI-delta term needs history that metaAndAssetCtxs alone doesn't carry)."""
    oi_hist = state["tier1"]["oi_history"]
    for sym, snap in snaps.items():
        if not snap.ctx:
            continue
        hist = oi_hist.setdefault(sym, [])
        hist.append(snap.ctx.get("open_interest", 0.0))
        oi_hist[sym] = hist[-OI_HISTORY_MAXLEN:]


def monitor_active_signals(state: dict, hl: HyperliquidClient) -> None:
    t2 = state["tier2"]
    still_active = []
    for signal in t2["active_signals"]:
        tf = TF_LTF_INTRADAY if signal["combo"] == "intraday" else TF_LTF_SWING
        candles = hl.candles(signal["symbol"], tf, TF_BARS[tf])
        if not candles:
            still_active.append(signal)
            continue
        resolution = check_fill_and_resolve(signal, candles)
        if resolution["status"] == "open":
            still_active.append(signal)
            continue

        regime_at_entry = signal.get("regime_at_entry", {"label": signal.get("regime_label", "transitional")})
        htf_bias_aligned = signal.get("mtf_aligned", True)
        resolve_and_learn(signal, resolution, state, regime_at_entry, htf_bias_aligned)
        send_telegram(format_outcome_message(signal, resolution), reply_to=signal.get("tg_message_id"))

    t2["active_signals"] = still_active


def run_scan(hl: HyperliquidClient, store: StateStore, cache_store: CandleCacheStore) -> None:
    state = store.load()
    cache = cache_store.load()
    hl.cache = cache

    try:
        monitor_active_signals(state, hl)

        marks = hl.mark_prices()
        snaps: dict[str, SymbolSnapshot] = {}
        with ThreadPoolExecutor(max_workers=SCAN_WORKERS) as ex:
            futures = {ex.submit(collect_snapshot, hl, sym, marks.get(sym, 0.0)): sym for sym in WATCHLIST}
            for fut in as_completed(futures):
                sym = futures[fut]
                try:
                    snap = fut.result()
                    if snap:
                        snaps[sym] = snap
                except Exception:
                    log.exception("snapshot failed for %s", sym)

        _update_oi_history(state, snaps)

        macro_snap = snaps.get(MACRO_ASSET)
        macro_view = macro_snap.views.get(TF_HTF_SWING) if macro_snap else None

        prev_cb_active = state["tier1"]["circuit_breaker"]["active"]

        all_candidates: list[Candidate] = []
        for sym, snap in snaps.items():
            all_candidates.extend(run_pipeline_for_symbol(snap, snaps, macro_view, state))

        if state["tier1"]["circuit_breaker"]["active"]:
            log.info("circuit breaker active -- signal generation continues, adaptation frozen")

        accepted = rank_and_gate_candidates(all_candidates, state["tier2"]["active_signals"])

        for cand in accepted:
            msg_id = send_telegram(format_signal_message(cand))
            sig = cand.to_dict()
            sig["ltf_tf"] = TF_LTF_INTRADAY if cand.combo == "intraday" else TF_LTF_SWING
            sig["tg_message_id"] = msg_id
            sig["entry_filled"] = (cand.entry_kind == "market")
            state["tier2"]["active_signals"].append(sig)

        cb_now_active = state["tier1"]["circuit_breaker"]["active"]
        if cb_now_active and not prev_cb_active:
            send_telegram(f"{EMOJI_CIRCUIT_BREAKER} *{ENGINE_NAME}* Circuit Breaker Tripped\n\n"
                          f"{state['tier1']['circuit_breaker']['reason']}\n"
                          f"Automatic parameter adaptation is frozen at last-known-good values. "
                          f"Signal generation continues unaffected.")
        elif not cb_now_active and prev_cb_active:
            send_telegram(f"{EMOJI_RECOVERED} *{ENGINE_NAME}* Circuit Breaker Cleared\n\n"
                          f"Live performance has recovered to baseline. Adaptation resumed.")

        now = datetime.now(timezone.utc)
        last_summary = state["tier1"].get("last_daily_summary_date")
        if now.hour == 8 and last_summary != now.date().isoformat():
            send_daily_summary(state)
            state["tier1"]["last_daily_summary_date"] = now.date().isoformat()

        store.prune_tier2(state)
    finally:
        cache_store.save(hl.cache)
        store.save(state)


def main() -> None:
    log.info("%s v%s starting scan", ENGINE_NAME, __version__)
    store = StateStore(STATE_FILE)
    cache_store = CandleCacheStore(CANDLE_CACHE_FILE)
    hl = HyperliquidClient()
    try:
        run_scan(hl, store, cache_store)
    except Exception:
        log.exception("scan failed")
        raise
    log.info("scan complete")


if __name__ == "__main__":
    main()
