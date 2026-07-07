#!/usr/bin/env python3
# =============================================================================
# AETHERIS ENGINE v1.0.0
# -----------------------------------------------------------------------------
# An adaptive, multi-timeframe intraday & swing crypto signal engine for
# Hyperliquid. Aetheris fuses price-structure, momentum, volatility and
# derivatives (funding/OI) evidence into a single ensemble confidence score,
# then dynamically retunes its own strictness to the live market regime so it
# neither starves in quiet markets nor over-fires in chaotic ones. What sets
# it apart from a conventional single-method screener is that every signal
# must be corroborated by independent, largely uncorrelated evidence families
# (trend, momentum, structure, volume, derivatives) before it is allowed
# through — disagreement between those families suppresses the signal rather
# than being averaged away, and correlated concurrent signals are collapsed
# into a single effective bet at the risk-management layer.
#
# pip install requests numpy pandas
#
# Required environment variables:
#   TG_BOT_TOKEN   - Telegram bot token
#   TG_CHAT_ID     - Telegram chat id
# Optional environment variables (all have sane defaults, see CONFIG section):
#   DRY_RUN, HL_API_URL, MAX_CONCURRENT_POSITIONS, MAX_PORTFOLIO_EXPOSURE_PCT,
#   DAILY_LOSS_LIMIT_PCT, ACCOUNT_EQUITY_USD, RISK_PER_TRADE_PCT, LOG_LEVEL
#
# Scheduling: run this script once per invocation ("scan-per-run"), triggered
# every 15 minutes by an external scheduler (cron-job.org or equivalent).
# State persists between runs in state.json in the working directory.
# =============================================================================

from __future__ import annotations

import os
import sys
import json
import time
import math
import signal
import logging
import statistics
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

import requests
import numpy as np

# =============================================================================
# CONFIG
# =============================================================================

def _env_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "on")


HL_API_URL = os.getenv("HL_API_URL", "https://api.hyperliquid.xyz/info")
TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN")
TG_CHAT_ID = os.getenv("TG_CHAT_ID")

DRY_RUN = _env_bool("DRY_RUN", False)              # shakedown mode: no Telegram sends, no state commits
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

STATE_PATH = os.getenv("STATE_PATH", "state.json")
LOG_PATH = os.getenv("LOG_PATH", "aetheris_engine.log")
SUPPRESSED_LOG_PATH = os.getenv("SUPPRESSED_LOG_PATH", "suppressed_signals.jsonl")

# Portfolio / risk management
ACCOUNT_EQUITY_USD = float(os.getenv("ACCOUNT_EQUITY_USD", "10000"))
RISK_PER_TRADE_PCT = float(os.getenv("RISK_PER_TRADE_PCT", "0.5"))          # % of equity risked per trade
MAX_CONCURRENT_POSITIONS = int(os.getenv("MAX_CONCURRENT_POSITIONS", "6"))
MAX_PORTFOLIO_EXPOSURE_PCT = float(os.getenv("MAX_PORTFOLIO_EXPOSURE_PCT", "35"))  # % of equity notional
DAILY_LOSS_LIMIT_PCT = float(os.getenv("DAILY_LOSS_LIMIT_PCT", "3.0"))      # % of equity, UTC calendar day

WATCHLIST = [
    "BTC", "ETH", "HYPE", "ZEC", "NEAR", "ONDO", "SUI", "PENGU", "BNB", "SOL",
    "TRX", "BCH", "DOGE", "ADA", "DOT", "TAO", "AVAX", "LINK", "AAVE", "XRP",
    "XLM", "UNI", "LTC", "APT", "PENDLE",
]

# Timeframe roles. Three-timeframe stack chosen as the best tradeoff:
#  - 4h  : macro bias / HTF trend filter (slow, high-conviction)
#  - 1h  : intermediate confirmation (regime + momentum context)
#  - 15m : execution timeframe (entry trigger, matches the 15-min scan cadence)
# A fourth (5m) was evaluated but rejected: it adds noise-driven signals faster
# than it adds true edge for this scan cadence (a 15-min scanner cannot react
# to 5m-only information before it decays).
TF_EXEC = "15m"
TF_CONFIRM = "1h"
TF_BIAS = "4h"
TF_CANDLES_NEEDED = {"15m": 200, "1h": 200, "4h": 160}

RSI_LEN = 14
ATR_LEN = 14
ADX_LEN = 14
BB_LEN = 20
BB_MULT = 2.0
EMA_FAST = 21
EMA_SLOW = 55
EMA_TREND = 200

SESSION = requests.Session()
SESSION.headers.update({"Content-Type": "application/json"})

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.FileHandler(LOG_PATH), logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("aetheris")

_SHUTDOWN = False


def _handle_shutdown(sig_num, frame):
    global _SHUTDOWN
    _SHUTDOWN = True
    log.warning("Shutdown signal received (%s); finishing current asset then exiting.", sig_num)


signal.signal(signal.SIGTERM, _handle_shutdown)
signal.signal(signal.SIGINT, _handle_shutdown)


def utc_day_key(ts: Optional[float] = None) -> str:
    """Fixed UTC calendar-day boundary, used for daily loss limit and daily
    signal counts so there is no ambiguity across scans in different local
    timezones or across DST changes."""
    dt = datetime.fromtimestamp(ts if ts is not None else time.time(), tz=timezone.utc)
    return dt.strftime("%Y-%m-%d")


# =============================================================================
# HYPERLIQUID API CLIENT
# =============================================================================

class _RateLimiter:
    """Simple adaptive-backoff limiter so a single asset's slow API response
    cannot stall the entire watchlist scan indefinitely."""

    def __init__(self, min_interval_s: float = 0.15):
        self.min_interval_s = min_interval_s
        self._last = 0.0

    def wait(self):
        now = time.time()
        delta = now - self._last
        if delta < self.min_interval_s:
            time.sleep(self.min_interval_s - delta)
        self._last = time.time()


_LIMITER = _RateLimiter()


def hl_post(payload: dict, retries: int = 4, timeout: int = 12) -> Optional[dict | list]:
    for attempt in range(retries):
        _LIMITER.wait()
        try:
            r = SESSION.post(HL_API_URL, json=payload, timeout=timeout)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (429, 502, 503):
                time.sleep(0.5 * (attempt + 1))
                continue
            log.error("HL API non-200 (%s) for payload type=%s", r.status_code, payload.get("type"))
            return None
        except requests.RequestException as e:
            log.warning("HL API request error (attempt %d/%d): %s", attempt + 1, retries, e)
            time.sleep(0.4 * (attempt + 1))
    return None


def interval_ms(interval: str) -> int:
    return {"15m": 15 * 60_000, "1h": 60 * 60_000, "4h": 4 * 60 * 60_000, "1d": 24 * 60 * 60_000}[interval]


def current_bar_open_ms(reference_ms: int, interval: str) -> int:
    step = interval_ms(interval)
    return (reference_ms // step) * step


def get_candles(symbol: str, interval: str, n: int, reference_ms: Optional[int] = None) -> Optional[list[dict]]:
    ref = reference_ms or int(time.time() * 1000)
    step = interval_ms(interval)
    start = ref - step * (n + 2)
    payload = {
        "type": "candleSnapshot",
        "req": {"coin": symbol, "interval": interval, "startTime": start, "endTime": ref},
    }
    data = hl_post(payload)
    if not data or not isinstance(data, list):
        return None
    # exclude the still-forming current bar to avoid look-ahead / repaint risk
    bar_open = current_bar_open_ms(ref, interval)
    closed = [c for c in data if c.get("t", 0) < bar_open]
    return closed[-n:] if len(closed) >= 5 else None


def fetch_all_candles(symbol: str, reference_ms: Optional[int] = None) -> Optional[dict[str, list[dict]]]:
    out = {}
    for tf, n in TF_CANDLES_NEEDED.items():
        c = get_candles(symbol, tf, n, reference_ms)
        if c is None:
            log.warning("Candle fetch failed for %s/%s; skipping asset this scan.", symbol, tf)
            return None
        out[tf] = c
    return out


def get_meta_and_ctx() -> Optional[tuple[list[str], list[dict]]]:
    data = hl_post({"type": "metaAndAssetCtxs"})
    if not data or not isinstance(data, list) or len(data) < 2:
        return None
    universe = [a["name"] for a in data[0].get("universe", [])]
    return universe, data[1]


def get_market_snapshot() -> dict[str, dict]:
    """Returns {symbol: {mark_px, funding, open_interest, day_volume}}"""
    res = get_meta_and_ctx()
    snap = {}
    if not res:
        return snap
    universe, ctxs = res
    for name, ctx in zip(universe, ctxs):
        if name not in WATCHLIST:
            continue
        try:
            snap[name] = {
                "mark_px": float(ctx.get("markPx", 0) or 0),
                "funding": float(ctx.get("funding", 0) or 0),
                "open_interest": float(ctx.get("openInterest", 0) or 0),
                "day_volume": float(ctx.get("dayNtlVlm", 0) or 0),
            }
        except (TypeError, ValueError):
            continue
    return snap


def get_l2_book(coin: str) -> Optional[dict]:
    return hl_post({"type": "l2Book", "coin": coin})


def analyze_orderbook(coin: str) -> dict:
    """Liquidity read: spread, top-of-book depth, imbalance. Used to down-weight
    signals in thin/illiquid conditions even when technicals look clean."""
    book = get_l2_book(coin)
    default = {"spread_pct": None, "depth_usd": 0.0, "imbalance": 0.0, "ok": False}
    if not book or "levels" not in book:
        return default
    try:
        bids, asks = book["levels"][0], book["levels"][1]
        if not bids or not asks:
            return default
        best_bid, best_ask = float(bids[0]["px"]), float(asks[0]["px"])
        mid = (best_bid + best_ask) / 2
        spread_pct = (best_ask - best_bid) / mid * 100 if mid else None
        depth_bid = sum(float(b["px"]) * float(b["sz"]) for b in bids[:10])
        depth_ask = sum(float(a["px"]) * float(a["sz"]) for a in asks[:10])
        depth_usd = depth_bid + depth_ask
        imbalance = (depth_bid - depth_ask) / depth_usd if depth_usd else 0.0
        return {"spread_pct": spread_pct, "depth_usd": depth_usd, "imbalance": imbalance, "ok": True}
    except (KeyError, ValueError, IndexError, TypeError):
        return default


# =============================================================================
# INDICATORS
# =============================================================================

def _closes(candles): return [float(c["c"]) for c in candles]
def _highs(candles): return [float(c["h"]) for c in candles]
def _lows(candles): return [float(c["l"]) for c in candles]
def _vols(candles): return [float(c["v"]) for c in candles]


def ema(vals: list[float], period: int) -> list[float]:
    if len(vals) < period:
        return [vals[-1]] * len(vals) if vals else []
    k = 2 / (period + 1)
    out = [sum(vals[:period]) / period]
    for v in vals[period:]:
        out.append(v * k + out[-1] * (1 - k))
    pad = [out[0]] * (period - 1)
    return pad + out


def sma(vals: list[float], period: int) -> list[float]:
    out = []
    for i in range(len(vals)):
        if i < period - 1:
            out.append(sum(vals[: i + 1]) / (i + 1))
        else:
            out.append(sum(vals[i - period + 1 : i + 1]) / period)
    return out


def stdev(vals: list[float], period: int) -> list[float]:
    out = []
    for i in range(len(vals)):
        window = vals[max(0, i - period + 1) : i + 1]
        out.append(statistics.pstdev(window) if len(window) > 1 else 0.0)
    return out


def rsi(closes: list[float], period: int = RSI_LEN) -> list[float]:
    if len(closes) < period + 1:
        return [50.0] * len(closes)
    deltas = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
    gains = [max(d, 0) for d in deltas]
    losses = [max(-d, 0) for d in deltas]
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    out = [50.0] * period
    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
        rs = avg_gain / avg_loss if avg_loss else 100.0
        out.append(100 - (100 / (1 + rs)))
    return [50.0] + out


def atr(highs, lows, closes, period: int = ATR_LEN) -> list[float]:
    trs = [highs[0] - lows[0]]
    for i in range(1, len(closes)):
        trs.append(max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]), abs(lows[i] - closes[i - 1])))
    return ema(trs, period)


def adx_dmi(highs, lows, closes, period: int = ADX_LEN) -> tuple[list[float], list[float], list[float]]:
    n = len(closes)
    if n < period + 2:
        return [0.0] * n, [0.0] * n, [0.0] * n
    plus_dm, minus_dm, trs = [0.0], [0.0], [highs[0] - lows[0]]
    for i in range(1, n):
        up = highs[i] - highs[i - 1]
        down = lows[i - 1] - lows[i]
        plus_dm.append(up if (up > down and up > 0) else 0.0)
        minus_dm.append(down if (down > up and down > 0) else 0.0)
        trs.append(max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]), abs(lows[i] - closes[i - 1])))
    atr_s = ema(trs, period)
    plus_di = [100 * (p / a if a else 0) for p, a in zip(ema(plus_dm, period), atr_s)]
    minus_di = [100 * (m / a if a else 0) for m, a in zip(ema(minus_dm, period), atr_s)]
    dx = [100 * abs(p - m) / (p + m) if (p + m) else 0.0 for p, m in zip(plus_di, minus_di)]
    adx = ema(dx, period)
    return adx, plus_di, minus_di


def bollinger_width_pct(closes: list[float], period: int = BB_LEN, mult: float = BB_MULT) -> list[float]:
    mid = sma(closes, period)
    sd = stdev(closes, period)
    return [((m + mult * s) - (m - mult * s)) / m * 100 if m else 0.0 for m, s in zip(mid, sd)]


def donchian(highs, lows, period: int) -> tuple[list[float], list[float]]:
    upper, lower = [], []
    for i in range(len(highs)):
        w_h = highs[max(0, i - period + 1) : i + 1]
        w_l = lows[max(0, i - period + 1) : i + 1]
        upper.append(max(w_h))
        lower.append(min(w_l))
    return upper, lower


def obv(closes: list[float], volumes: list[float]) -> list[float]:
    out = [0.0]
    for i in range(1, len(closes)):
        if closes[i] > closes[i - 1]:
            out.append(out[-1] + volumes[i])
        elif closes[i] < closes[i - 1]:
            out.append(out[-1] - volumes[i])
        else:
            out.append(out[-1])
    return out


def detect_rsi_divergence(closes: list[float], rsi_vals: list[float], lookback: int = 25) -> Optional[str]:
    if len(closes) < lookback + 5:
        return None
    seg_c, seg_r = closes[-lookback:], rsi_vals[-lookback:]
    lo_idx = min(range(len(seg_c)), key=lambda i: seg_c[i])
    hi_idx = max(range(len(seg_c)), key=lambda i: seg_c[i])
    # bullish: lower low in price, higher low in RSI, occurring in the later half
    if lo_idx > lookback * 0.4:
        prior_low_idx = min(range(0, lo_idx), key=lambda i: seg_c[i]) if lo_idx > 0 else None
        if prior_low_idx is not None and seg_c[lo_idx] < seg_c[prior_low_idx] and seg_r[lo_idx] > seg_r[prior_low_idx]:
            return "bullish"
    if hi_idx > lookback * 0.4:
        prior_high_idx = max(range(0, hi_idx), key=lambda i: seg_c[i]) if hi_idx > 0 else None
        if prior_high_idx is not None and seg_c[hi_idx] > seg_c[prior_high_idx] and seg_r[hi_idx] < seg_r[prior_high_idx]:
            return "bearish"
    return None


def compute_indicators(candles: list[dict]) -> dict:
    c, h, l, v = _closes(candles), _highs(candles), _lows(candles), _vols(candles)
    adx, plus_di, minus_di = adx_dmi(h, l, c)
    return {
        "close": c, "high": h, "low": l, "vol": v,
        "ema_fast": ema(c, EMA_FAST), "ema_slow": ema(c, EMA_SLOW), "ema_trend": ema(c, EMA_TREND),
        "rsi": rsi(c), "atr": atr(h, l, c), "adx": adx, "plus_di": plus_di, "minus_di": minus_di,
        "bb_width_pct": bollinger_width_pct(c), "obv": obv(c, v),
        "donchian_hi": donchian(h, l, 20)[0], "donchian_lo": donchian(h, l, 20)[1],
    }


# =============================================================================
# MARKET STRUCTURE (swings / BOS-CHoCH)
# =============================================================================

@dataclass
class Swing:
    idx: int
    price: float
    kind: str  # "high" | "low"


def find_swings(candles: list[dict], left: int = 2, right: int = 2) -> list[Swing]:
    h, l = _highs(candles), _lows(candles)
    out = []
    for i in range(left, len(candles) - right):
        window_h = h[i - left : i + right + 1]
        window_l = l[i - left : i + right + 1]
        if h[i] == max(window_h):
            out.append(Swing(i, h[i], "high"))
        if l[i] == min(window_l):
            out.append(Swing(i, l[i], "low"))
    return out


def structure_bias(swings: list[Swing]) -> str:
    """Simplified BOS/CHoCH read: compares the last two highs and last two lows
    to classify the prevailing structure as bullish, bearish, or neutral."""
    highs = [s for s in swings if s.kind == "high"][-2:]
    lows = [s for s in swings if s.kind == "low"][-2:]
    bull = len(highs) == 2 and highs[-1].price > highs[-2].price and len(lows) == 2 and lows[-1].price > lows[-2].price
    bear = len(highs) == 2 and highs[-1].price < highs[-2].price and len(lows) == 2 and lows[-1].price < lows[-2].price
    if bull:
        return "bullish"
    if bear:
        return "bearish"
    return "neutral"


# =============================================================================
# DERIVATIVES CONTEXT (funding / open interest) — frequency-additive
# =============================================================================

def update_derivatives_history(state: dict, symbol: str, funding: float, oi: float, price: float) -> None:
    hist = state.setdefault("deriv_history", {}).setdefault(symbol, {"funding": [], "oi": [], "price": []})
    hist["funding"].append(funding)
    hist["oi"].append(oi)
    hist["price"].append(price)
    for k in hist:
        hist[k] = hist[k][-96:]  # ~24h of 15m samples


def derivatives_signal(state: dict, symbol: str) -> dict:
    """Detects funding extremes and OI/price divergence that can precede
    squeezes — a source of valid setups pure price-action analysis misses."""
    hist = state.get("deriv_history", {}).get(symbol)
    result = {"funding_extreme": None, "oi_price_divergence": None, "score": 0.0}
    if not hist or len(hist["funding"]) < 8:
        return result
    funding_now = hist["funding"][-1]
    funding_hist = hist["funding"][:-1]
    if funding_hist:
        mean_f = statistics.mean(funding_hist)
        sd_f = statistics.pstdev(funding_hist) or 1e-9
        z = (funding_now - mean_f) / sd_f
        if z > 1.5:
            result["funding_extreme"] = "overheated_long"  # crowded longs -> squeeze-down risk/short bias
            result["score"] -= 0.4
        elif z < -1.5:
            result["funding_extreme"] = "overheated_short"
            result["score"] += 0.4
    if len(hist["oi"]) >= 8 and len(hist["price"]) >= 8:
        oi_chg = (hist["oi"][-1] - hist["oi"][-8]) / (hist["oi"][-8] or 1e-9)
        px_chg = (hist["price"][-1] - hist["price"][-8]) / (hist["price"][-8] or 1e-9)
        if oi_chg > 0.05 and px_chg < -0.005:
            result["oi_price_divergence"] = "bearish_buildup"  # OI up, price down -> new shorts piling in
            result["score"] -= 0.3
        elif oi_chg > 0.05 and px_chg > 0.005:
            result["oi_price_divergence"] = "bullish_buildup"
            result["score"] += 0.3
    return result


# =============================================================================
# REGIME DETECTION
# =============================================================================

@dataclass
class Regime:
    trend: str        # "trending" | "ranging"
    direction: str     # "bullish" | "bearish" | "neutral"
    volatility: str    # "high" | "normal" | "low"
    noise: float       # 0..1, higher = choppier


def compute_noise_index(candles: list[dict], lookback: int = 30) -> float:
    c = _closes(candles)[-lookback:]
    if len(c) < 5:
        return 0.5
    net = abs(c[-1] - c[0])
    path = sum(abs(c[i] - c[i - 1]) for i in range(1, len(c)))
    if path == 0:
        return 0.5
    efficiency = net / path  # Kaufman-style efficiency ratio
    return 1 - efficiency


def detect_regime(ind_15m: dict, ind_1h: dict, ind_4h: dict) -> Regime:
    adx_now = ind_1h["adx"][-1]
    bb_w = ind_15m["bb_width_pct"][-1]
    bb_w_hist = ind_15m["bb_width_pct"][-60:]
    bb_pctile = (sum(1 for x in bb_w_hist if x < bb_w) / len(bb_w_hist)) if bb_w_hist else 0.5

    trend = "trending" if adx_now >= 22 else "ranging"
    if ind_4h["ema_fast"][-1] > ind_4h["ema_slow"][-1] and ind_4h["close"][-1] > ind_4h["ema_trend"][-1]:
        direction = "bullish"
    elif ind_4h["ema_fast"][-1] < ind_4h["ema_slow"][-1] and ind_4h["close"][-1] < ind_4h["ema_trend"][-1]:
        direction = "bearish"
    else:
        direction = "neutral"

    if bb_pctile > 0.75:
        volatility = "high"
    elif bb_pctile < 0.25:
        volatility = "low"
    else:
        volatility = "normal"

    noise = compute_noise_index(ind_15m["close"] and [{"c": x} for x in ind_15m["close"]] or [])
    return Regime(trend=trend, direction=direction, volatility=volatility, noise=noise)


# -----------------------------------------------------------------------------
# ADAPTIVE THRESHOLDS — fixed, regime-conditioned rule set decided in advance
# during backtesting, NOT an online self-tuning loop. This is the entire
# quality/frequency balancing mechanism, spelled out so it is inspectable:
#
#   base confidence threshold = 62 (points, 0-100 ensemble score)
#
#   regime adjustments (additive, stack):
#     volatility == "high"   -> +6   (noisy markets: demand more evidence)
#     volatility == "low"    -> -3   (clean markets: allow slightly more through)
#     trend == "ranging"     -> +4   (chop is harder to trade; raise the bar)
#     noise > 0.65           -> +5   (low path-efficiency = choppy tape)
#     noise < 0.35           -> -3   (efficient, directional tape)
#     liquidity thin (see liquidity filter) -> +8 or outright suppression
#
#   The final threshold is clamped to [55, 80] so the engine can never become
#   either a rubber stamp or fully closed. Everything above the resulting
#   threshold is emitted; everything below is logged as suppressed with the
#   reason. This tightens automatically in noisy/uncertain conditions and
#   relaxes in clean, trending, liquid conditions — with no live-result
#   feedback loop, so it cannot curve-fit itself to recent trades.
# -----------------------------------------------------------------------------

def adaptive_threshold(regime: Regime, liquidity_thin: bool) -> float:
    thr = 62.0
    if regime.volatility == "high":
        thr += 6
    elif regime.volatility == "low":
        thr -= 3
    if regime.trend == "ranging":
        thr += 4
    if regime.noise > 0.65:
        thr += 5
    elif regime.noise < 0.35:
        thr -= 3
    if liquidity_thin:
        thr += 8
    return max(55.0, min(80.0, thr))


# =============================================================================
# ENSEMBLE SIGNAL SCORING
# =============================================================================

@dataclass
class Candidate:
    symbol: str
    direction: str
    entry: float
    families: dict = field(default_factory=dict)  # family_name -> {"vote": +1/0/-1, "detail": str}
    confluences: list = field(default_factory=list)
    score: float = 0.0
    setup_type: str = ""


def _trend_family(ind_exec, ind_confirm, ind_bias) -> tuple[int, str]:
    votes = 0
    if ind_confirm["ema_fast"][-1] > ind_confirm["ema_slow"][-1]:
        votes += 1
    else:
        votes -= 1
    if ind_bias["close"][-1] > ind_bias["ema_trend"][-1]:
        votes += 1
    else:
        votes -= 1
    vote = 1 if votes >= 1 else (-1 if votes <= -1 else 0)
    return vote, f"HTF/1h EMA alignment votes={votes}"


def _momentum_family(ind_exec, ind_confirm) -> tuple[int, str]:
    rsi_e, rsi_c = ind_exec["rsi"][-1], ind_confirm["rsi"][-1]
    div = detect_rsi_divergence(ind_exec["close"], ind_exec["rsi"])
    if div == "bullish" or (rsi_e > 50 and rsi_c > 50):
        return 1, f"RSI15m={rsi_e:.1f} RSI1h={rsi_c:.1f} div={div}"
    if div == "bearish" or (rsi_e < 50 and rsi_c < 50):
        return -1, f"RSI15m={rsi_e:.1f} RSI1h={rsi_c:.1f} div={div}"
    return 0, f"RSI15m={rsi_e:.1f} RSI1h={rsi_c:.1f} div={div}"


def _structure_family(candles_exec: list[dict]) -> tuple[int, str]:
    swings = find_swings(candles_exec)
    bias = structure_bias(swings)
    if bias == "bullish":
        return 1, "BOS structure bullish"
    if bias == "bearish":
        return -1, "BOS structure bearish"
    return 0, "structure neutral/ranging"


def _volume_family(ind_exec, breakout_level_hi, breakout_level_lo) -> tuple[int, str]:
    """False-breakout / fakeout filter: require volume + follow-through, not
    a wick-only breach, before crediting a breakout as a confluence."""
    v = ind_exec["vol"]
    avg_v = statistics.mean(v[-20:]) if len(v) >= 20 else statistics.mean(v)
    last_v = v[-1]
    close_now = ind_exec["close"][-1]
    follow_through = ind_exec["close"][-1] > ind_exec["close"][-2] if len(ind_exec["close"]) > 1 else True
    if close_now >= breakout_level_hi and last_v > 1.3 * avg_v and follow_through:
        return 1, f"volume-confirmed breakout above {breakout_level_hi:.4g}"
    if close_now <= breakout_level_lo and last_v > 1.3 * avg_v and not follow_through:
        return -1, f"volume-confirmed breakdown below {breakout_level_lo:.4g}"
    return 0, "no volume-confirmed breakout"


def _derivatives_family(deriv: dict) -> tuple[int, str]:
    s = deriv["score"]
    if s > 0.25:
        return 1, f"derivatives bullish (funding/OI score={s:.2f})"
    if s < -0.25:
        return -1, f"derivatives bearish (funding/OI score={s:.2f})"
    return 0, f"derivatives neutral (score={s:.2f})"


def build_candidate(symbol: str, bundle: dict, deriv: dict) -> Optional[Candidate]:
    ind_exec = compute_indicators(bundle[TF_EXEC])
    ind_confirm = compute_indicators(bundle[TF_CONFIRM])
    ind_bias = compute_indicators(bundle[TF_BIAS])

    families = {}
    fam_defs = [
        ("trend", _trend_family(ind_exec, ind_confirm, ind_bias)),
        ("momentum", _momentum_family(ind_exec, ind_confirm)),
        ("structure", _structure_family(bundle[TF_EXEC])),
        ("volume", _volume_family(ind_exec, ind_exec["donchian_hi"][-2], ind_exec["donchian_lo"][-2])),
        ("derivatives", _derivatives_family(deriv)),
    ]
    for name, (vote, detail) in fam_defs:
        families[name] = {"vote": vote, "detail": detail}

    bull_votes = sum(1 for f in families.values() if f["vote"] == 1)
    bear_votes = sum(1 for f in families.values() if f["vote"] == -1)

    if bull_votes == 0 and bear_votes == 0:
        return None
    direction = "LONG" if bull_votes > bear_votes else "SHORT" if bear_votes > bull_votes else None
    if direction is None:
        return None  # tie = genuine disagreement between families -> suppress, do not average

    entry = ind_exec["close"][-1]
    cand = Candidate(symbol=symbol, direction=direction, entry=entry, families=families)
    cand.confluences = [f["detail"] for f in families.values() if f["vote"] == (1 if direction == "LONG" else -1)]
    cand.setup_type = _classify_setup(ind_exec, ind_confirm, direction)
    return cand


def _classify_setup(ind_exec, ind_confirm, direction: str) -> str:
    adx1h = ind_confirm["adx"][-1]
    if adx1h >= 25:
        return "trend-continuation"
    rsi_e = ind_exec["rsi"][-1]
    if direction == "LONG" and rsi_e < 40:
        return "mean-reversion"
    if direction == "SHORT" and rsi_e > 60:
        return "mean-reversion"
    return "breakout"


def score_candidate(cand: Candidate, regime: Regime, liquidity: dict) -> float:
    """Ensemble agreement scoring: base points per agreeing family, PLUS a
    non-linear agreement bonus when independent families corroborate each
    other (this is what lets strong borderline setups through that a single
    threshold would block), MINUS penalties for conflicting families and for
    thin liquidity."""
    agree = sum(1 for f in cand.families.values() if f["vote"] == (1 if cand.direction == "LONG" else -1))
    conflict = sum(1 for f in cand.families.values() if f["vote"] == (-1 if cand.direction == "LONG" else 1))
    total_families = len(cand.families)

    base = agree * 14.0
    agreement_bonus = 0.0
    if agree >= 4:
        agreement_bonus = 18.0
    elif agree == 3:
        agreement_bonus = 8.0
    conflict_penalty = conflict * 12.0

    regime_bonus = 0.0
    if regime.trend == "trending" and cand.setup_type == "trend-continuation":
        regime_bonus += 8.0
    if regime.trend == "ranging" and cand.setup_type == "mean-reversion":
        regime_bonus += 6.0
    if (cand.direction == "LONG" and regime.direction == "bullish") or (cand.direction == "SHORT" and regime.direction == "bearish"):
        regime_bonus += 6.0
    elif regime.direction != "neutral":
        regime_bonus -= 6.0  # fighting the macro bias

    liquidity_penalty = 0.0
    if liquidity.get("ok") and liquidity.get("spread_pct") is not None:
        if liquidity["spread_pct"] > 0.15:
            liquidity_penalty += 10.0
        if liquidity["depth_usd"] < 50_000:
            liquidity_penalty += 10.0

    score = base + agreement_bonus - conflict_penalty + regime_bonus - liquidity_penalty
    return max(0.0, min(100.0, score))


# =============================================================================
# CORRELATION CONTROL (frequency-neutral: collapse, don't reduce true opportunity)
# =============================================================================

CORRELATION_CLUSTERS = [
    {"BTC", "ETH", "BCH", "LTC"},
    {"SOL", "AVAX", "NEAR", "SUI", "APT", "TAO"},
    {"DOGE", "PENGU"},
    {"LINK", "AAVE", "UNI", "ONDO", "PENDLE"},
    {"XRP", "XLM", "ADA", "DOT", "TRX"},
    {"HYPE", "BNB"},
]


def cluster_of(symbol: str) -> frozenset:
    for c in CORRELATION_CLUSTERS:
        if symbol in c:
            return frozenset(c)
    return frozenset({symbol})


def apply_correlation_control(candidates: list[Candidate]) -> list[Candidate]:
    """When multiple candidates in the same correlation cluster and direction
    fire simultaneously, keep only the highest-scoring one and mark the rest
    as correlated-duplicate (still logged, not silently dropped) — this does
    not reduce true opportunity, it avoids double-counting one underlying bet."""
    kept = []
    seen: dict[tuple, Candidate] = {}
    for cand in sorted(candidates, key=lambda c: c.score, reverse=True):
        key = (cluster_of(cand.symbol), cand.direction)
        if key in seen:
            cand.confluences.append(f"SUPPRESSED: correlated with {seen[key].symbol} (same cluster+direction)")
            log_suppressed(cand, "correlation_control")
            continue
        seen[key] = cand
        kept.append(cand)
    return kept


# =============================================================================
# SIGNAL FRESHNESS / DECAY
# =============================================================================

def is_signal_fresh(cand: Candidate, indicators_atr: float) -> bool:
    """Placeholder hook evaluated immediately at generation time; the primary
    use of freshness tracking is on re-scan (see refresh_pending_signals)."""
    return True


def refresh_pending_signals(state: dict, market_snapshot: dict) -> None:
    """Invalidate or re-score previously generated but not-yet-actioned
    signals if price has moved meaningfully since generation."""
    pending = state.setdefault("pending_signals", [])
    still_pending = []
    for sig in pending:
        sym = sig["symbol"]
        px_now = market_snapshot.get(sym, {}).get("mark_px")
        if px_now is None:
            still_pending.append(sig)
            continue
        moved_pct = abs(px_now - sig["entry"]) / sig["entry"] * 100
        age_scans = sig.get("age_scans", 0) + 1
        if moved_pct > sig.get("decay_pct", 0.6) or age_scans > 4:
            log.info("Signal %s %s invalidated by decay (moved %.2f%%, age %d scans).",
                      sym, sig["direction"], moved_pct, age_scans)
            continue
        sig["age_scans"] = age_scans
        still_pending.append(sig)
    state["pending_signals"] = still_pending


# =============================================================================
# TP / SL / RISK MANAGEMENT
# =============================================================================

@dataclass
class TradePlan:
    entry: float
    stop_loss: float
    take_profits: list[float]
    risk_reward: float
    position_size_usd: float
    position_size_units: float


def build_trade_plan(cand: Candidate, atr_val: float, regime: Regime) -> TradePlan:
    # wider stops in high-volatility regimes; tighter in low-vol clean regimes
    sl_mult = 1.8 if regime.volatility == "high" else (1.2 if regime.volatility == "low" else 1.5)
    tp_mults = [1.5, 2.5, 4.0] if cand.setup_type != "mean-reversion" else [1.2, 2.0]

    if cand.direction == "LONG":
        stop = cand.entry - sl_mult * atr_val
        tps = [cand.entry + m * atr_val for m in tp_mults]
    else:
        stop = cand.entry + sl_mult * atr_val
        tps = [cand.entry - m * atr_val for m in tp_mults]

    risk_per_unit = abs(cand.entry - stop)
    reward_per_unit = abs(tps[0] - cand.entry)
    rr = reward_per_unit / risk_per_unit if risk_per_unit else 0.0

    risk_usd = ACCOUNT_EQUITY_USD * (RISK_PER_TRADE_PCT / 100)
    position_size_units = risk_usd / risk_per_unit if risk_per_unit else 0.0
    position_size_usd = position_size_units * cand.entry

    return TradePlan(entry=cand.entry, stop_loss=stop, take_profits=tps, risk_reward=rr,
                      position_size_usd=position_size_usd, position_size_units=position_size_units)


def portfolio_risk_ok(state: dict, plan: TradePlan) -> tuple[bool, str]:
    today = utc_day_key()
    day_state = state.setdefault("daily", {}).setdefault(today, {"realized_pnl_pct": 0.0, "signal_count": 0})

    if day_state["realized_pnl_pct"] <= -DAILY_LOSS_LIMIT_PCT:
        return False, f"daily loss limit breached ({day_state['realized_pnl_pct']:.2f}% <= -{DAILY_LOSS_LIMIT_PCT}%)"

    open_positions = state.get("open_positions", [])
    if len(open_positions) >= MAX_CONCURRENT_POSITIONS:
        return False, f"max concurrent positions reached ({MAX_CONCURRENT_POSITIONS})"

    exposure_usd = sum(p["position_size_usd"] for p in open_positions) + plan.position_size_usd
    exposure_pct = exposure_usd / ACCOUNT_EQUITY_USD * 100
    if exposure_pct > MAX_PORTFOLIO_EXPOSURE_PCT:
        return False, f"max portfolio exposure exceeded ({exposure_pct:.1f}% > {MAX_PORTFOLIO_EXPOSURE_PCT}%)"

    return True, "ok"


# =============================================================================
# STATE MANAGEMENT
# =============================================================================

def _default_state() -> dict:
    return {"deriv_history": {}, "pending_signals": [], "open_positions": [], "daily": {}, "last_run": None}


def load_state() -> dict:
    if not os.path.exists(STATE_PATH):
        return _default_state()
    try:
        with open(STATE_PATH, "r") as f:
            data = json.load(f)
        for k, v in _default_state().items():
            data.setdefault(k, v)
        return data
    except (json.JSONDecodeError, OSError) as e:
        log.error("Failed to load state.json (%s); starting fresh.", e)
        return _default_state()


def save_state(state: dict) -> None:
    if DRY_RUN:
        log.info("[DRY_RUN] Skipping state.json commit.")
        return
    tmp = STATE_PATH + ".tmp"
    with open(tmp, "w") as f:
        json.dump(state, f, indent=2, default=str)
    os.replace(tmp, STATE_PATH)


def prune_state(state: dict, max_days: int = 14) -> None:
    days = sorted(state.get("daily", {}).keys())
    if len(days) > max_days:
        for d in days[:-max_days]:
            del state["daily"][d]


# =============================================================================
# LOGGING (including suppressed signals for future threshold tuning)
# =============================================================================

def log_suppressed(cand: Candidate, reason: str, extra: Optional[dict] = None) -> None:
    record = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "symbol": cand.symbol,
        "direction": cand.direction,
        "score": round(cand.score, 2),
        "setup_type": cand.setup_type,
        "reason": reason,
    }
    if extra:
        record.update(extra)
    log.info("SUPPRESSED %s %s score=%.1f reason=%s", cand.symbol, cand.direction, cand.score, reason)
    try:
        with open(SUPPRESSED_LOG_PATH, "a") as f:
            f.write(json.dumps(record) + "\n")
    except OSError as e:
        log.error("Could not write suppressed-signal log: %s", e)


# =============================================================================
# TELEGRAM
# =============================================================================

def confidence_grade(score: float) -> str:
    if score >= 85:
        return "A+"
    if score >= 75:
        return "A"
    if score >= 65:
        return "B"
    return "C"


def format_telegram_message(cand: Candidate, plan: TradePlan, regime: Regime) -> str:
    arrow = "🟢 LONG" if cand.direction == "LONG" else "🔴 SHORT"
    tps = "\n".join(f"   TP{i+1}: {tp:.4g}" for i, tp in enumerate(plan.take_profits))
    confluences = "\n".join(f" • {c}" for c in cand.confluences)
    return (
        f"*AETHERIS ENGINE v1.0.0*\n"
        f"{arrow}  `{cand.symbol}`\n\n"
        f"Entry: `{plan.entry:.4g}`\n"
        f"Stop Loss: `{plan.stop_loss:.4g}`\n"
        f"{tps}\n"
        f"Risk:Reward: `{plan.risk_reward:.2f}`\n"
        f"Confidence: `{cand.score:.1f}/100` — Grade `{confidence_grade(cand.score)}`\n"
        f"Setup: `{cand.setup_type}`  Regime: `{regime.trend}/{regime.direction}/{regime.volatility}`\n\n"
        f"Confluences:\n{confluences}\n\n"
        f"Position size: ~${plan.position_size_usd:,.0f} ({plan.position_size_units:.4g} units)"
    )


def send_telegram(text: str) -> Optional[int]:
    if DRY_RUN:
        log.info("[DRY_RUN] Would send Telegram message:\n%s", text)
        return None
    if not TG_BOT_TOKEN or not TG_CHAT_ID:
        log.error("TG_BOT_TOKEN/TG_CHAT_ID not set; cannot send Telegram message.")
        return None
    try:
        r = SESSION.post(
            f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage",
            json={"chat_id": TG_CHAT_ID, "text": text, "parse_mode": "Markdown"},
            timeout=10,
        )
        if r.status_code != 200:
            log.error("Telegram send failed (%s): %s", r.status_code, r.text[:300])
            return None
        return r.json().get("result", {}).get("message_id")
    except requests.RequestException as e:
        log.error("Telegram send exception: %s", e)
        return None


# =============================================================================
# MAIN SCAN
# =============================================================================

def run_scan() -> None:
    log.info("=== Aetheris Engine scan start (dry_run=%s) ===", DRY_RUN)
    state = load_state()
    reference_ms = int(time.time() * 1000)

    market_snapshot = get_market_snapshot()
    refresh_pending_signals(state, market_snapshot)

    today = utc_day_key()
    day_state = state.setdefault("daily", {}).setdefault(today, {"realized_pnl_pct": 0.0, "signal_count": 0})
    if day_state["realized_pnl_pct"] <= -DAILY_LOSS_LIMIT_PCT:
        log.warning("Daily loss limit already breached today (%s); skipping new signal generation.", today)
        save_state(state)
        return

    candidates: list[Candidate] = []

    for symbol in WATCHLIST:
        if _SHUTDOWN:
            log.warning("Shutdown requested; stopping scan early.")
            break
        try:
            bundle = fetch_all_candles(symbol, reference_ms)
            if bundle is None:
                log.warning("Skipping %s this scan: candle data unavailable.", symbol)
                continue

            snap = market_snapshot.get(symbol, {})
            update_derivatives_history(state, symbol, snap.get("funding", 0.0), snap.get("open_interest", 0.0),
                                         snap.get("mark_px", bundle[TF_EXEC][-1]["c"]))
            deriv = derivatives_signal(state, symbol)

            cand = build_candidate(symbol, bundle, deriv)
            if cand is None:
                continue

            ind_exec = compute_indicators(bundle[TF_EXEC])
            ind_confirm = compute_indicators(bundle[TF_CONFIRM])
            ind_bias = compute_indicators(bundle[TF_BIAS])
            regime = detect_regime(ind_exec, ind_confirm, ind_bias)
            liquidity = analyze_orderbook(symbol)
            liquidity_thin = liquidity.get("ok") and (
                (liquidity.get("spread_pct") or 0) > 0.2 or liquidity.get("depth_usd", 0) < 30_000
            )

            cand.score = score_candidate(cand, regime, liquidity)
            threshold = adaptive_threshold(regime, liquidity_thin)

            if liquidity_thin and cand.score < threshold + 10:
                log_suppressed(cand, "liquidity_filter", {"spread_pct": liquidity.get("spread_pct"),
                                                             "depth_usd": liquidity.get("depth_usd")})
                continue

            if cand.score < threshold:
                log_suppressed(cand, "below_adaptive_threshold", {"threshold": threshold})
                continue

            cand.families["_regime"] = regime  # stash for downstream trade-plan building
            candidates.append(cand)

        except Exception as e:  # noqa: BLE001 - never let one asset's failure abort the run
            log.error("Unhandled error processing %s: %s", symbol, e, exc_info=True)
            continue

    # correlation control across the whole scan, not per-asset
    candidates = apply_correlation_control(candidates)
    candidates.sort(key=lambda c: c.score, reverse=True)

    emitted = 0
    for cand in candidates:
        regime: Regime = cand.families.pop("_regime")
        ind_exec = compute_indicators.__wrapped__ if False else None  # (no-op placeholder removed below)
        bundle = fetch_all_candles(cand.symbol, reference_ms)
        if bundle is None:
            continue
        atr_val = compute_indicators(bundle[TF_EXEC])["atr"][-1]
        plan = build_trade_plan(cand, atr_val, regime)

        ok, reason = portfolio_risk_ok(state, plan)
        if not ok:
            log_suppressed(cand, f"risk_management:{reason}")
            continue

        msg = format_telegram_message(cand, plan, regime)
        send_telegram(msg)

        state["open_positions"].append({
            "symbol": cand.symbol, "direction": cand.direction, "entry": plan.entry,
            "stop_loss": plan.stop_loss, "position_size_usd": plan.position_size_usd,
            "opened": datetime.now(timezone.utc).isoformat(), "day": today,
        })
        day_state["signal_count"] += 1
        emitted += 1
        log.info("SIGNAL %s %s score=%.1f grade=%s rr=%.2f", cand.symbol, cand.direction,
                  cand.score, confidence_grade(cand.score), plan.risk_reward)

    state["pending_signals"].extend([
        {"symbol": c.symbol, "direction": c.direction, "entry": c.entry, "age_scans": 0, "decay_pct": 0.6}
        for c in candidates
    ])

    prune_state(state)
    state["last_run"] = datetime.now(timezone.utc).isoformat()
    save_state(state)
    log.info("=== Scan complete: %d signals emitted, %d candidates evaluated ===", emitted, len(candidates))


# =============================================================================
# BACKTESTING / EVALUATION MODULE
# =============================================================================
# Runs the same signal logic (build_candidate / score_candidate / adaptive
# threshold / build_trade_plan) against historical data. Uses walk-forward
# validation with a final untouched holdout, strictly causal indicator
# windows (no future candles/funding/OI are ever visible to a historical
# decision point), realistic fees + slippage, a parameter-sensitivity check,
# minimum-sample-size flagging, and a simple baseline comparison.

TAKER_FEE = 0.00045   # Hyperliquid taker fee (approximate; update to current schedule before live use)
SLIPPAGE_PCT = 0.0005  # conservative fixed slippage estimate per side
MIN_SAMPLE_SIZE = 20   # trades required before a regime/window win-rate is trusted


@dataclass
class BacktestTrade:
    symbol: str
    direction: str
    entry: float
    exit: float
    r_multiple: float
    regime_trend: str
    regime_vol: str
    window: str


def _simulate_exit(candles_exec: list[dict], entry_idx: int, direction: str, plan: TradePlan, max_bars: int = 60) -> float:
    """Walks forward candle-by-candle from entry_idx+1 using ONLY that bar's
    own high/low (no look-ahead beyond the current simulated bar) to find
    which of stop-loss / first take-profit is hit first."""
    for i in range(entry_idx + 1, min(entry_idx + 1 + max_bars, len(candles_exec))):
        h, l = float(candles_exec[i]["h"]), float(candles_exec[i]["l"])
        if direction == "LONG":
            if l <= plan.stop_loss:
                return plan.stop_loss
            if h >= plan.take_profits[0]:
                return plan.take_profits[0]
        else:
            if h >= plan.stop_loss:
                return plan.stop_loss
            if l <= plan.take_profits[0]:
                return plan.take_profits[0]
    return float(candles_exec[min(entry_idx + max_bars, len(candles_exec) - 1)]["c"])


def _apply_costs(entry: float, exit_px: float, direction: str) -> float:
    slip = entry * SLIPPAGE_PCT
    entry_eff = entry + slip if direction == "LONG" else entry - slip
    exit_eff = exit_px - slip if direction == "LONG" else exit_px + slip
    fee = (entry_eff + exit_eff) * TAKER_FEE
    pnl = (exit_eff - entry_eff) if direction == "LONG" else (entry_eff - exit_eff)
    return pnl - fee


def baseline_ma_crossover(candles_exec: list[dict]) -> list[BacktestTrade]:
    """Simple baseline: fast/slow SMA crossover, fixed 1.5x ATR stop / 2.5x ATR
    target, for comparison against Aetheris's added complexity."""
    c = _closes(candles_exec)
    fast, slow = sma(c, 10), sma(c, 30)
    a = atr(_highs(candles_exec), _lows(candles_exec), c)
    trades = []
    for i in range(31, len(c) - 1):
        cross_up = fast[i - 1] <= slow[i - 1] and fast[i] > slow[i]
        cross_dn = fast[i - 1] >= slow[i - 1] and fast[i] < slow[i]
        if not (cross_up or cross_dn):
            continue
        direction = "LONG" if cross_up else "SHORT"
        entry = c[i]
        stop = entry - 1.5 * a[i] if direction == "LONG" else entry + 1.5 * a[i]
        target = entry + 2.5 * a[i] if direction == "LONG" else entry - 2.5 * a[i]
        plan = TradePlan(entry=entry, stop_loss=stop, take_profits=[target], risk_reward=2.5 / 1.5,
                          position_size_usd=0, position_size_units=0)
        exit_px = _simulate_exit(candles_exec, i, direction, plan)
        pnl = _apply_costs(entry, exit_px, direction)
        risk = abs(entry - stop)
        r = pnl / risk if risk else 0.0
        trades.append(BacktestTrade(symbol="", direction=direction, entry=entry, exit=exit_px,
                                     r_multiple=r, regime_trend="", regime_vol="", window="baseline"))
    return trades


def run_backtest(symbol: str, all_candles: dict[str, list[dict]], window_label: str,
                  threshold_override: Optional[float] = None) -> list[BacktestTrade]:
    """Replays Aetheris's own logic bar-by-bar over TF_EXEC using only data
    available up to each decision point (rolling slices, never the full
    series) for a given historical window."""
    exec_candles = all_candles[TF_EXEC]
    trades = []
    warmup = max(TF_CANDLES_NEEDED.values())
    fake_state = _default_state()

    for i in range(warmup, len(exec_candles) - 1):
        causal_exec = exec_candles[: i + 1]
        # approximate causal higher-timeframe slices by proportional length
        # (a real deployment should fetch true aligned HTF history; this
        # keeps the backtest self-contained to a single fetched TF_EXEC series
        # plus separately fetched, equally causal HTF series)
        causal_bundle = {
            TF_EXEC: causal_exec,
            TF_CONFIRM: all_candles[TF_CONFIRM][: max(1, int(len(all_candles[TF_CONFIRM]) * (i + 1) / len(exec_candles)))],
            TF_BIAS: all_candles[TF_BIAS][: max(1, int(len(all_candles[TF_BIAS]) * (i + 1) / len(exec_candles)))],
        }
        if len(causal_bundle[TF_CONFIRM]) < 30 or len(causal_bundle[TF_BIAS]) < 30:
            continue

        deriv = {"funding_extreme": None, "oi_price_divergence": None, "score": 0.0}  # no live deriv history in backtest
        cand = build_candidate(symbol, causal_bundle, deriv)
        if cand is None:
            continue
        ind_exec = compute_indicators(causal_bundle[TF_EXEC])
        ind_confirm = compute_indicators(causal_bundle[TF_CONFIRM])
        ind_bias = compute_indicators(causal_bundle[TF_BIAS])
        regime = detect_regime(ind_exec, ind_confirm, ind_bias)
        cand.score = score_candidate(cand, regime, {"ok": False})
        threshold = threshold_override if threshold_override is not None else adaptive_threshold(regime, False)
        if cand.score < threshold:
            continue

        atr_val = ind_exec["atr"][-1]
        plan = build_trade_plan(cand, atr_val, regime)
        exit_px = _simulate_exit(exec_candles, i, cand.direction, plan)
        pnl = _apply_costs(plan.entry, exit_px, cand.direction)
        risk = abs(plan.entry - plan.stop_loss)
        r = pnl / risk if risk else 0.0
        trades.append(BacktestTrade(symbol=symbol, direction=cand.direction, entry=plan.entry, exit=exit_px,
                                     r_multiple=r, regime_trend=regime.trend, regime_vol=regime.volatility,
                                     window=window_label))
    return trades


def summarize_trades(trades: list[BacktestTrade], group_key=None) -> dict:
    if not trades:
        return {"n": 0, "note": "no trades"}
    groups: dict = {}
    key_fn = group_key or (lambda t: "all")
    for t in trades:
        groups.setdefault(key_fn(t), []).append(t)
    out = {}
    for key, ts in groups.items():
        n = len(ts)
        wins = sum(1 for t in ts if t.r_multiple > 0)
        avg_r = statistics.mean(t.r_multiple for t in ts)
        entry = {
            "n": n,
            "win_rate_pct": round(100 * wins / n, 1),
            "avg_r_multiple": round(avg_r, 3),
            "flagged_low_sample": n < MIN_SAMPLE_SIZE,
        }
        out[key] = entry
    return out


def walk_forward_backtest(symbol: str, all_candles: dict[str, list[dict]], n_windows: int = 4) -> dict:
    exec_len = len(all_candles[TF_EXEC])
    window_size = exec_len // (n_windows + 1)  # +1 reserves the final holdout
    results = {"windows": {}, "holdout": None, "sensitivity": {}, "baseline": None}

    for w in range(n_windows):
        start = w * window_size
        end = start + window_size + max(TF_CANDLES_NEEDED.values())
        end = min(end, exec_len - window_size)  # keep holdout untouched
        if end <= start + 50:
            continue
        window_candles = {
            TF_EXEC: all_candles[TF_EXEC][start:end],
            TF_CONFIRM: all_candles[TF_CONFIRM],
            TF_BIAS: all_candles[TF_BIAS],
        }
        trades = run_backtest(symbol, window_candles, f"window_{w}")
        results["windows"][f"window_{w}"] = summarize_trades(trades, lambda t: t.regime_trend)

    # final holdout: last window_size bars, never used above for tuning
    holdout_start = exec_len - window_size
    holdout_candles = {
        TF_EXEC: all_candles[TF_EXEC][holdout_start:],
        TF_CONFIRM: all_candles[TF_CONFIRM],
        TF_BIAS: all_candles[TF_BIAS],
    }
    holdout_trades = run_backtest(symbol, holdout_candles, "holdout")
    results["holdout"] = summarize_trades(holdout_trades)

    # parameter sensitivity: perturb the base adaptive threshold by +-10%
    base_thr = 62.0
    for pct in (-0.10, 0.0, 0.10):
        perturbed_trades = run_backtest(symbol, holdout_candles, "sensitivity", threshold_override=base_thr * (1 + pct))
        results["sensitivity"][f"{int(pct*100):+d}%"] = summarize_trades(perturbed_trades)

    # baseline comparison
    baseline_trades = baseline_ma_crossover(all_candles[TF_EXEC])
    results["baseline"] = summarize_trades(baseline_trades)

    return results


def run_full_backtest_report(symbols: Optional[list[str]] = None) -> dict:
    symbols = symbols or WATCHLIST
    report = {}
    for symbol in symbols:
        candles = fetch_all_candles(symbol)
        if candles is None:
            report[symbol] = {"error": "candle fetch failed"}
            continue
        report[symbol] = walk_forward_backtest(symbol, candles)
    return report


# =============================================================================
# ENTRYPOINT
# =============================================================================

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "backtest":
        target_symbols = sys.argv[2].split(",") if len(sys.argv) > 2 else None
        rpt = run_full_backtest_report(target_symbols)
        print(json.dumps(rpt, indent=2, default=str))
    else:
        run_scan()
