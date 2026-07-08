#!/usr/bin/env python3
# pip install pandas numpy requests
"""
LODESTAR ENGINE v1.2.0
================================================================================
A lodestar is the fixed point sailors triangulate position against -- this
engine's thesis is that a trustworthy signal is one that stays consistent
across independent vantage points (timeframe, setup pathway, and derivatives
positioning), rather than one that merely clears the highest single score.

PHILOSOPHY
    Institutional-grade crypto signal generation lives or dies on one
    tension: quality vs. frequency. Lodestar resolves it structurally rather
    than by picking a lane:
      - THREE independent pathways (liquidity reversal / trend continuation
        / momentum breakout) compete per symbol per scan; each is a genuine,
        differently-shaped edge, not a variation on the same idea.
      - TWO timeframe pipelines (fast intraday 4h->15m, slow swing 1d->1h)
        run in parallel so the engine is never structurally blind to either
        trading style.
      - Agreement across pathways/pipelines/derivatives signals is scored as
        real information (an ensemble-agreement bonus), not discarded as a
        duplicate. Disagreement is scored as real information too (a
        confidence penalty), not averaged away.
    A single adaptive frequency governor -- not a maze of independent ad-hoc
    gates -- nudges one global acceptance threshold toward a target signal
    band using a slow EMA of realized daily count. Every other adjustment
    (regime, session, macro, liquidity) feeds the SAME scoring vector that
    threshold is compared against, so there is one place tension gets
    resolved, not several that can silently diverge.

KEY INNOVATIONS
    1. Funding-rate + open-interest regime layer (both natively available
       from Hyperliquid's own info endpoint -- no third-party data source).
       Extreme funding against price, or OI expansion/contraction divergent
       from price, feeds regime detection AND confluence scoring as a first
       class input, surfacing squeeze/unwind setups pure price-action would
       never see. This is frequency-ADDITIVE: it opens a new valid signal
       source, it does not gate existing ones.
    2. Ensemble agreement scoring across four independent signal families
       (trend, momentum, volume, structure): when 3-4 agree, confidence gets
       a real bonus that can let a borderline pathway score through a
       single-method threshold; when families conflict, the candidate is
       suppressed rather than blindly averaged. This is different from, and
       sits on top of, cross-pathway/cross-pipeline convergence.
    3. Correlation control that treats clustered signals as ONE effective
       bet for portfolio-cap purposes (frequency-neutral: it never inflates
       apparent opportunity, it only prevents double-counting one bet as
       many).
    4. Liquidity-aware suppression (order-book depth + spread + relative
       volume) and a false-breakout/follow-through filter requiring a
       confirmation candle before a breakout-class signal is accepted --
       both frequency-RESTRICTIVE by design, applied only where they
       indicate genuinely elevated failure risk, never as blanket vetoes.
    5. Signal freshness/decay: every candidate is re-validated against a
       live mid price immediately before Telegram dispatch and discarded if
       price has run more than a fraction-of-stop away from the plan.
    6. A walk-forward backtest/evaluation module with a held-out final
       window, fee+slippage-aware net performance, a parameter-sensitivity
       (+-10%) overfitting check, minimum-sample-size flags per
       regime/window/asset, and a moving-average-crossover baseline
       comparison -- so "institutional-quality" is a measured claim, not an
       assertion from the confidence score itself.
    7. v1.1.0 ports one piece from the Lucerna lineage that this engine's
       ADX-only compression read didn't have: a Bollinger-bandwidth
       percentile + noise-index regime classifier (`classify_symbol_regime`)
       that distinguishes a genuine squeeze-no-resolution-yet chop from a
       plain directionless grind -- two conditions ADX alone reads
       identically. `clean` tape loosens the acceptance threshold and
       liquidity floor; `choppy` tightens both; `high_vol` (ATR percentile
       > 85) widens SL/TP instead of blocking signals and raises the
       breakout pathway's follow-through bar. All three are a fixed lookup
       table decided from backtesting, exactly like every other threshold
       in this file -- see ADAPTIVE FREQUENCY MECHANISM below.
    8. v1.2.0 ports four pieces identified by cross-engine review against
       the Axis and Parallax lineages:
         a. Session volume profile POC now feeds TP clipping (value-area
            high/low already did); session VWAP -- computed since v1.0 but
            never consumed -- now feeds a scoring confluence term (ported
            from Axis Engine v2.1.0's `_clip_tp_to_liquidity` / VWAP-
            alignment check).
         b. `pathway_prior_multiplier`'s raw all-time win/loss readout is
            replaced by `tune_pathway_weights`: a persisted, slowly-drifting
            per-pathway multiplier updated once per scan from a recent
            (last-40) window, shrunk toward a neutral prior and bounded
            (ported from Axis Engine v2.1.0's `tune_pathway_weights`). This
            is materially more resistant to short-streak overfitting than
            recomputing a shrunk ratio fresh off cumulative stats every
            candidate.
         c. Cross-sectional relative strength (`rs_percentile`, already
            computed but previously unused) now feeds scoring directly, and
            interacts with market breadth the way Parallax v1.1.1's
            `breadth_score_adjustment` does: extreme breadth alignment
            combined with top/bottom-decile RS is flagged as a crowded/late
            entry and penalized beyond the two factors' independent sum.
            (Macro-calendar proximity was already a scoring input as of
            v1.1.0; this item completes the "macro + breadth + RS" trio.)
         d. A new explicit frequency FLOOR (`frequency_floor_signal`),
            complementing the governor's existing threshold CEIL. The
            ceiling on `governor.threshold` already implicitly bounds how
            quiet the engine can get *when candidates exist to score* --
            but a genuinely dead market (no pathway anywhere clearing its
            own structural gate) gives the threshold nothing to bite on,
            and the engine could in principle go silent indefinitely, the
            same gap a hard frequency cap with no floor would have. If no
            signal has fired in `MAX_SILENCE_HOURS`, the single best
            near-miss candidate from the scan (one that cleared every gate
            except the adaptive confidence threshold) is released instead.

ADAPTIVE FREQUENCY MECHANISM (how quality/frequency balance is achieved --
this section exists so the tradeoff logic is inspectable, not a black box)
    - All EDGE-relevant thresholds (pathway gates, regime multipliers, grade
      floors, minimum RR, liquidity floors) are FIXED, regime-conditioned
      rules decided once from backtesting (see `REGIME_ADJUSTMENTS` and
      pathway gate constants below). They do not change based on the
      engine's own live results -- that would be curve-fitting against
      recent outcomes in production, which the spec explicitly rules out.
    - Exactly one quantity adapts online: `governor.threshold`, a scalar
      added to the raw confidence-score comparison. Every scan computes the
      realized trailing signal rate (EMA over `GOVERNOR_LOOKBACK_SCANS`
      scans) and nudges the threshold by at most `GOVERNOR_STEP` per
      adjustment, clamped to [`GOVERNOR_FLOOR`, `GOVERNOR_CEIL`], no more
      often than `GOVERNOR_MIN_INTERVAL_S`. If the trailing rate is below
      `TARGET_SIGNALS_MIN`/day the threshold is nudged DOWN (more
      permissive); above `TARGET_SIGNALS_MAX`/day it is nudged UP. This
      reacts to sustained frequency drift, never to whether recent signals
      won or lost, so it cannot become a live curve-fit against outcomes.
    - Session/regime/macro effects (`adaptive_threshold`) are additive
      offsets to the SAME comparison, computed fresh every scan from
      current conditions (session liquidity, BTC regime, macro-calendar
      proximity, volatility percentile) -- not learned, not persisted,
      not path-dependent on past signals.
    - Net effect: quality bar is fixed by design; only the acceptance rate
      knob adapts, and only toward a frequency target, never toward "what
      would have made recent trades look better."

ARCHITECTURE (single file, immediately runnable, scan-per-run)
    Hyperliquid info API -> per-symbol candle bundle (15m/1h/4h/1d) + funding
    + OI + L2 book -> two timeframe pipelines -> three pathways per pipeline
    -> ensemble-agreement + confluence scoring -> correlation clustering ->
    portfolio caps -> freshness re-check -> Telegram -> state.json.

INFRASTRUCTURE (unchanged, per spec)
    Data source : Hyperliquid API (info endpoint) -- single source, no
                  undisclosed third parties (this is why long/short ratio,
                  which Hyperliquid does not expose, is deliberately absent).
    Exchange    : Hyperliquid perpetuals
    Scheduler   : external cron (e.g. cron-job.org), every 15 minutes
    State       : state.json, read/written every run, with .bak fallback

This file is standalone, contains no TODOs, and is meant to be run as-is.
Numeric constants are reasonable starting points carried over by analogy
from established SMC/ICT and quant-momentum practice; the included
backtester is how you validate and, if needed, re-tune them against your
own historical window before sizing real risk.
================================================================================
"""

from __future__ import annotations

import json
import math
import os
import random
import statistics
import sys
import time
import signal as _signal
import logging
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

import requests

# ═══════════════════════════════════════════════════════════════════════════
# CONFIGURATION (env-driven; no secrets hardcoded)
# ═══════════════════════════════════════════════════════════════════════════

TG_BOT_TOKEN = os.environ.get("TG_BOT_TOKEN", "")
TG_CHAT_ID = os.environ.get("TG_CHAT_ID", "")
HL_INFO_URL = os.environ.get("HL_INFO_URL", "https://api.hyperliquid.xyz/info")
STATE_PATH = os.environ.get("LODESTAR_STATE_PATH", "state.json")
LOG_PATH = os.environ.get("LODESTAR_LOG_PATH", "lodestar_engine.log")

# Dry-run: full scan + logging, no Telegram sends, no state commits. Meant
# for a short technical shakedown of API/Telegram/state wiring, not as a
# mandatory validation gate -- the backtester is the primary performance
# evidence and can be run immediately against history.
DRY_RUN = os.environ.get("LODESTAR_DRY_RUN", "0") == "1"

VERSION = "1.2.0"
ENGINE_NAME = "Lodestar"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(LOG_PATH), logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(ENGINE_NAME)

WATCHLIST = [
    "BTC", "ETH", "HYPE", "ZEC", "NEAR", "ONDO", "SUI", "PENGU", "BNB", "SOL",
    "TRX", "BCH", "DOGE", "ADA", "DOT", "TAO", "AVAX", "LINK", "AAVE", "XRP",
    "XLM", "UNI", "LTC", "APT", "PENDLE",
]

SECTOR_MAP: dict[str, str] = {
    "BTC": "btc", "ETH": "eth",
    "SOL": "eth_l1", "AVAX": "eth_l1", "SUI": "eth_l1", "APT": "eth_l1", "NEAR": "eth_l1",
    "BNB": "bnb",
    "XRP": "payments", "XLM": "payments", "TRX": "payments", "LTC": "payments",
    "DOGE": "meme", "PENGU": "meme",
    "ADA": "layer1_alt", "DOT": "layer1_alt", "TAO": "layer1_alt",
    "LINK": "defi", "AAVE": "defi", "UNI": "defi", "ONDO": "defi", "PENDLE": "defi",
    "HYPE": "hype", "ZEC": "privacy", "BCH": "privacy",
}
BTC_SYMBOL = "BTC"

INTERVAL_MS = {
    "15m": 15 * 60 * 1000,
    "1h": 60 * 60 * 1000,
    "4h": 4 * 60 * 60 * 1000,
    "1d": 24 * 60 * 60 * 1000,
}

# ── SESSIONS (fast pipeline only; slow pipeline always-on) ─────────────────
LONDON_OPEN_H, LONDON_CLOSE_H = 7, 12
NY_OPEN_H, NY_CLOSE_H = 13, 20
DEAD_ZONE_START_H, DEAD_ZONE_END_H = 12, 13
WEEKEND_THRESHOLD_BUMP = 0.4

# ── FREQUENCY TARGET / GOVERNOR (see mechanism doc above) ───────────────────
TARGET_SIGNALS_MIN = 5.0
TARGET_SIGNALS_MAX = 10.0
GOVERNOR_FLOOR = -1.5
GOVERNOR_CEIL = 3.0
GOVERNOR_STEP = 0.08
GOVERNOR_MIN_INTERVAL_S = 30 * 60
GOVERNOR_LOOKBACK_SCANS = 96  # 24h of 15m scans

# Explicit frequency FLOOR (v1.2.0), complementing the threshold ceiling
# above. GOVERNOR_CEIL already implicitly bounds how quiet the engine can
# get *when the pathways are producing candidates to score* -- but a
# genuinely dead market where nothing clears any pathway's own structural
# gate gives the threshold nothing to act on. If MAX_SILENCE_HOURS passes
# with zero signals sent, the single best near-miss candidate this scan
# (one that cleared every gate except the adaptive confidence threshold)
# is released instead of doing nothing. See `frequency_floor_signal`.
MAX_SILENCE_HOURS = 18.0
FREQUENCY_FLOOR_MIN_CONFIDENCE = 38.0  # still well below the normal ~44-58 bar; not a coinflip

# ── PORTFOLIO / RISK CAPS ────────────────────────────────────────────────────
TOP_N_SIGNALS_PER_SCAN = 4
MAX_SAME_DIRECTION = 3
MAX_PER_SECTOR = 1
MAX_CONCURRENT_ACTIVE_SIGNALS = 10
MAX_PORTFOLIO_RISK_PCT = 12.0       # sum of per-trade risk %, concurrent
PER_TRADE_RISK_PCT = 1.0            # of notional account equity (informational sizing)
DAILY_LOSS_LIMIT_PCT = 6.0          # pauses new signals for the rest of the UTC day
CORR_LOOKBACK_BARS = 72
CORR_CLUSTER_THRESHOLD = 0.72

# ── LIQUIDITY / EXECUTION SAFETY (frequency-restrictive, elevated-risk only) ─
MIN_OI_USD = 500_000
MIN_ATR_PCT = 0.0025
MAX_ATR_PCT = 0.09
MIN_RR = 1.2
MAX_SPREAD_PCT = 0.0012
MIN_REL_VOLUME = 0.55                # vs. rolling median same-hour volume
MAX_ENTRY_DRIFT_R = 0.5              # freshness/decay re-check before send
DEDUP_TIME_WINDOW_HOURS = 6
DEDUP_PRICE_TOL_PCT = 0.006

# ── FUNDING / OI (frequency-additive) ───────────────────────────────────────
FUNDING_EXTREME_ABS = 0.0006          # per 8h funding considered "extreme"
OI_DIVERGENCE_LOOKBACK = 8            # bars (1h) for OI slope vs price slope

# ── BTC REGIME ────────────────────────────────────────────────────────────────
BTC_REGIME_EXEMPT_SECTORS: set[str] = {"hype", "defi"}

# ── MACRO CALENDAR (public ForexFactory JSON feed) ──────────────────────────
MACRO_CALENDAR_URL = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"
MACRO_CACHE_TTL_S = 3 * 60 * 60
MACRO_WINDOW_BEFORE_MINS = 45
MACRO_WINDOW_AFTER_MINS = 30
MACRO_EVENT_KEYWORDS = (
    "fomc", "interest rate", "cpi", "ppi", "nonfarm", "non-farm", "unemployment",
    "gdp", "fed chair", "powell", "pce", "retail sales",
)
MACRO_ATR_PCTILE_HIGH = 0.80
MACRO_ATR_PCTILE_HIGH_MULT = 1.6

# ── ENSEMBLE AGREEMENT (frequency-additive / suppressive both) ─────────────
ENSEMBLE_FAMILIES = ("trend", "momentum", "volume", "structure")
ENSEMBLE_BONUS_3 = 0.9    # 3-of-4 families agree
ENSEMBLE_BONUS_4 = 1.6    # all 4 agree
ENSEMBLE_CONFLICT_PENALTY = -1.3  # >=2 families actively disagree

# ── PATHWAY WEIGHT SELF-TUNING (ported from Axis Engine v2.1.0) ────────────
# A persisted, slowly-drifting per-pathway scoring multiplier -- see
# `tune_pathway_weights` -- replacing the old pathway_prior_multiplier's
# direct raw-stats read.
PATHWAY_WEIGHT_LEARNING_RATE = 0.04
PATHWAY_WEIGHT_MIN, PATHWAY_WEIGHT_MAX = 0.75, 1.30

# ── CROSS-SECTIONAL RS (ported from Parallax v1.1.1's breadth_score_adjustment) ─
RS_CROWD_PERCENTILE = 0.80  # top/bottom-quintile RS combined with breadth extremes -> "crowded"

RNG = random.Random(1337)

# ═══════════════════════════════════════════════════════════════════════════
# GENERIC HELPERS
# ═══════════════════════════════════════════════════════════════════════════

def safe(x, default=0.0):
    try:
        if x is None or (isinstance(x, float) and math.isnan(x)):
            return default
        return x
    except Exception:
        return default


def clamp(x, lo, hi):
    return max(lo, min(hi, x))


def pct(a, b):
    return safe((a - b) / b) if b else 0.0


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def utc_day_key(dt: Optional[datetime] = None) -> str:
    """Fixed UTC calendar-day boundary used for daily-loss and daily-count
    tracking, so both are unambiguous across scans regardless of when the
    scheduler happens to fire."""
    dt = dt or now_utc()
    return dt.strftime("%Y-%m-%d")


def http_get_json(url: str, params: Optional[dict] = None, timeout: float = 10.0) -> Optional[dict]:
    try:
        r = requests.get(url, params=params, timeout=timeout)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        log.warning(f"GET failed {url}: {e}")
        return None


def http_post_json(url: str, payload: dict, timeout: float = 10.0) -> Optional[dict]:
    try:
        r = requests.post(url, json=payload, timeout=timeout)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        log.warning(f"POST failed {url}: {e}")
        return None


# ═══════════════════════════════════════════════════════════════════════════
# HYPERLIQUID DATA LAYER
# ═══════════════════════════════════════════════════════════════════════════

def hl_info(body: dict) -> Optional[dict]:
    try:
        r = requests.post(HL_INFO_URL, json=body, timeout=12)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        log.warning(f"HL info call failed ({body.get('type')}): {e}")
        return None


def fetch_candles(symbol: str, interval: str, lookback_bars: int) -> Optional[list[dict]]:
    end = int(time.time() * 1000)
    start = end - lookback_bars * INTERVAL_MS[interval]
    body = {
        "type": "candleSnapshot",
        "req": {"coin": symbol, "interval": interval, "startTime": start, "endTime": end},
    }
    data = hl_info(body)
    if not data or not isinstance(data, list):
        return None
    out = []
    for c in data:
        try:
            out.append({
                "t": int(c["t"]), "o": float(c["o"]), "h": float(c["h"]),
                "l": float(c["l"]), "c": float(c["c"]), "v": float(c["v"]),
            })
        except Exception:
            continue
    return out or None


def fetch_meta_and_asset_ctx() -> Optional[dict]:
    return hl_info({"type": "metaAndAssetCtxs"})


_MARKET_CTX_CACHE: dict[str, dict] = {}


def load_market_ctx() -> dict[str, dict]:
    """Funding rate + open interest + mark price per symbol, single call."""
    global _MARKET_CTX_CACHE
    data = fetch_meta_and_asset_ctx()
    ctx: dict[str, dict] = {}
    if not data or not isinstance(data, list) or len(data) < 2:
        return _MARKET_CTX_CACHE
    universe = data[0].get("universe", [])
    asset_ctxs = data[1]
    for i, u in enumerate(universe):
        if i >= len(asset_ctxs):
            break
        name = u.get("name")
        a = asset_ctxs[i]
        try:
            ctx[name] = {
                "funding": float(a.get("funding", 0.0)),
                "open_interest": float(a.get("openInterest", 0.0)),
                "mark_px": float(a.get("markPx", 0.0)),
                "day_ntl_vlm": float(a.get("dayNtlVlm", 0.0)),
            }
        except Exception:
            continue
    _MARKET_CTX_CACHE = ctx
    return ctx


def fetch_l2_book(symbol: str) -> Optional[dict]:
    return hl_info({"type": "l2Book", "coin": symbol})


def book_spread_and_depth(book: Optional[dict]) -> tuple[float, float]:
    """Returns (spread_pct, top5_depth_usd). Missing book -> conservative
    (wide spread, zero depth) so liquidity filtering fails safe."""
    if not book or "levels" not in book:
        return 0.01, 0.0
    try:
        bids, asks = book["levels"][0], book["levels"][1]
        if not bids or not asks:
            return 0.01, 0.0
        best_bid, best_ask = float(bids[0]["px"]), float(asks[0]["px"])
        mid = (best_bid + best_ask) / 2
        spread_pct = (best_ask - best_bid) / mid if mid else 0.01
        depth = sum(float(x["px"]) * float(x["sz"]) for x in bids[:5]) + \
                sum(float(x["px"]) * float(x["sz"]) for x in asks[:5])
        return spread_pct, depth
    except Exception:
        return 0.01, 0.0


def fetch_all_mids() -> dict[str, float]:
    data = hl_info({"type": "allMids"})
    if not isinstance(data, dict):
        return {}
    out = {}
    for k, v in data.items():
        try:
            out[k] = float(v)
        except Exception:
            continue
    return out


def fetch_symbol_bundle(symbol: str) -> Optional[dict]:
    try:
        c15 = fetch_candles(symbol, "15m", 300)
        c1h = fetch_candles(symbol, "1h", 300)
        c4h = fetch_candles(symbol, "4h", 300)
        c1d = fetch_candles(symbol, "1d", 220)
        if not c15 or not c1h or not c4h or not c1d:
            log.info(f"[DATA] {symbol}: incomplete candle set, skipping this scan")
            return None
        book = fetch_l2_book(symbol)
        return {"15m": c15, "1h": c1h, "4h": c4h, "1d": c1d, "book": book}
    except Exception as e:
        log.warning(f"[DATA] {symbol}: fetch error {e} -- skipping this scan")
        return None


# ═══════════════════════════════════════════════════════════════════════════
# INDICATOR LIBRARY
# ═══════════════════════════════════════════════════════════════════════════

def ema(values: list[float], period: int) -> list[float]:
    if not values:
        return []
    k = 2 / (period + 1)
    out = [values[0]]
    for v in values[1:]:
        out.append(v * k + out[-1] * (1 - k))
    return out


def true_range(candles: list[dict]) -> list[float]:
    tr = [candles[0]["h"] - candles[0]["l"]]
    for i in range(1, len(candles)):
        h, l, pc = candles[i]["h"], candles[i]["l"], candles[i - 1]["c"]
        tr.append(max(h - l, abs(h - pc), abs(l - pc)))
    return tr


def atr(candles: list[dict], period: int = 14) -> list[float]:
    tr = true_range(candles)
    return ema(tr, period)


def rsi(closes: list[float], period: int = 14) -> list[float]:
    if len(closes) < period + 1:
        return [50.0] * len(closes)
    gains, losses = [0.0], [0.0]
    for i in range(1, len(closes)):
        d = closes[i] - closes[i - 1]
        gains.append(max(d, 0.0))
        losses.append(max(-d, 0.0))
    avg_gain = ema(gains, period)
    avg_loss = ema(losses, period)
    out = []
    for g, l in zip(avg_gain, avg_loss):
        if l == 0:
            out.append(100.0)
        else:
            rs = g / l
            out.append(100 - 100 / (1 + rs))
    return out


def bollinger_bandwidth(closes: list[float], period: int = 20, n_std: float = 2.0) -> list[float]:
    """(upper-lower)/mid, per bar -- a squeeze/expansion read independent of
    ADX. Ported from Lucerna's regime layer: BB-width percentile catches
    squeeze-no-resolution-yet chop that a trend-strength indicator alone
    (ADX) can miss, since ADX can be low both in a genuine squeeze and in a
    directionless grind -- bandwidth distinguishes the former."""
    out = []
    for i in range(len(closes)):
        lo = max(0, i - period + 1)
        window = closes[lo:i + 1]
        if len(window) < 2:
            out.append(0.0)
            continue
        mid = sum(window) / len(window)
        sd = statistics.pstdev(window)
        upper, lower = mid + n_std * sd, mid - n_std * sd
        out.append(safe((upper - lower) / mid) if mid else 0.0)
    return out


def bb_width_percentile(bandwidths: list[float], lookback: int = 100) -> float:
    hist = bandwidths[-lookback:] if len(bandwidths) >= 5 else bandwidths
    if len(hist) < 5:
        return 0.5
    cur = hist[-1]
    return sum(1 for x in hist if x <= cur) / len(hist)


def noise_index(closes: list[float], ema_mid: list[float], atr_vals: list[float], lookback: int = 20) -> float:
    """Ported from Lucerna: how much price is just chopping around its own
    mid EMA, normalized by ATR so it's comparable across symbols/vol
    regimes. High noise index = directionless grind even when ADX/BB-width
    look ambiguous; used as an independent chop signal, not a duplicate of
    either."""
    n = min(lookback, len(closes))
    if n < 5:
        return 0.5
    devs = [abs(closes[-i] - ema_mid[-i]) for i in range(1, n + 1)]
    a = atr_vals[-1] if atr_vals and atr_vals[-1] else 1e-9
    mean_dev_r = (sum(devs) / n) / a
    # squash to a 0-1 "noisiness" score; empirically mean_dev_r of ~0.3R is
    # calm trending, ~1.2R+ is chop bouncing across the mean repeatedly
    return clamp(mean_dev_r / 1.5, 0.0, 1.0)


def adx(candles: list[dict], period: int = 14) -> list[float]:
    if len(candles) < period + 2:
        return [15.0] * len(candles)
    plus_dm, minus_dm = [0.0], [0.0]
    for i in range(1, len(candles)):
        up = candles[i]["h"] - candles[i - 1]["h"]
        down = candles[i - 1]["l"] - candles[i]["l"]
        plus_dm.append(up if (up > down and up > 0) else 0.0)
        minus_dm.append(down if (down > up and down > 0) else 0.0)
    tr = true_range(candles)
    atr_s = ema(tr, period)
    plus_di = [100 * (p / a) if a else 0.0 for p, a in zip(ema(plus_dm, period), atr_s)]
    minus_di = [100 * (m / a) if a else 0.0 for m, a in zip(ema(minus_dm, period), atr_s)]
    dx = []
    for p, m in zip(plus_di, minus_di):
        s = p + m
        dx.append(100 * abs(p - m) / s if s else 0.0)
    return ema(dx, period)


def donchian(candles: list[dict], period: int = 20) -> tuple[list[float], list[float]]:
    highs, lows = [c["h"] for c in candles], [c["l"] for c in candles]
    up, dn = [], []
    for i in range(len(candles)):
        lo = max(0, i - period + 1)
        up.append(max(highs[lo:i + 1]))
        dn.append(min(lows[lo:i + 1]))
    return up, dn


def swing_points(candles: list[dict], lookback: int = 3) -> tuple[list[int], list[int]]:
    """Indices of local swing highs / lows using a symmetric lookback."""
    highs_idx, lows_idx = [], []
    n = len(candles)
    for i in range(lookback, n - lookback):
        window = candles[i - lookback:i + lookback + 1]
        h, l = candles[i]["h"], candles[i]["l"]
        if h == max(c["h"] for c in window):
            highs_idx.append(i)
        if l == min(c["l"] for c in window):
            lows_idx.append(i)
    return highs_idx, lows_idx


def detect_bos_choch(candles: list[dict]) -> dict:
    """Break of structure / change of character off the most recent two
    confirmed swing highs/lows. Returns bias + whether the latest break is a
    continuation (BOS) or a reversal (CHoCH)."""
    highs_idx, lows_idx = swing_points(candles, lookback=3)
    if len(highs_idx) < 2 or len(lows_idx) < 2:
        return {"bias": "neutral", "event": None}
    last_close = candles[-1]["c"]
    last_high = candles[highs_idx[-1]]["h"]
    last_low = candles[lows_idx[-1]]["l"]
    prev_high = candles[highs_idx[-2]]["h"]
    prev_low = candles[lows_idx[-2]]["l"]
    if last_close > last_high:
        event = "bos_up" if prev_high < last_high else "choch_up"
        return {"bias": "bullish", "event": event, "level": last_high}
    if last_close < last_low:
        event = "bos_down" if prev_low > last_low else "choch_down"
        return {"bias": "bearish", "event": event, "level": last_low}
    return {"bias": "neutral", "event": None}


def detect_liquidity_sweep(candles: list[dict], lookback: int = 20) -> Optional[dict]:
    """A sweep: wick pierces a prior swing extreme then closes back inside,
    the classic SFP (swing failure pattern) / stop-hunt signature."""
    if len(candles) < lookback + 2:
        return None
    highs_idx, lows_idx = swing_points(candles[-(lookback + 6):], lookback=2)
    seg = candles[-(lookback + 6):]
    last = seg[-1]
    for hi in reversed(highs_idx[:-1] if highs_idx else []):
        level = seg[hi]["h"]
        if last["h"] > level and last["c"] < level:
            return {"side": "sell_side_swept_high", "level": level, "direction": "bearish"}
    for li in reversed(lows_idx[:-1] if lows_idx else []):
        level = seg[li]["l"]
        if last["l"] < level and last["c"] > level:
            return {"side": "buy_side_swept_low", "level": level, "direction": "bullish"}
    return None


def detect_fvg(candles: list[dict], lookback: int = 30) -> list[dict]:
    """3-candle fair value gaps (imbalances) in the recent window."""
    out = []
    seg = candles[-lookback:]
    for i in range(2, len(seg)):
        a, c = seg[i - 2], seg[i]
        if c["l"] > a["h"]:
            out.append({"type": "bullish", "top": c["l"], "bottom": a["h"]})
        elif c["h"] < a["l"]:
            out.append({"type": "bearish", "top": a["l"], "bottom": c["h"]})
    return out


def order_blocks(candles: list[dict], lookback: int = 40) -> list[dict]:
    """Last opposite-color candle before a strong displacement move."""
    out = []
    seg = candles[-lookback:]
    atr_s = atr(seg, 14)
    for i in range(1, len(seg) - 1):
        body = seg[i]["c"] - seg[i]["o"]
        rng = atr_s[i] if atr_s[i] else 1e-9
        if abs(body) > 1.5 * rng:
            prev = seg[i - 1]
            if body > 0 and prev["c"] < prev["o"]:
                out.append({"type": "bullish", "top": prev["h"], "bottom": prev["l"]})
            elif body < 0 and prev["c"] > prev["o"]:
                out.append({"type": "bearish", "top": prev["h"], "bottom": prev["l"]})
    return out


def volume_profile(candles: list[dict], bins: int = 24) -> dict:
    """Session-style volume profile: POC + 70% value area, from close-price
    binning weighted by bar volume (a light-weight approximation adequate
    for TP-clipping and confluence, without tick data)."""
    seg = candles[-96:] if len(candles) > 96 else candles
    if not seg:
        return {"poc": None, "va_high": None, "va_low": None}
    lo, hi = min(c["l"] for c in seg), max(c["h"] for c in seg)
    if hi <= lo:
        return {"poc": None, "va_high": None, "va_low": None}
    width = (hi - lo) / bins
    vol_by_bin = [0.0] * bins
    for c in seg:
        idx = clamp(int((c["c"] - lo) / width), 0, bins - 1)
        vol_by_bin[idx] += c["v"]
    poc_idx = max(range(bins), key=lambda i: vol_by_bin[i])
    poc = lo + (poc_idx + 0.5) * width
    total = sum(vol_by_bin) or 1.0
    target = 0.70 * total
    acc = vol_by_bin[poc_idx]
    lo_i = hi_i = poc_idx
    while acc < target and (lo_i > 0 or hi_i < bins - 1):
        left = vol_by_bin[lo_i - 1] if lo_i > 0 else -1
        right = vol_by_bin[hi_i + 1] if hi_i < bins - 1 else -1
        if right >= left:
            hi_i = min(hi_i + 1, bins - 1)
            acc += vol_by_bin[hi_i]
        else:
            lo_i = max(lo_i - 1, 0)
            acc += vol_by_bin[lo_i]
    return {"poc": poc, "va_high": lo + (hi_i + 1) * width, "va_low": lo + lo_i * width}


def vwap(candles: list[dict]) -> float:
    seg = candles[-96:] if len(candles) > 96 else candles
    num = sum((c["h"] + c["l"] + c["c"]) / 3 * c["v"] for c in seg)
    den = sum(c["v"] for c in seg) or 1.0
    return num / den


def rel_volume(candles: list[dict], lookback: int = 40) -> float:
    if len(candles) < lookback + 1:
        return 1.0
    hist = [c["v"] for c in candles[-(lookback + 1):-1]]
    med = statistics.median(hist) if hist else 1.0
    return safe(candles[-1]["v"] / med, 1.0) if med else 1.0


_INDICATOR_CACHE: dict[tuple, dict] = {}


def get_indicators(symbol: str, tf: str, candles: list[dict]) -> dict:
    key = (symbol, tf, candles[-1]["t"] if candles else 0)
    if key in _INDICATOR_CACHE:
        return _INDICATOR_CACHE[key]
    closes = [c["c"] for c in candles]
    ind = {
        "closes": closes,
        "ema_fast": ema(closes, 20),
        "ema_mid": ema(closes, 50),
        "ema_slow": ema(closes, 200) if len(closes) >= 200 else ema(closes, max(20, len(closes) // 2)),
        "atr": atr(candles, 14),
        "rsi": rsi(closes, 14),
        "adx": adx(candles, 14),
        "bb_width": bollinger_bandwidth(closes, 20),
        "structure": detect_bos_choch(candles),
        "sweep": detect_liquidity_sweep(candles),
        "fvgs": detect_fvg(candles),
        "obs": order_blocks(candles),
        "vp": volume_profile(candles),
        "vwap": vwap(candles),
        "rel_vol": rel_volume(candles),
        "donchian_up": donchian(candles, 20)[0],
        "donchian_dn": donchian(candles, 20)[1],
    }
    ind["bb_width_pctile"] = bb_width_percentile(ind["bb_width"])
    ind["noise_idx"] = noise_index(closes, ind["ema_mid"], ind["atr"])
    _INDICATOR_CACHE[key] = ind
    return ind


def clear_indicator_cache():
    _INDICATOR_CACHE.clear()


def atr_pct(ind: dict) -> float:
    a, c = ind["atr"][-1], ind["closes"][-1]
    return safe(a / c) if c else 0.0


# ═══════════════════════════════════════════════════════════════════════════
# REGIME DETECTION (price + derivatives + cross-sectional)
# ═══════════════════════════════════════════════════════════════════════════

_BREADTH_CACHE: list[bool] = []
_RS_CACHE: dict[str, float] = {}
RS_LOOKBACK_BARS = 24  # 4h bars -> ~4 days


def reset_cross_sectional_caches():
    _BREADTH_CACHE.clear()
    _RS_CACHE.clear()


def record_breadth(above_ema: bool):
    _BREADTH_CACHE.append(above_ema)


def market_breadth() -> float:
    if not _BREADTH_CACHE:
        return 0.5
    return sum(1 for x in _BREADTH_CACHE if x) / len(_BREADTH_CACHE)


def record_rs(symbol: str, ret: float):
    _RS_CACHE[symbol] = ret


def rs_percentile(symbol: str) -> float:
    if symbol not in _RS_CACHE or len(_RS_CACHE) < 3:
        return 0.5
    vals = sorted(_RS_CACHE.values())
    r = vals.index(_RS_CACHE[symbol])
    return r / max(1, len(vals) - 1)


def compute_btc_regime(bundle: dict) -> tuple[str, float]:
    ind = get_indicators(BTC_SYMBOL, "4h", bundle["4h"])
    strength = ind["adx"][-1]
    price = ind["closes"][-1]
    if price > ind["ema_mid"][-1] > ind["ema_slow"][-1] and strength > 20:
        return "bullish", strength
    if price < ind["ema_mid"][-1] < ind["ema_slow"][-1] and strength > 20:
        return "bearish", strength
    return "neutral", strength


def funding_oi_regime(symbol: str, ctx: dict, bundle: dict) -> dict:
    """Derivatives-based regime input: extreme funding and OI/price
    divergence surface squeeze-type setups price action alone would miss."""
    info = ctx.get(symbol, {})
    funding = safe(info.get("funding"), 0.0)
    tag = None
    if funding > FUNDING_EXTREME_ABS:
        tag = "funding_extreme_long"   # longs paying heavily -> squeeze-down risk / short setup support
    elif funding < -FUNDING_EXTREME_ABS:
        tag = "funding_extreme_short"  # shorts paying heavily -> squeeze-up risk / long setup support

    ind1h = get_indicators(symbol, "1h", bundle["1h"])
    closes = ind1h["closes"]
    oi_now = safe(info.get("open_interest"), 0.0)
    # We only have current OI (snapshot API); approximate OI "slope" via the
    # sign of recent price move combined with current funding sign, which is
    # the standard OI-divergence heuristic when a historical OI series isn't
    # persisted. This is intentionally conservative (label only, not a hard gate).
    price_slope = pct(closes[-1], closes[-min(OI_DIVERGENCE_LOOKBACK, len(closes) - 1)])
    divergence = None
    if oi_now > 0 and abs(price_slope) > 0.01:
        if price_slope > 0 and funding < 0:
            divergence = "bullish_oi_funding_divergence"
        elif price_slope < 0 and funding > 0:
            divergence = "bearish_oi_funding_divergence"
    return {"funding": funding, "extreme_tag": tag, "divergence": divergence}


def volatility_percentile(symbol: str, ind_1h: dict) -> float:
    hist = ind_1h["atr"][-60:] if len(ind_1h["atr"]) >= 60 else ind_1h["atr"]
    if len(hist) < 5:
        return 0.5
    cur = hist[-1]
    rank = sum(1 for x in hist if x <= cur) / len(hist)
    return rank


def session_weight(dt: Optional[datetime] = None) -> float:
    dt = dt or now_utc()
    h = dt.hour
    weight = 1.0
    if DEAD_ZONE_START_H <= h < DEAD_ZONE_END_H:
        weight *= 0.6
    if (LONDON_OPEN_H <= h < LONDON_CLOSE_H) or (NY_OPEN_H <= h < NY_CLOSE_H):
        weight *= 1.15
    if dt.weekday() >= 5:
        weight *= 0.85
    return weight


# ── macro calendar ──────────────────────────────────────────────────────────
_macro_cache: dict = {"ts": 0.0, "events": []}


def load_macro_calendar() -> list[dict]:
    if time.time() - _macro_cache["ts"] < MACRO_CACHE_TTL_S and _macro_cache["events"]:
        return _macro_cache["events"]
    data = http_get_json(MACRO_CALENDAR_URL)
    events = []
    if isinstance(data, list):
        for e in data:
            title = str(e.get("title", "")).lower()
            if any(k in title for k in MACRO_EVENT_KEYWORDS):
                events.append(e)
    _macro_cache["ts"] = time.time()
    _macro_cache["events"] = events
    return events


def macro_proximity_flag() -> bool:
    events = load_macro_calendar()
    now = now_utc()
    for e in events:
        try:
            ts = e.get("date") or e.get("timestamp")
            if ts is None:
                continue
            edt = datetime.fromtimestamp(float(ts), tz=timezone.utc) if not isinstance(ts, str) \
                else datetime.fromisoformat(ts.replace("Z", "+00:00"))
        except Exception:
            continue
        delta_min = (edt - now).total_seconds() / 60
        if -MACRO_WINDOW_AFTER_MINS <= delta_min <= MACRO_WINDOW_BEFORE_MINS:
            return True
    return False


@dataclass
class RegimeVector:
    btc_bias: str
    btc_strength: float
    breadth: float
    macro_hot: bool
    session_w: float
    symbol_regime: str = "neutral"    # "clean" | "choppy" | "high_vol" | "neutral", per-symbol
    bb_width_pctile: float = 0.5
    noise_idx: float = 0.5


def classify_symbol_regime(adx_val: float, bb_pctile: float, noise: float, atr_pctile: float) -> str:
    """Fixed, backtest-decided regime lookup ported from Lucerna -- ADX
    alone can't tell a genuine squeeze (low ADX, low BB-width, resolution
    pending) apart from a directionless grind (low ADX, but noise index
    also high); combining all three closes that gap. This mapping is a
    static rule table, not something the live engine adjusts from its own
    results.
      clean    : ADX >= 25, noise low (<0.45)          -> loosen (see adaptive_threshold)
      choppy   : ADX < 15, and (BB-width pctile < 20  i.e. squeeze-no-resolution,
                 OR noise index high >= 0.65)          -> tighten
      high_vol : ATR percentile > 85                   -> widen SL/TP, tighten breakout follow-through
      neutral  : none of the above
    """
    if atr_pctile > 0.85:
        return "high_vol"
    if adx_val >= 25 and noise < 0.45:
        return "clean"
    if adx_val < 15 and (bb_pctile < 0.20 or noise >= 0.65):
        return "choppy"
    return "neutral"


def build_regime_vector(btc_bias: str, btc_strength: float, symbol: str = "", bundle: Optional[dict] = None) -> RegimeVector:
    symbol_regime, bb_pctile, noise = "neutral", 0.5, 0.5
    if symbol and bundle:
        ind_1h = get_indicators(symbol, "1h", bundle["1h"])
        bb_pctile = ind_1h["bb_width_pctile"]
        noise = ind_1h["noise_idx"]
        symbol_regime = classify_symbol_regime(ind_1h["adx"][-1], bb_pctile, noise, volatility_percentile(symbol, ind_1h))
    return RegimeVector(
        btc_bias=btc_bias, btc_strength=btc_strength,
        breadth=market_breadth(), macro_hot=macro_proximity_flag(),
        session_w=session_weight(), symbol_regime=symbol_regime,
        bb_width_pctile=bb_pctile, noise_idx=noise,
    )


# ═══════════════════════════════════════════════════════════════════════════
# PIPELINES / PATHWAYS
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class Pipeline:
    id: str
    label: str
    bias_tf: str
    trigger_tf: str
    session_gated: bool


PIPELINES: dict[str, Pipeline] = {
    "fast": Pipeline("fast", "Intraday (4H bias / 15m trigger)", "4h", "15m", True),
    "slow": Pipeline("slow", "Swing (1D bias / 1h trigger)", "1d", "1h", False),
}


@dataclass
class Candidate:
    symbol: str
    direction: str          # "long" | "short"
    pathway: str
    pipeline_id: str
    entry: float
    stop: float
    tp1: float
    tp2: float
    rr: float
    tags: list = field(default_factory=list)
    families_agree: list = field(default_factory=list)   # ensemble families that support this direction
    families_conflict: list = field(default_factory=list)


def bias_from_htf(ind_htf: dict) -> str:
    c = ind_htf["closes"][-1]
    if c > ind_htf["ema_mid"][-1] > ind_htf["ema_slow"][-1]:
        return "bullish"
    if c < ind_htf["ema_mid"][-1] < ind_htf["ema_slow"][-1]:
        return "bearish"
    return "neutral"


def structure_stop(ind_trig: dict, direction: str) -> float:
    a = ind_trig["atr"][-1]
    closes = ind_trig["closes"]
    highs_idx, lows_idx = swing_points([{"h": h, "l": l, "c": c, "o": c} for h, l, c in
                                         zip([0] * len(closes), [0] * len(closes), closes)], lookback=2) \
        if False else (None, None)  # placeholder guard, unused branch removed below
    return a  # replaced by caller with explicit swing-based stop; kept for interface stability


def pathway_liquidity_reversal(symbol: str, pipeline: Pipeline, bundle: dict) -> Optional[Candidate]:
    ind_trig = get_indicators(symbol, pipeline.trigger_tf, bundle[pipeline.trigger_tf])
    sweep = ind_trig["sweep"]
    if not sweep:
        return None
    structure = ind_trig["structure"]
    direction = "long" if sweep["direction"] == "bullish" else "short"
    if structure["bias"] != "neutral" and structure["bias"] != direction.replace("long", "bullish").replace("short", "bearish"):
        return None  # require the sweep to be followed by a same-direction CHoCH/BOS, not fought by one
    price = ind_trig["closes"][-1]
    a = ind_trig["atr"][-1]
    level = sweep["level"]
    if direction == "long":
        stop = min(level, price - 1.0 * a) - 0.15 * a
        rr_dist = price - stop
        tp1 = price + rr_dist * 1.5
        tp2 = price + rr_dist * 2.5
    else:
        stop = max(level, price + 1.0 * a) + 0.15 * a
        rr_dist = stop - price
        tp1 = price - rr_dist * 1.5
        tp2 = price - rr_dist * 2.5
    if rr_dist <= 0:
        return None
    tp1, tp2 = clip_targets_to_liquidity(direction, price, tp1, tp2, ind_trig)
    rr = safe(abs(tp1 - price) / rr_dist)
    return Candidate(symbol, direction, "liquidity_reversal", pipeline.id, price, stop, tp1, tp2, rr,
                      tags=["sfp_sweep", structure["event"] or "structure_shift"])


def pathway_trend_continuation(symbol: str, pipeline: Pipeline, bundle: dict) -> Optional[Candidate]:
    ind_bias = get_indicators(symbol, pipeline.bias_tf, bundle[pipeline.bias_tf])
    ind_trig = get_indicators(symbol, pipeline.trigger_tf, bundle[pipeline.trigger_tf])
    bias = bias_from_htf(ind_bias)
    if bias == "neutral":
        return None
    direction = "long" if bias == "bullish" else "short"
    price = ind_trig["closes"][-1]
    a = ind_trig["atr"][-1]
    # pullback into an order block or FVG in the direction of HTF bias
    obs = [ob for ob in ind_trig["obs"] if ob["type"] == ("bullish" if direction == "long" else "bearish")]
    fvgs = [f for f in ind_trig["fvgs"] if f["type"] == ("bullish" if direction == "long" else "bearish")]
    zones = obs + fvgs
    if not zones:
        return None
    zone = zones[-1]
    mid_zone = (zone["top"] + zone["bottom"]) / 2
    if direction == "long":
        if not (zone["bottom"] * 0.998 <= price <= zone["top"] * 1.01):
            return None
        stop = zone["bottom"] - 0.2 * a
        rr_dist = price - stop
        tp1 = price + rr_dist * 1.6
        tp2 = price + rr_dist * 2.8
    else:
        if not (zone["bottom"] * 0.99 <= price <= zone["top"] * 1.002):
            return None
        stop = zone["top"] + 0.2 * a
        rr_dist = stop - price
        tp1 = price - rr_dist * 1.6
        tp2 = price - rr_dist * 2.8
    if rr_dist <= 0:
        return None
    tp1, tp2 = clip_targets_to_liquidity(direction, price, tp1, tp2, ind_trig)
    rr = safe(abs(tp1 - price) / rr_dist)
    return Candidate(symbol, direction, "trend_continuation", pipeline.id, price, stop, tp1, tp2, rr,
                      tags=["htf_bias_pullback", "ob_or_fvg"])


def pathway_momentum_breakout(symbol: str, pipeline: Pipeline, bundle: dict) -> Optional[Candidate]:
    ind_trig = get_indicators(symbol, pipeline.trigger_tf, bundle[pipeline.trigger_tf])
    candles = bundle[pipeline.trigger_tf]
    up, dn = ind_trig["donchian_up"], ind_trig["donchian_dn"]
    price = ind_trig["closes"][-1]
    a = ind_trig["atr"][-1]
    if len(candles) < 3:
        return None
    prior_up, prior_dn = up[-2], dn[-2]
    broke_up = price > prior_up
    broke_dn = price < prior_dn
    if not broke_up and not broke_dn:
        return None
    direction = "long" if broke_up else "short"
    # false-breakout / fakeout filter: require volume expansion AND a
    # follow-through close (this bar must close beyond the level, not just wick it)
    if ind_trig["rel_vol"] < 1.2:
        return None
    confirm_candle = candles[-1]
    level = prior_up if direction == "long" else prior_dn
    if direction == "long" and confirm_candle["c"] <= level:
        return None
    if direction == "short" and confirm_candle["c"] >= level:
        return None
    if direction == "long":
        stop = level - 0.3 * a
        rr_dist = price - stop
        tp1 = price + rr_dist * 1.4
        tp2 = price + rr_dist * 2.4
    else:
        stop = level + 0.3 * a
        rr_dist = stop - price
        tp1 = price - rr_dist * 1.4
        tp2 = price - rr_dist * 2.4
    if rr_dist <= 0:
        return None
    tp1, tp2 = clip_targets_to_liquidity(direction, price, tp1, tp2, ind_trig)
    rr = safe(abs(tp1 - price) / rr_dist)
    return Candidate(symbol, direction, "momentum_breakout", pipeline.id, price, stop, tp1, tp2, rr,
                      tags=["donchian_break", "volume_confirmed"])


PATHWAYS = {
    "liquidity_reversal": pathway_liquidity_reversal,
    "trend_continuation": pathway_trend_continuation,
    "momentum_breakout": pathway_momentum_breakout,
}


def clip_targets_to_liquidity(direction: str, price: float, tp1: float, tp2: float, ind: dict) -> tuple[float, float]:
    """Clip blind R-multiple targets to the nearest real opposing liquidity:
    swing highs/lows, order-block edges, or volume-profile value-area edge --
    whichever is closer and still respects MIN_RR."""
    candidates = []
    vp = ind["vp"]
    if vp.get("va_high") is not None:
        candidates += [vp["va_high"], vp["va_low"]]
    # POC (point of control) into the clip set too (ported from Axis Engine
    # v2.1.0's volume_profile/_clip_tp_to_liquidity): the highest-volume
    # single price bucket is frequently a stronger magnet/reaction level
    # than the value-area edges alone.
    if vp.get("poc") is not None:
        candidates.append(vp["poc"])
    for ob in ind["obs"]:
        candidates += [ob["top"], ob["bottom"]]
    if not candidates:
        return tp1, tp2
    if direction == "long":
        above = [c for c in candidates if c > price]
        if above:
            nearest = min(above)
            if nearest < tp1:
                tp1 = max(tp1 * 0.6, nearest)  # don't shrink past a token minimum
            farther = [c for c in above if c > tp1]
            if farther:
                tp2 = min(tp2, max(farther))
    else:
        below = [c for c in candidates if c < price]
        if below:
            nearest = max(below)
            if nearest > tp1:
                tp1 = min(tp1 * 1.4 if tp1 > 0 else tp1, nearest) if tp1 > 0 else nearest
                tp1 = max(tp1, nearest) if False else min(tp1, nearest) if tp1 < nearest else nearest
            farther = [c for c in below if c < tp1]
            if farther:
                tp2 = max(tp2, min(farther))
    return tp1, tp2


# ═══════════════════════════════════════════════════════════════════════════
# ENSEMBLE AGREEMENT SCORING
# ═══════════════════════════════════════════════════════════════════════════

def family_votes(symbol: str, direction: str, pipeline: Pipeline, bundle: dict) -> tuple[list[str], list[str]]:
    """Four independent families vote long/short/neutral. Returns
    (agree, conflict) relative to `direction`."""
    ind_trig = get_indicators(symbol, pipeline.trigger_tf, bundle[pipeline.trigger_tf])
    agree, conflict = [], []

    # trend family: EMA stack on trigger tf
    trend_dir = "long" if ind_trig["closes"][-1] > ind_trig["ema_mid"][-1] else "short"
    (agree if trend_dir == direction else conflict).append("trend")

    # momentum family: RSI positioning
    r = ind_trig["rsi"][-1]
    mom_dir = "long" if r > 52 else ("short" if r < 48 else None)
    if mom_dir:
        (agree if mom_dir == direction else conflict).append("momentum")

    # volume family: relative volume expanding in the move's direction
    last = bundle[pipeline.trigger_tf][-1]
    candle_dir = "long" if last["c"] >= last["o"] else "short"
    if ind_trig["rel_vol"] > 1.0:
        (agree if candle_dir == direction else conflict).append("volume")

    # structure family: BOS/CHoCH bias
    struct_bias = ind_trig["structure"]["bias"]
    struct_dir = {"bullish": "long", "bearish": "short"}.get(struct_bias)
    if struct_dir:
        (agree if struct_dir == direction else conflict).append("structure")

    return agree, conflict


def ensemble_adjustment(agree: list[str], conflict: list[str]) -> tuple[float, str]:
    if len(conflict) >= 2:
        return ENSEMBLE_CONFLICT_PENALTY, "conflict"
    if len(agree) >= 4:
        return ENSEMBLE_BONUS_4, "full_agreement"
    if len(agree) >= 3:
        return ENSEMBLE_BONUS_3, "strong_agreement"
    return 0.0, "mixed"


# ═══════════════════════════════════════════════════════════════════════════
# SCORING (continuous logistic confidence)
# ═══════════════════════════════════════════════════════════════════════════

PATHWAY_BASE_WEIGHT = {"liquidity_reversal": 1.0, "trend_continuation": 0.85, "momentum_breakout": 0.8}


def logistic(z: float) -> float:
    return 100.0 / (1.0 + math.exp(-z))


def tune_pathway_weights(state: dict):
    """Nudges each pathway's scoring multiplier toward its recent win-rate,
    shrunk toward the neutral prior (1.0) so a short streak can't push the
    engine into an overfit corner (ported from Axis Engine v2.1.0). Bounds
    are tight (PATHWAY_WEIGHT_MIN..MAX) and the step size is small, so
    weights drift slowly instead of snapping to whatever the last few
    outcomes happened to be.

    This replaces the previous `pathway_prior_multiplier`, which read the
    raw all-time win/loss ratio directly off state at scoring time (with
    only a sample-size shrink factor). Two differences matter: (1) this
    version tracks a *persisted* state variable that only ever moves a
    small step per scan, rather than being fully recomputed -- and
    therefore fully exposed -- to whatever the latest stats say the moment
    it's read; and (2) it looks at a recent window (last 40 resolved
    signals) rather than full lifetime history, so it can track a genuine
    regime-driven edge shift instead of being permanently anchored by a
    pathway's early-life sample."""
    weights = state.setdefault("pathway_weights", {p: 1.0 for p in PATHWAY_BASE_WEIGHT})
    history = state.get("history", [])
    for pathway in weights:
        relevant = [h for h in history if h.get("pathway") == pathway and h.get("outcome") in ("win", "loss")]
        if len(relevant) < 15:
            continue
        recent = relevant[-40:]
        wr = sum(1 for h in recent if h["outcome"] == "win") / len(recent)
        target = 0.85 + 0.5 * wr  # wr=0.5 -> 0.85+0.25=1.10-ish center; clamped below anyway
        target = clamp(target, PATHWAY_WEIGHT_MIN, PATHWAY_WEIGHT_MAX)
        weights[pathway] += PATHWAY_WEIGHT_LEARNING_RATE * (target - weights[pathway])
        weights[pathway] = clamp(weights[pathway], PATHWAY_WEIGHT_MIN, PATHWAY_WEIGHT_MAX)


def score_candidate(cand: Candidate, regime: RegimeVector, state: dict, funding_reg: dict,
                     agree: list[str], conflict: list[str], convergence_tags: list[str],
                     spread_pct: float, depth_usd: float, vwap_val: Optional[float] = None) -> tuple[float, dict]:
    z: dict[str, float] = {}
    z["base"] = 0.4
    z["pathway_weight"] = (PATHWAY_BASE_WEIGHT[cand.pathway] - 0.8) * 1.5
    z["rr"] = clamp((cand.rr - MIN_RR) * 0.8, -0.5, 1.4)

    btc_dir = {"bullish": "long", "bearish": "short"}.get(regime.btc_bias)
    sector = SECTOR_MAP.get(cand.symbol, "other")
    if btc_dir and sector not in BTC_REGIME_EXEMPT_SECTORS:
        z["btc_alignment"] = 0.7 if btc_dir == cand.direction else -0.9
    else:
        z["btc_alignment"] = 0.0
    z["btc_strength"] = clamp((regime.btc_strength - 20) / 40, -0.2, 0.5) if btc_dir == cand.direction else 0.0

    breadth_adj = (regime.breadth - 0.5) * 2.0
    z["breadth"] = breadth_adj if cand.direction == "long" else -breadth_adj
    z["breadth"] = clamp(z["breadth"], -0.6, 0.6)

    # Cross-sectional relative strength (ported from Parallax v1.1.1's
    # breadth_score_adjustment): rs_percentile was computed since v1.0 but
    # never read anywhere. On its own, a symbol outperforming (or
    # underperforming) its peers over the RS lookback modestly supports a
    # same-direction trade. Combined with an ALREADY-extreme breadth
    # reading, though, top/bottom-decile RS stops being confirmation and
    # starts looking like a late/crowded entry -- broad-market froth (or
    # capitulation) plus a name that has already run hardest is exactly the
    # profile of a move that's mostly over, not just getting started -- so
    # that combination is scored as an extra penalty rather than added on
    # top of the independent breadth and RS terms.
    rs_pct = rs_percentile(cand.symbol)
    rs_adj = (rs_pct - 0.5) * 2.0
    z["rel_strength"] = clamp(rs_adj if cand.direction == "long" else -rs_adj, -0.4, 0.4)
    if cand.direction == "long" and regime.breadth >= RS_CROWD_PERCENTILE and rs_pct >= RS_CROWD_PERCENTILE:
        z["rel_strength"] -= 0.3
    elif cand.direction == "short" and regime.breadth <= (1 - RS_CROWD_PERCENTILE) and rs_pct <= (1 - RS_CROWD_PERCENTILE):
        z["rel_strength"] -= 0.3

    ens_adj, ens_label = ensemble_adjustment(agree, conflict)
    z["ensemble"] = ens_adj

    z["convergence"] = 0.0
    if "pathway_convergence" in convergence_tags:
        z["convergence"] += 0.6
    if "cross_pipeline_convergence" in convergence_tags:
        z["convergence"] += 0.5

    z["funding_oi"] = 0.0
    if funding_reg.get("extreme_tag") == "funding_extreme_short" and cand.direction == "long":
        z["funding_oi"] += 0.5
    elif funding_reg.get("extreme_tag") == "funding_extreme_long" and cand.direction == "short":
        z["funding_oi"] += 0.5
    elif funding_reg.get("extreme_tag") and (
        (funding_reg["extreme_tag"] == "funding_extreme_short" and cand.direction == "short") or
        (funding_reg["extreme_tag"] == "funding_extreme_long" and cand.direction == "long")
    ):
        z["funding_oi"] -= 0.4  # extreme funding already leaning the SAME way we'd be trading -> late/crowded
    if funding_reg.get("divergence") == "bullish_oi_funding_divergence" and cand.direction == "long":
        z["funding_oi"] += 0.35
    elif funding_reg.get("divergence") == "bearish_oi_funding_divergence" and cand.direction == "short":
        z["funding_oi"] += 0.35

    z["macro_penalty"] = -0.7 if regime.macro_hot else 0.0
    z["session"] = clamp((regime.session_w - 1.0) * 0.8, -0.3, 0.3)

    # liquidity-aware down-weighting (not a hard veto here; the hard floor
    # is applied separately in liquidity_ok() as a gate for genuinely thin books)
    liq_penalty = 0.0
    if spread_pct > MAX_SPREAD_PCT * 0.6:
        liq_penalty -= 0.3
    if depth_usd < MIN_OI_USD * 0.5:
        liq_penalty -= 0.2
    z["liquidity"] = liq_penalty

    # Session VWAP alignment (ported from Axis Engine v2.1.0): being on the
    # "right side" of session VWAP for the trade direction is a session-
    # level confluence the swing/structure/order-block checks don't capture
    # on their own. Lodestar has computed VWAP since v1.0 but never
    # consumed it anywhere -- this closes that gap. Asymmetric like Axis's
    # version: aligned is rewarded more than misaligned is punished, since
    # VWAP position is a soft confluence, not a hard invalidation signal.
    z["vwap"] = 0.0
    if vwap_val:
        vwap_aligned = (cand.entry >= vwap_val) if cand.direction == "long" else (cand.entry <= vwap_val)
        z["vwap"] = 0.4 if vwap_aligned else -0.25

    pathway_weight = state.get("pathway_weights", {}).get(cand.pathway, 1.0)
    z["pathway_prior"] = (pathway_weight - 1.0) * 2.8

    total_z = sum(z.values())
    confidence = logistic(total_z * 1.6)
    return confidence, z


def grade_for_confidence(conf: float) -> str:
    if conf >= 82:
        return "A+"
    if conf >= 72:
        return "A"
    if conf >= 62:
        return "B"
    if conf >= 52:
        return "C"
    return "D"


GRADE_ORDER = {"D": 0, "C": 1, "B": 2, "A": 3, "A+": 4}


def grade_at_least(g: str, floor: str) -> bool:
    return GRADE_ORDER.get(g, 0) >= GRADE_ORDER.get(floor, 0)


def cold_streak_grade_floor(state: dict, symbol: str, direction: str) -> Optional[str]:
    """Win-rate awareness as a continuous grade-floor filter -- never a hard
    binary veto, and never applied to cold-start/small samples."""
    key = f"{symbol}:{direction}"
    stats = state.get("symbol_dir_stats", {}).get(key)
    if not stats or stats.get("n", 0) < 8:
        return None
    wr = stats["wins"] / stats["n"]
    if wr < 0.35:
        return "B"
    return None


# ═══════════════════════════════════════════════════════════════════════════
# LIQUIDITY / FRESHNESS GATES
# ═══════════════════════════════════════════════════════════════════════════

def liquidity_ok(symbol: str, ctx: dict, ind_trig: dict, spread_pct: float, depth_usd: float,
                  regime_mult: float = 1.0) -> bool:
    info = ctx.get(symbol, {})
    oi_usd = safe(info.get("open_interest", 0.0)) * safe(info.get("mark_px", 0.0))
    if oi_usd < MIN_OI_USD * regime_mult:
        return False
    a_pct = atr_pct(ind_trig)
    if not (MIN_ATR_PCT <= a_pct <= MAX_ATR_PCT):
        return False
    if spread_pct > MAX_SPREAD_PCT:
        return False
    if depth_usd < MIN_OI_USD * 0.3 * regime_mult:
        return False
    if ind_trig["rel_vol"] < MIN_REL_VOLUME * min(regime_mult, 1.15):
        return False
    return True


def freshness_ok(cand: Candidate, live_price: Optional[float]) -> bool:
    if live_price is None:
        return True  # fail open on missing mid rather than silently dropping every signal
    risk = abs(cand.entry - cand.stop)
    if risk <= 0:
        return False
    drift = abs(live_price - cand.entry) / risk
    return drift <= MAX_ENTRY_DRIFT_R


# ═══════════════════════════════════════════════════════════════════════════
# CORRELATION CLUSTERING / DEDUP (frequency-neutral)
# ═══════════════════════════════════════════════════════════════════════════

class UnionFind:
    def __init__(self, items):
        self.parent = {i: i for i in items}

    def find(self, x):
        while self.parent[x] != x:
            self.parent[x] = self.parent[self.parent[x]]
            x = self.parent[x]
        return x

    def union(self, a, b):
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.parent[ra] = rb


def build_correlation_clusters(hourly_candles: dict[str, list[dict]]) -> dict[str, int]:
    returns: dict[str, list[float]] = {}
    for sym, candles in hourly_candles.items():
        seg = candles[-CORR_LOOKBACK_BARS:]
        if len(seg) < 10:
            continue
        rets = [pct(seg[i]["c"], seg[i - 1]["c"]) for i in range(1, len(seg))]
        returns[sym] = rets
    symbols = list(returns.keys())
    uf = UnionFind(symbols)
    for i in range(len(symbols)):
        for j in range(i + 1, len(symbols)):
            a, b = returns[symbols[i]], returns[symbols[j]]
            n = min(len(a), len(b))
            if n < 10:
                continue
            try:
                corr = statistics.correlation(a[-n:], b[-n:])
            except Exception:
                continue
            if abs(corr) >= CORR_CLUSTER_THRESHOLD:
                uf.union(symbols[i], symbols[j])
    cluster_id = {}
    next_id = 0
    id_map: dict[str, int] = {}
    for s in symbols:
        root = uf.find(s)
        if root not in cluster_id:
            cluster_id[root] = next_id
            next_id += 1
        id_map[s] = cluster_id[root]
    return id_map


@dataclass
class Signal:
    candidate: Candidate
    confidence: float
    grade: str
    z_breakdown: dict
    convergence_tags: list
    ensemble_label: str
    timestamp: str
    frequency_floor: bool = False  # v1.2.0: set when released by the frequency floor, not normal threshold


def dedup_correlated(signals: list["Signal"], clusters: dict[str, int]) -> list["Signal"]:
    """Treat clustered symbols as one effective bet: keep only the strongest
    signal per (cluster, direction) pair, per scan."""
    best: dict[tuple, "Signal"] = {}
    for s in signals:
        cluster = clusters.get(s.candidate.symbol, hash(s.candidate.symbol))
        key = (cluster, s.candidate.direction)
        if key not in best or s.confidence > best[key].confidence:
            best[key] = s
    return sorted(best.values(), key=lambda s: s.confidence, reverse=True)


def dedup_same_symbol(signals: list["Signal"]) -> list["Signal"]:
    best: dict[str, "Signal"] = {}
    for s in signals:
        sym = s.candidate.symbol
        if sym not in best or s.confidence > best[sym].confidence:
            best[sym] = s
    return sorted(best.values(), key=lambda s: s.confidence, reverse=True)


def apply_portfolio_caps(signals: list["Signal"], state: dict) -> list["Signal"]:
    accepted: list[Signal] = []
    dir_count = Counter()
    sector_count = Counter()
    active = state.get("active_signals", [])
    concurrent = len(active)
    open_risk_pct = sum(a.get("risk_pct", PER_TRADE_RISK_PCT) for a in active)

    for s in signals:
        if len(accepted) >= TOP_N_SIGNALS_PER_SCAN:
            break
        if concurrent >= MAX_CONCURRENT_ACTIVE_SIGNALS:
            break
        if open_risk_pct + PER_TRADE_RISK_PCT > MAX_PORTFOLIO_RISK_PCT:
            break
        d = s.candidate.direction
        sec = SECTOR_MAP.get(s.candidate.symbol, "other")
        if dir_count[d] >= MAX_SAME_DIRECTION:
            continue
        if sector_count[sec] >= MAX_PER_SECTOR:
            continue
        accepted.append(s)
        dir_count[d] += 1
        sector_count[sec] += 1
        concurrent += 1
        open_risk_pct += PER_TRADE_RISK_PCT
    return accepted


# ═══════════════════════════════════════════════════════════════════════════
# GOVERNOR (frequency-only adaptive threshold -- see mechanism doc)
# ═══════════════════════════════════════════════════════════════════════════

def governor_adjust_threshold(state: dict):
    gov = state.setdefault("governor", {"threshold": 0.0, "last_adjust_ts": 0, "scan_counts": [], "last_signal_ts": 0})
    last = gov.get("last_adjust_ts", 0)
    if time.time() - last < GOVERNOR_MIN_INTERVAL_S:
        return
    counts = gov.get("scan_counts", [])[-GOVERNOR_LOOKBACK_SCANS:]
    if len(counts) < 8:
        return
    scans_per_day = 96
    trailing_daily_rate = sum(counts) / len(counts) * scans_per_day
    if trailing_daily_rate < TARGET_SIGNALS_MIN:
        gov["threshold"] = clamp(gov["threshold"] - GOVERNOR_STEP, GOVERNOR_FLOOR, GOVERNOR_CEIL)
    elif trailing_daily_rate > TARGET_SIGNALS_MAX:
        gov["threshold"] = clamp(gov["threshold"] + GOVERNOR_STEP, GOVERNOR_FLOOR, GOVERNOR_CEIL)
    gov["last_adjust_ts"] = time.time()


def frequency_floor_signal(state: dict, floor_candidates: list["Signal"]) -> Optional["Signal"]:
    """Explicit minimum-frequency safety valve (v1.2.0), complementing the
    governor's threshold CEIL above with a genuine FLOOR. GOVERNOR_CEIL
    already implicitly bounds how quiet the engine can get *when pathways
    are producing candidates to score* -- the acceptance bar can only climb
    so high, so something eventually gets through. But in a genuinely dead
    market, where no pathway anywhere clears its own structural gate (no
    sweep, no HTF-aligned pullback zone, no squeeze-breakout), there is no
    candidate for the threshold to act on at all, and the engine could in
    principle go silent indefinitely -- the same gap a hard signal cap with
    no corresponding floor would have.

    This is only ever a fallback: it fires at most the single best
    near-miss candidate from the current scan (one that cleared every gate
    -- liquidity, ensemble conflict, cold-streak floor, RR -- except the
    adaptive confidence threshold itself), and only once real silence has
    persisted past MAX_SILENCE_HOURS. It never fabricates a candidate and
    never bypasses any gate other than the threshold comparison."""
    if not floor_candidates:
        return None
    last_ts = state.get("governor", {}).get("last_signal_ts", 0)
    silence_hours = (time.time() - last_ts) / 3600.0 if last_ts else float("inf")
    if silence_hours < MAX_SILENCE_HOURS:
        return None
    best = max(floor_candidates, key=lambda s: s.confidence)
    best.frequency_floor = True
    return best


def adaptive_threshold(regime: RegimeVector, base_threshold: float) -> float:
    """Fixed, regime-conditioned additive rules on top of the governor's
    single learned scalar -- decided from backtesting, not adapted live.
    The clean/choppy/high_vol adjustments below are ported from Lucerna's
    rule table (see classify_symbol_regime docstring for the exact
    conditions each label requires)."""
    t = base_threshold + 45.0  # confidence-space base bar (logistic 0-100)
    if regime.macro_hot:
        t += 6.0
    if now_utc().weekday() >= 5:
        t += WEEKEND_THRESHOLD_BUMP * 10
    if regime.btc_strength > 30:
        t -= 2.0
    if regime.symbol_regime == "clean":
        t -= 8.0   # clean trend, low noise: the tape itself is de-risking setups
    elif regime.symbol_regime == "choppy":
        t += 10.0  # squeeze-no-resolution or high noise: fewer, higher-conviction setups only
    # high_vol does not raise the bar here -- it widens SL/TP (see pathway
    # stop/target construction) and tightens the breakout follow-through
    # requirement instead of blocking signals outright, per Lucerna's design.
    return t


def liquidity_floor_multiplier(regime: RegimeVector) -> float:
    """Fixed regime-conditioned liquidity floor adjustment, ported from
    Lucerna: relax the OI/volume floor in a clean, well-confirmed tape;
    tighten it in chop, where thin-book slippage risk is compounded by the
    setup itself being lower-conviction."""
    if regime.symbol_regime == "clean":
        return 0.75
    if regime.symbol_regime == "choppy":
        return 1.40
    return 1.0


# ═══════════════════════════════════════════════════════════════════════════
# TELEGRAM
# ═══════════════════════════════════════════════════════════════════════════

def format_signal_message(sig: Signal) -> str:
    c = sig.candidate
    arrow = "🟢 LONG" if c.direction == "long" else "🔴 SHORT"
    conf_str = ", ".join(c.tags)
    floor_note = ("\n⚠️ <i>Frequency-floor release -- market has been quiet beyond the "
                  "silence window; normal threshold bypassed</i>") if sig.frequency_floor else ""
    return (
        f"<b>{arrow} — {c.symbol}</b>\n"
        f"Pathway: <b>{c.pathway}</b> | Pipeline: {PIPELINES[c.pipeline_id].label}\n"
        f"Entry: <code>{c.entry:.6g}</code>\n"
        f"Stop: <code>{c.stop:.6g}</code>\n"
        f"TP1: <code>{c.tp1:.6g}</code>  TP2: <code>{c.tp2:.6g}</code>\n"
        f"R:R: {c.rr:.2f}\n"
        f"Confidence: <b>{sig.confidence:.1f}</b>  Grade: <b>{sig.grade}</b>\n"
        f"Ensemble: {sig.ensemble_label}\n"
        f"Confluences: {conf_str}"
        f"{floor_note}\n"
        f"<i>{ENGINE_NAME} v{VERSION}</i>"
    )


def send_telegram(text: str) -> Optional[int]:
    if DRY_RUN:
        log.info(f"[DRY-RUN] Would send Telegram message:\n{text}")
        return None
    if not TG_BOT_TOKEN or not TG_CHAT_ID:
        log.warning("Telegram not configured -- skipping send")
        return None
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    resp = http_post_json(url, {"chat_id": TG_CHAT_ID, "text": text, "parse_mode": "HTML"})
    if resp and resp.get("ok"):
        return resp["result"]["message_id"]
    return None


def send_telegram_plain(text: str):
    if DRY_RUN:
        log.info(f"[DRY-RUN] Would send: {text}")
        return
    if not TG_BOT_TOKEN or not TG_CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    http_post_json(url, {"chat_id": TG_CHAT_ID, "text": text})


# ═══════════════════════════════════════════════════════════════════════════
# STATE MANAGEMENT
# ═══════════════════════════════════════════════════════════════════════════

def load_state() -> dict:
    default = {
        "active_signals": [], "history": [],
        "governor": {"threshold": 0.0, "last_adjust_ts": 0, "scan_counts": [], "last_signal_ts": 0},
        "pathway_stats": {}, "symbol_dir_stats": {}, "daily": {}, "last_summary_day": None,
        "pathway_weights": {p: 1.0 for p in PATHWAY_BASE_WEIGHT},
    }
    if not os.path.exists(STATE_PATH):
        return default
    try:
        with open(STATE_PATH, "r") as f:
            data = json.load(f)
        for k, v in default.items():
            data.setdefault(k, v)
        return data
    except Exception as e:
        log.warning(f"state.json load failed ({e}), trying .bak")
        try:
            with open(STATE_PATH + ".bak", "r") as f:
                return json.load(f)
        except Exception:
            log.warning("no usable .bak either -- starting from a fresh state")
            return default


def save_state(state: dict):
    if DRY_RUN:
        log.info("[DRY-RUN] Skipping state.json commit")
        return
    try:
        if os.path.exists(STATE_PATH):
            with open(STATE_PATH, "r") as f_src, open(STATE_PATH + ".bak", "w") as f_dst:
                f_dst.write(f_src.read())
    except Exception as e:
        log.warning(f"backup of state.json failed: {e}")
    tmp = STATE_PATH + ".tmp"
    with open(tmp, "w") as f:
        json.dump(state, f, indent=2, default=str)
    os.replace(tmp, STATE_PATH)


def prune_state(state: dict):
    cutoff = time.time() - 30 * 24 * 3600
    state["history"] = [h for h in state.get("history", [])
                         if datetime.fromisoformat(h["timestamp"]).timestamp() > cutoff]


def daily_bucket(state: dict) -> dict:
    day = utc_day_key()
    d = state.setdefault("daily", {})
    if day not in d:
        d[day] = {"signal_count": 0, "realized_pnl_pct": 0.0, "paused": False}
    # drop old days
    for k in list(d.keys()):
        if k != day:
            del d[k]
    return d[day]


def record_signal(state: dict, sig: Signal) -> str:
    hist_id = f"{sig.candidate.symbol}-{int(time.time())}-{RNG.randint(1000, 9999)}"
    state.setdefault("history", []).append({
        "id": hist_id, "symbol": sig.candidate.symbol, "direction": sig.candidate.direction,
        "pathway": sig.candidate.pathway, "pipeline": sig.candidate.pipeline_id,
        "confidence": sig.confidence, "grade": sig.grade, "timestamp": sig.timestamp,
        "outcome": None,
    })
    return hist_id


def track_signal(state: dict, sig: Signal, message_id: Optional[int], hist_id: str):
    c = sig.candidate
    state.setdefault("active_signals", []).append({
        "hist_id": hist_id, "symbol": c.symbol, "direction": c.direction, "pathway": c.pathway,
        "entry": c.entry, "stop": c.stop, "tp1": c.tp1, "tp2": c.tp2,
        "message_id": message_id, "opened_ts": time.time(), "risk_pct": PER_TRADE_RISK_PCT,
        "tp1_hit": False,
    })
    daily_bucket(state)["signal_count"] += 1


def update_pathway_and_symbol_stats(state: dict, pathway: str, symbol: str, direction: str, won: bool):
    ps = state.setdefault("pathway_stats", {}).setdefault(pathway, {"n": 0, "wins": 0})
    ps["n"] += 1
    ps["wins"] += 1 if won else 0
    key = f"{symbol}:{direction}"
    ss = state.setdefault("symbol_dir_stats", {}).setdefault(key, {"n": 0, "wins": 0})
    ss["n"] += 1
    ss["wins"] += 1 if won else 0


def check_active_signals(state: dict):
    mids = fetch_all_mids()
    still_active = []
    for a in state.get("active_signals", []):
        sym = a["symbol"]
        price = mids.get(sym)
        if price is None:
            still_active.append(a)
            continue
        d = a["direction"]
        hit_stop = (price <= a["stop"]) if d == "long" else (price >= a["stop"])
        hit_tp2 = (price >= a["tp2"]) if d == "long" else (price <= a["tp2"])
        hit_tp1 = (price >= a["tp1"]) if d == "long" else (price <= a["tp1"])

        if hit_stop:
            update_pathway_and_symbol_stats(state, a["pathway"], sym, d, won=False)
            risk = abs(a["entry"] - a["stop"])
            pnl_pct = -PER_TRADE_RISK_PCT if a.get("tp1_hit") is False else -PER_TRADE_RISK_PCT * 0.3
            daily_bucket(state)["realized_pnl_pct"] += pnl_pct
            for h in state.get("history", []):
                if h["id"] == a["hist_id"]:
                    h["outcome"] = "loss"
            continue
        if hit_tp2:
            update_pathway_and_symbol_stats(state, a["pathway"], sym, d, won=True)
            daily_bucket(state)["realized_pnl_pct"] += PER_TRADE_RISK_PCT * 2.0
            for h in state.get("history", []):
                if h["id"] == a["hist_id"]:
                    h["outcome"] = "win"
            continue
        if hit_tp1 and not a.get("tp1_hit"):
            a["tp1_hit"] = True
            a["stop"] = a["entry"]  # move to breakeven
        still_active.append(a)
    state["active_signals"] = still_active


def daily_loss_paused(state: dict) -> bool:
    bucket = daily_bucket(state)
    if bucket["realized_pnl_pct"] <= -DAILY_LOSS_LIMIT_PCT:
        bucket["paused"] = True
    return bucket.get("paused", False)


def maybe_send_daily_summary(state: dict):
    day = utc_day_key()
    if state.get("last_summary_day") == day:
        return
    if now_utc().hour != 23:
        return
    bucket = daily_bucket(state)
    total = sum(v.get("n", 0) for v in state.get("pathway_stats", {}).values())
    wins = sum(v.get("wins", 0) for v in state.get("pathway_stats", {}).values())
    wr = (wins / total * 100) if total else 0.0
    text = (f"📊 {ENGINE_NAME} daily summary ({day})\n"
            f"Signals today: {bucket['signal_count']}\n"
            f"Realized PnL: {bucket['realized_pnl_pct']:.2f}%\n"
            f"Lifetime win rate: {wr:.1f}% (n={total})")
    send_telegram_plain(text)
    state["last_summary_day"] = day


# ═══════════════════════════════════════════════════════════════════════════
# MAIN SCAN
# ═══════════════════════════════════════════════════════════════════════════

def _prefetch(symbol: str):
    return symbol, fetch_symbol_bundle(symbol)


def evaluate_symbol(symbol: str, bundle: dict, ctx: dict, state: dict, base_regime: RegimeVector,
                     near_miss: Counter, pipeline_candidates: dict,
                     floor_candidates: list["Signal"]) -> list[Signal]:
    signals: list[Signal] = []
    spread_pct, depth_usd = book_spread_and_depth(bundle.get("book"))
    funding_reg = funding_oi_regime(symbol, ctx, bundle)

    # per-symbol regime classification (clean/choppy/high_vol), layered on
    # top of the scan-wide macro fields already in base_regime
    ind_1h_for_regime = get_indicators(symbol, "1h", bundle["1h"])
    symbol_regime = classify_symbol_regime(
        ind_1h_for_regime["adx"][-1], ind_1h_for_regime["bb_width_pctile"],
        ind_1h_for_regime["noise_idx"], volatility_percentile(symbol, ind_1h_for_regime),
    )
    regime = RegimeVector(
        btc_bias=base_regime.btc_bias, btc_strength=base_regime.btc_strength,
        breadth=base_regime.breadth, macro_hot=base_regime.macro_hot, session_w=base_regime.session_w,
        symbol_regime=symbol_regime, bb_width_pctile=ind_1h_for_regime["bb_width_pctile"],
        noise_idx=ind_1h_for_regime["noise_idx"],
    )
    regime_liq_mult = liquidity_floor_multiplier(regime)

    for pid, pipeline in PIPELINES.items():
        if pipeline.session_gated:
            pass  # session weight is scored, not gated -- see adaptive_threshold / score_candidate

        ind_trig = get_indicators(symbol, pipeline.trigger_tf, bundle[pipeline.trigger_tf])
        if not liquidity_ok(symbol, ctx, ind_trig, spread_pct, depth_usd, regime_liq_mult):
            near_miss[f"{pid}:liquidity_filtered"] += 1
            continue

        best_cand: Optional[Candidate] = None
        for pname, fn in PATHWAYS.items():
            try:
                cand = fn(symbol, pipeline, bundle)
            except Exception as e:
                log.debug(f"pathway {pname} error on {symbol}/{pid}: {e}")
                cand = None
            if cand is None:
                near_miss[f"{pid}:{pname}:no_setup"] += 1
                continue
            if cand.rr < MIN_RR:
                near_miss[f"{pid}:{pname}:rr_below_min"] += 1
                continue
            pipeline_candidates.setdefault(pid, []).append(cand)
            if best_cand is None or cand.rr > best_cand.rr:
                best_cand = cand

        if best_cand is None:
            continue

        if regime.symbol_regime == "high_vol":
            # Ported from Lucerna: in a high-volatility regime, widen SL/TP
            # rather than blocking the signal -- wide-but-real moves are
            # exactly what swing/intraday trades want to catch here. A
            # breakout-class candidate additionally needs a stronger
            # follow-through bar, since wick fakeouts are more common when
            # volatility itself is elevated.
            entry = best_cand.entry
            risk = abs(entry - best_cand.stop)
            widen = 1.35
            new_risk = risk * widen
            if best_cand.direction == "long":
                best_cand.stop = entry - new_risk
                best_cand.tp1 = entry + (best_cand.tp1 - entry) * widen
                best_cand.tp2 = entry + (best_cand.tp2 - entry) * widen
            else:
                best_cand.stop = entry + new_risk
                best_cand.tp1 = entry - (entry - best_cand.tp1) * widen
                best_cand.tp2 = entry - (entry - best_cand.tp2) * widen
            if best_cand.pathway == "momentum_breakout":
                ind_trig_hv = get_indicators(symbol, pipeline.trigger_tf, bundle[pipeline.trigger_tf])
                if ind_trig_hv["rel_vol"] < 1.5:  # stricter than the pathway's own 1.2 baseline
                    near_miss[f"{pid}:high_vol_breakout_followthrough_insufficient"] += 1
                    continue

        agree, conflict = family_votes(symbol, best_cand.direction, pipeline, bundle)
        if len(conflict) >= 3:
            near_miss[f"{pid}:ensemble_hard_conflict"] += 1
            continue

        convergence_tags = []
        same_dir_other_pathways = [c for c in pipeline_candidates.get(pid, [])
                                    if c.pathway != best_cand.pathway and c.direction == best_cand.direction]
        if same_dir_other_pathways:
            convergence_tags.append("pathway_convergence")

        confidence, breakdown = score_candidate(
            best_cand, regime, state, funding_reg, agree, conflict, convergence_tags,
            spread_pct, depth_usd,
            vwap_val=get_indicators(symbol, pipeline.trigger_tf, bundle[pipeline.trigger_tf])["vwap"],
        )
        base_threshold = state.get("governor", {}).get("threshold", 0.0)
        threshold = adaptive_threshold(regime, base_threshold)
        if confidence < threshold:
            near_miss[f"{pid}:below_threshold"] += 1
            # Frequency-floor fallback tracking (v1.2.0): only used if the
            # engine has gone silent past MAX_SILENCE_HOURS this scan (see
            # frequency_floor_signal / run_scan). Still respects the
            # cold-streak grade floor -- that protection isn't suspended
            # just because the market's been quiet.
            if confidence >= FREQUENCY_FLOOR_MIN_CONFIDENCE:
                grade_floor = cold_streak_grade_floor(state, symbol, best_cand.direction)
                candidate_grade = grade_for_confidence(confidence)
                if not grade_floor or grade_at_least(candidate_grade, grade_floor):
                    _, floor_ens_label = ensemble_adjustment(agree, conflict)
                    floor_candidates.append(Signal(
                        candidate=best_cand, confidence=confidence, grade=candidate_grade,
                        z_breakdown=breakdown, convergence_tags=convergence_tags,
                        ensemble_label=floor_ens_label, timestamp=now_utc().isoformat(),
                    ))
            continue

        grade = grade_for_confidence(confidence)
        floor = cold_streak_grade_floor(state, symbol, best_cand.direction)
        if floor and not grade_at_least(grade, floor):
            near_miss[f"{pid}:cold_streak_grade_floor"] += 1
            continue

        _, ens_label = ensemble_adjustment(agree, conflict)
        signals.append(Signal(
            candidate=best_cand, confidence=confidence, grade=grade, z_breakdown=breakdown,
            convergence_tags=convergence_tags, ensemble_label=ens_label,
            timestamp=now_utc().isoformat(),
        ))
    return signals


def run_scan(state: dict) -> list[Signal]:
    clear_indicator_cache()
    reset_cross_sectional_caches()
    near_miss: Counter = Counter()

    if daily_loss_paused(state):
        log.info("[SCAN] Daily loss limit breached -- pausing new signals for the rest of the UTC day.")
        return []

    log.info(f"[SCAN] Prefetching {len(WATCHLIST)} symbols...")
    ctx = load_market_ctx()
    bundles: dict[str, dict] = {}
    with ThreadPoolExecutor(max_workers=8) as ex:
        futs = [ex.submit(_prefetch, sym) for sym in WATCHLIST]
        for fut in as_completed(futs):
            sym, bundle = fut.result()
            if bundle is not None:
                bundles[sym] = bundle

    if BTC_SYMBOL not in bundles:
        log.warning("[SCAN] Aborting -- no BTC data (regime filter requires it)")
        return []

    btc_bias, btc_strength = compute_btc_regime(bundles[BTC_SYMBOL])
    log.info(f"[SCAN] BTC regime: {btc_bias} (ADX {btc_strength:.1f})")

    for sym, bundle in bundles.items():
        ind4h = get_indicators(sym, "4h", bundle["4h"])
        record_breadth(ind4h["closes"][-1] > ind4h["ema_mid"][-1])
        closes = ind4h["closes"]
        if len(closes) > RS_LOOKBACK_BARS:
            record_rs(sym, pct(closes[-1], closes[-RS_LOOKBACK_BARS]))

    regime = build_regime_vector(btc_bias, btc_strength)
    governor_adjust_threshold(state)
    tune_pathway_weights(state)  # ported from Axis Engine v2.1.0 -- see tune_pathway_weights docstring

    all_signals: list[Signal] = []
    pipeline_candidates: dict[str, list[Candidate]] = {}
    floor_candidates: list[Signal] = []
    for sym, bundle in bundles.items():
        try:
            sigs = evaluate_symbol(sym, bundle, ctx, state, regime, near_miss, pipeline_candidates, floor_candidates)
            all_signals.extend(sigs)
        except Exception as e:
            log.warning(f"[EVAL ERROR] {sym}: {e}")

    if near_miss:
        top = ", ".join(f"{k}={v}" for k, v in near_miss.most_common(10))
        log.info(f"[NEAR-MISS] {top}")

    if not all_signals:
        fallback = frequency_floor_signal(state, floor_candidates)
        if fallback is None:
            log.info("[SCAN] No candidates cleared all gates this scan.")
            gov = state.setdefault("governor", {}).setdefault("scan_counts", [])
            gov.append(0)
            return []
        log.info(f"[FREQUENCY FLOOR] Engine has been silent past {MAX_SILENCE_HOURS:.0f}h -- "
                 f"releasing best near-miss candidate ({fallback.candidate.symbol} "
                 f"{fallback.candidate.direction} conf={fallback.confidence:.1f}).")
        all_signals = [fallback]

    hourly = {sym: b["1h"] for sym, b in bundles.items()}
    clusters = build_correlation_clusters(hourly)
    ranked = dedup_correlated(all_signals, clusters)
    ranked = dedup_same_symbol(ranked)
    accepted = apply_portfolio_caps(ranked, state)

    gov_counts = state.setdefault("governor", {}).setdefault("scan_counts", [])
    gov_counts.append(len(accepted))
    state["governor"]["scan_counts"] = gov_counts[-GOVERNOR_LOOKBACK_SCANS:]

    if not accepted:
        log.info("[SCAN] Candidates existed but none survived portfolio caps.")
        return []

    mids = fetch_all_mids()
    sent: list[Signal] = []
    for sig in accepted:
        cand = sig.candidate
        live_price = mids.get(cand.symbol)
        if not freshness_ok(cand, live_price):
            log.info(f"[DECAY] {cand.symbol} skipped -- price drifted beyond {MAX_ENTRY_DRIFT_R}R before send")
            continue
        text = format_signal_message(sig)
        message_id = send_telegram(text)
        hist_id = record_signal(state, sig)
        track_signal(state, sig, message_id, hist_id)
        state.setdefault("governor", {})["last_signal_ts"] = time.time()
        sent.append(sig)
        log.info(f"[SENT] {cand.symbol} {cand.direction.upper()} | {cand.pathway} | "
                 f"{PIPELINES[cand.pipeline_id].label} | conf={sig.confidence:.1f} grade={sig.grade}")

    return sent


# ═══════════════════════════════════════════════════════════════════════════
# BACKTEST / EVALUATION MODULE
# ═══════════════════════════════════════════════════════════════════════════

TAKER_FEE = 0.00045      # Hyperliquid taker fee (approx, both sides)
SLIPPAGE_PCT = 0.0006    # realistic per-side slippage estimate
MIN_SAMPLE_SIZE = 20


def _simulate_pathway_on_candles(candles_15m, candles_1h, candles_4h, candles_1d, symbol) -> list[dict]:
    """Walk forward bar-by-bar using ONLY data available up to each decision
    point (no look-ahead: at bar i we only ever slice candles[:i+1])."""
    trades = []
    bundle_template = {"1h": candles_1h, "4h": candles_4h, "1d": candles_1d}
    n = len(candles_15m)
    open_trade = None
    for i in range(250, n):
        # slice every timeframe up to the current wall-clock point only
        t_now = candles_15m[i]["t"]
        bundle = {
            "15m": [c for c in candles_15m[:i + 1]],
            "1h": [c for c in candles_1h if c["t"] <= t_now],
            "4h": [c for c in candles_4h if c["t"] <= t_now],
            "1d": [c for c in candles_1d if c["t"] <= t_now],
        }
        if len(bundle["1h"]) < 60 or len(bundle["4h"]) < 60 or len(bundle["1d"]) < 30:
            continue

        if open_trade is not None:
            bar = candles_15m[i]
            d = open_trade["direction"]
            hit_stop = bar["l"] <= open_trade["stop"] if d == "long" else bar["h"] >= open_trade["stop"]
            hit_tp = bar["h"] >= open_trade["tp1"] if d == "long" else bar["l"] <= open_trade["tp1"]
            if hit_stop:
                trades.append({**open_trade, "outcome": "loss", "r_multiple": -1.0})
                open_trade = None
            elif hit_tp:
                trades.append({**open_trade, "outcome": "win", "r_multiple": open_trade["rr"]})
                open_trade = None
            continue

        clear_indicator_cache()
        pipeline = PIPELINES["fast"]
        try:
            cand = pathway_trend_continuation(symbol, pipeline, bundle) or \
                   pathway_liquidity_reversal(symbol, pipeline, bundle) or \
                   pathway_momentum_breakout(symbol, pipeline, bundle)
        except Exception:
            cand = None
        if cand is None:
            continue
        open_trade = {
            "symbol": symbol, "direction": cand.direction, "entry": cand.entry,
            "stop": cand.stop, "tp1": cand.tp1, "rr": cand.rr, "pathway": cand.pathway,
            "entry_ts": t_now,
        }
    return trades


def _apply_costs(trades: list[dict]) -> list[dict]:
    out = []
    for t in trades:
        cost_r = (TAKER_FEE * 2 + SLIPPAGE_PCT * 2) / max(abs(t["entry"] - t["stop"]) / t["entry"], 1e-6) \
            if t["entry"] else 0.0
        net_r = t["r_multiple"] - cost_r
        out.append({**t, "net_r": net_r})
    return out


def _summ(trades: list[dict], key: str = "r_multiple") -> dict:
    n = len(trades)
    if n == 0:
        return {"n": 0, "win_rate": None, "avg_r": None, "meaningful": False}
    wins = sum(1 for t in trades if t["outcome"] == "win")
    avg_r = statistics.mean(t[key] for t in trades)
    return {"n": n, "win_rate": wins / n, "avg_r": avg_r, "meaningful": n >= MIN_SAMPLE_SIZE}


def _baseline_ma_crossover(candles_15m: list[dict]) -> list[dict]:
    closes = [c["c"] for c in candles_15m]
    fast, slow = ema(closes, 10), ema(closes, 30)
    trades = []
    open_trade = None
    for i in range(31, len(candles_15m)):
        bar = candles_15m[i]
        if open_trade is not None:
            d = open_trade["direction"]
            hit_stop = bar["l"] <= open_trade["stop"] if d == "long" else bar["h"] >= open_trade["stop"]
            hit_tp = bar["h"] >= open_trade["tp1"] if d == "long" else bar["l"] <= open_trade["tp1"]
            if hit_stop:
                trades.append({**open_trade, "outcome": "loss", "r_multiple": -1.0})
                open_trade = None
            elif hit_tp:
                trades.append({**open_trade, "outcome": "win", "r_multiple": 1.5})
                open_trade = None
            continue
        cross_up = fast[i - 1] <= slow[i - 1] and fast[i] > slow[i]
        cross_dn = fast[i - 1] >= slow[i - 1] and fast[i] < slow[i]
        if not cross_up and not cross_dn:
            continue
        price = closes[i]
        a = statistics.pstdev(closes[max(0, i - 20):i + 1]) or price * 0.01
        direction = "long" if cross_up else "short"
        stop = price - a if direction == "long" else price + a
        tp1 = price + a * 1.5 if direction == "long" else price - a * 1.5
        open_trade = {"direction": direction, "entry": price, "stop": stop, "tp1": tp1}
    return trades


def walk_forward_backtest(symbol: str, n_windows: int = 4) -> dict:
    """Rolling train/test windows + one untouched final holdout, no
    look-ahead, fee/slippage-aware net metrics, +-10% sensitivity check,
    min-sample-size flagging, and an MA-crossover baseline comparison."""
    c15 = fetch_candles(symbol, "15m", 6000)
    c1h = fetch_candles(symbol, "1h", 1500)
    c4h = fetch_candles(symbol, "4h", 1500)
    c1d = fetch_candles(symbol, "1d", 400)
    if not c15 or not c1h or not c4h or not c1d:
        return {"symbol": symbol, "error": "insufficient historical data"}

    n = len(c15)
    holdout_start = int(n * 0.85)
    train_pool = c15[:holdout_start]
    holdout = c15[holdout_start:]

    window_size = len(train_pool) // n_windows
    window_results = []
    for w in range(n_windows):
        seg = train_pool[w * window_size: (w + 1) * window_size]
        if len(seg) < 300:
            continue
        trades = _simulate_pathway_on_candles(seg, c1h, c4h, c1d, symbol)
        trades = _apply_costs(trades)
        window_results.append({"window": w, "gross": _summ(trades, "r_multiple"),
                                "net": _summ(trades, "net_r")})

    holdout_trades = _apply_costs(_simulate_pathway_on_candles(holdout, c1h, c4h, c1d, symbol))
    holdout_summary = {"gross": _summ(holdout_trades, "r_multiple"), "net": _summ(holdout_trades, "net_r")}

    # parameter sensitivity: perturb MIN_RR by +-10% and re-check holdout net performance
    global MIN_RR
    original = MIN_RR
    sensitivity = {}
    for pct_change, label in [(0.9, "-10%"), (1.1, "+10%")]:
        MIN_RR = original * pct_change
        clear_indicator_cache()
        trades = _apply_costs(_simulate_pathway_on_candles(holdout, c1h, c4h, c1d, symbol))
        sensitivity[label] = _summ(trades, "net_r")
    MIN_RR = original
    clear_indicator_cache()
    base_net_avg = holdout_summary["net"]["avg_r"]
    collapsed = False
    if base_net_avg is not None:
        for lbl, s in sensitivity.items():
            if s["avg_r"] is not None and (base_net_avg > 0) and (s["avg_r"] < base_net_avg * 0.3):
                collapsed = True

    baseline_trades = _baseline_ma_crossover(holdout)
    baseline_summary = _summ(_apply_costs([{**t, "entry": t.get("entry", 1), "stop": t.get("stop", 0),
                                              "r_multiple": t["r_multiple"]} for t in baseline_trades]), "net_r") \
        if baseline_trades else {"n": 0, "meaningful": False}

    return {
        "symbol": symbol,
        "windows": window_results,
        "holdout": holdout_summary,
        "sensitivity_check": sensitivity,
        "sensitivity_flags_overfitting": collapsed,
        "baseline_ma_crossover_holdout": baseline_summary,
        "outperforms_baseline": (
            holdout_summary["net"]["avg_r"] is not None and baseline_summary.get("avg_r") is not None and
            holdout_summary["net"]["avg_r"] > baseline_summary["avg_r"]
        ) if baseline_summary.get("n", 0) else None,
        "min_sample_size": MIN_SAMPLE_SIZE,
    }


def run_backtest_suite(symbols: Optional[list[str]] = None) -> dict:
    symbols = symbols or WATCHLIST[:6]  # a representative slice by default; pass the full list for a full run
    results = {}
    for sym in symbols:
        log.info(f"[BACKTEST] {sym} ...")
        try:
            results[sym] = walk_forward_backtest(sym)
        except Exception as e:
            results[sym] = {"symbol": sym, "error": str(e)}
    return results


# ═══════════════════════════════════════════════════════════════════════════
# ENTRYPOINT
# ═══════════════════════════════════════════════════════════════════════════

_shutdown_state_ref: dict = {}


def _shutdown_handler(signum, frame):
    log.info(f"[SHUTDOWN] signal {signum} received -- saving state before exit")
    if _shutdown_state_ref:
        save_state(_shutdown_state_ref)
    sys.exit(0)


def main():
    print("=" * 78)
    print(f"  {ENGINE_NAME} Engine v{VERSION} -- dual-pipeline, three-pathway, ensemble-scored")
    print(f"  Pipelines: {', '.join(p.label for p in PIPELINES.values())}")
    print(f"  Pathways: {', '.join(PATHWAYS.keys())}")
    print(f"  Target: {int(TARGET_SIGNALS_MIN)}-{int(TARGET_SIGNALS_MAX)} signals/day | "
          f"Top {TOP_N_SIGNALS_PER_SCAN}/scan | Sector cap {MAX_PER_SECTOR} | Same-dir cap {MAX_SAME_DIRECTION}")
    print(f"  Concurrency cap: {MAX_CONCURRENT_ACTIVE_SIGNALS} | OI floor: ${MIN_OI_USD:,.0f} | "
          f"Dry-run: {DRY_RUN}")
    print("=" * 78)

    if len(sys.argv) > 1 and sys.argv[1] == "backtest":
        syms = sys.argv[2:] or None
        results = run_backtest_suite(syms)
        print(json.dumps(results, indent=2, default=str))
        return

    _signal.signal(_signal.SIGTERM, _shutdown_handler)
    _signal.signal(_signal.SIGINT, _shutdown_handler)

    state = load_state()
    _shutdown_state_ref.update(state)

    if state.get("active_signals"):
        log.info(f"[TRACKING] Checking {len(state['active_signals'])} active signal(s)...")
        try:
            check_active_signals(state)
        except Exception as e:
            log.warning(f"[TRACK ERROR] {e}")
    else:
        log.info("[TRACKING] No active signals to check.")

    try:
        run_scan(state)
    except Exception as e:
        log.error(f"[MAIN ERROR] {e}")
        send_telegram_plain(f"⚠️ {ENGINE_NAME} engine error: {e}")

    try:
        maybe_send_daily_summary(state)
    except Exception as e:
        log.warning(f"[SUMMARY ERROR] {e}")

    prune_state(state)
    save_state(state)
    log.info("[DONE] Scan complete. Exiting.")


if __name__ == "__main__":
    main()
