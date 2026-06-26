import os, json, time, math, random, threading, requests, re
import signal as os_signal
import sys
import copy
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo
from collections import OrderedDict  # [AUDIT-P7-6] for LRU _indicator_cache eviction
# [Fix-20] Import shared implementations instead of redefining locally — see audit
# Part 1 LOW item on duplicated safe/atr/sma/_cluster_levels risking silent drift.
# liquidity_confluence.py already imports safe/atr from utils.py the same way.
# sma and _cluster_levels are left as local definitions for now (minor differences
# in usage context per the audit) but their now-fully-dead utils.py copies are
# removed below in utils.py.
from utils import safe, atr

__version__ = "17.0.3"  # SL widening: BREAK 0.85->1.2, PULL 0.85->1.1, HIGH_ATR 0.90->1.25, REGIME_HIGH_VOL_SL 0.95->1.15; TP1 raised to match
# frequency-tuning constants (MIN_RR_RATIO, ADX_SCORE_MIN, MIN_DAILY_ADX,
# ADX_BREAK_GATE, TREND_HOLD_BARS, SIGNAL_COOLDOWN_BARS[_HIGHSCORE],
# MAX_SIGNALS_DEFAULT/BULL_TREND) back to their pre-Section-6 originals. See
# SECTION 9 below.

# ═══════════════════════════════════════════════════════════════
# CHANGELOG — BREAK & PULL Engine Overhaul (2026-06-25)
# Implementation of audit fixes for BREAK/PULL signal quality and timing
# ═══════════════════════════════════════════════════════════════

# PHASE 1 (Fix A) — Overextension/Blow-off Suppression for BREAK
#   - ENABLE_OVEREXTENSION_GATE = True
#   - BREAK_OVEREXT_ATR_MULT = 2.0
#   - BREAK_MA_DIST_ATR_MULT = 3.5
#   - BREAK_MULTIBAR_ATR_MULT = 3.5
#   - BREAK_OVEREXT_LOOKBACK = 4
#   - Soft penalty: -3 for 2+ overextension signals, -1 for 1 signal
#   - Breakout-trap detection: -3 penalty for liquidity grabs with closing fail
#   - Location: _apply_scoring_and_filters() BREAK section

# PHASE 2 (Fix F) — Suppress Counter-4H-Trend PULL
#   - ENABLE_COUNTERTREND_PULL_GATE = True
#   - Soft penalty: -2 for PULL into active 4H counter-trend without OI+divergence
#   - Hard-gate alternative commented for future validation
#   - Location: _apply_scoring_and_filters() PULL section

# PHASE 3 (Fix C) — Pullback Depth Gate
#   - ENABLE_PULL_DEPTH_GATE = True
#   - PULL_MIN_DEPTH_ATR = 0.15
#   - Hard gate: requires minimum 0.15 ATR penetration below EMA21
#   - Location: _detect_raw_signals() pull_touched block, folded into long_pull/short_pull

# PHASE 4 (Fix D) — PULL Momentum Collapse Penalty
#   - ENABLE_PULL_MOMENTUM_COLLAPSE_PENALTY = True
#   - PULL_RSI_COLLAPSE_THRESH = 5.0
#   - PULL_RSI_LOOKBACK = 4
#   - Soft penalty: -1 for RSI dropping >5 pts over 4 bars on PULL
#   - Location: _apply_scoring_and_filters() PULL section

# PHASE 4 (Fix E) — PULL EMA Velocity Check
#   - ENABLE_PULL_EMA_VELOCITY_CHECK = True
#   - Soft penalty: -1 for weakening EMA velocity on PULL
#   - Location: _apply_scoring_and_filters() EMA velocity block (extended BREAK-only logic)

# PHASE 5 (Fix B) — BREAK RSI Ceiling Expansion
#   - ENABLE_BREAK_RSI_WIDENING = True
#   - RSI_BREAK_LONG_MAX: 75.0 → 82.0
#   - RSI_BREAK_SHORT_MIN: 25.0 → 18.0
#   - Soft penalty: -1 for BREAK in extended zone (RSI >75 long, <25 short)
#   - Location: Constants section + _apply_scoring_and_filters() BREAK section

# PHASE 5 (Fix H) — PULL RSI Lower-Bound Tightening
#   - ENABLE_PULL_RSI_TIGHTENING = True
#   - Soft penalty: -1 for PULL near oversold/overbought (RSI <44 long, >56 short)
#   - Hard-gate alternative commented (constant changes)
#   - Location: _apply_scoring_and_filters() PULL section

# PHASE 6 (Fix G) — VWAP Restoration for Pullback Mode
#   - ENABLE_PULL_VWAP_RESTORATION = True
#   - Hard gate: VWAP check now applies to pullback mode (was bypassed)
#   - Soft-penalty fallback commented for validation
#   - Location: _detect_raw_signals() vwap_long/vwap_short logic

# PHASE 7 (Fix I) — ADX Expanding Check
#   - ENABLE_ADX_EXPANDING_CHECK = True
#   - ADX_RISING_LOOKBACK = 3
#   - ENABLE_SCORE_REBALANCE = True
#   - ADX_BREAK_OK: now accepts ADX ≥20 if rising (was ≥25 only)
#   - Soft bonus: +1 for expanding ADX, +1 for strong ADX ≥30
#   - Score rebalance: replaced adx4h_score_ok with h4_vol_expansion in base score
#   - Location: _detect_raw_signals() ADX logic + _apply_scoring_and_filters() BREAK section

# PHASE 8 (Fix J) — Two-Tier BREAK Engine
#   - ENABLE_EARLY_BREAK_TIER = True
#   - EARLY_BREAK_PRIOR_HIGHS_LOOKBACK = 3
#   - EARLY_BREAK_REQUIRE_ADX_RISING = True
#   - Tier 2 (Early): close-confirmed breakout of a short prior-N-bar lookback, fired
#     without the full BREAK quality gates — NOT live intra-candle/tick detection.
#     [AUDIT-P5-1] relabeled; see _detect_raw_signals() early_break_long/short.
#   - Tier 1 (Standard): unchanged close-confirmed BREAK
#   - res.break_tier field added for labeling ("early" vs "standard")
#   - Location: _detect_raw_signals() early_break logic + _apply_scoring_and_filters() scoring

# PHASE 9 — Quality-Stratified PULL Engine
#   - ENABLE_PULL_TIERING = True
#   - Tier A: +1 bonus for full quality confirmation (MTF, RSI, depth, VWAP, momentum)
#   - Tier B: standard PULL (default label)
#   - Tier C: hard gates for prohibited conditions (counter-trend + bad RSI, extreme breadth)
#   - Structural backing penalty: -1 if no support/resistance within 1 ATR
#   - Volume-quality filter: -1 if pullback volume not lower than impulse
#   - res.pull_tier field added ("A" or "B")
#   - Location: _apply_scoring_and_filters() PULL section

# CROSS-CUTTING ITEMS
#   - ENABLE_SR_CLEARANCE_FIX = True: SR clearance only blocks when level not yet cleared
#   - ENABLE_LIQUIDITY_CONFLUENCE = True: enabled HTF liquidity engine (was False)
#   - 4H stale-bias penalty: increased from -1 to -2 for PULL signals
#   - continuation_probability() function added (0-10 scale, baseline 5)
#   - ENABLE_CONTINUATION_PROB_GATE = False (display-only until validated)
#   - res.continuation_prob_score field added
#   - Location: Constants, SR filter logic, 4H stale bias logic, new function

# VALIDATION ORDER (per audit priority)
#   1. Phase 1 (overextension) → 2. Phase 2 (countertrend PULL) → 3. Phase 3 (depth)
#   4. Phase 4 (momentum/EMA) → 5. Phase 5 (RSI bands) → 7. Phase 7 (ADX expanding)
#   8. Phase 8 (early BREAK - validate last, highest complexity)
#   6. Phase 9 / cross-cutting as time allows

# All new behaviors default to enabled (True) except ENABLE_CONTINUATION_PROB_GATE (False)
# ═══════════════════════════════════════════════════════════════
# CHANGELOG — scalp_swing_bot_audit.md implementation pass (2026-06-20)
# One line per audit item, grouped by implementation_prompt.md section.
# Full rationale/before-after is in each [Fix-N] inline comment at point of
# change; this block is an index, not a substitute for reading those.
# ═══════════════════════════════════════════════════════════════
# SECTION 4 (deploy-blocking fixes):
#   1. [Fix-11] ATR history keyed by `symbol` param, not res.symbol (was always "").
#   2. [Fix-12] New "tp1_then_sl" outcome, distinct from clean "tp1" win.
#   3. [Fix-13] LiquidityConfig.max_bonus 20->10; MAX_POSITIVE_ADJUSTMENTS 20->10.
#   4. [Fix-14] External/internal liquidity bonus now gated by ATR-distance + strength.
#   5. [Fix-15] Hard backstop clamp after bonus-cap loop: adjusted_score <= score+cap.
#   6. [Fix-16] hard_suppress_counter_mss default False->True; false-sweep left False (documented).
#   7. [Fix-17] Same-group long+short conflict guard added to deduplicate_correlated().
#   8. [Fix-18] OI_STALE_CUTOFF_S=45min hard cutoff added to compute_oi_trend().
#   9. [Fix-19] daily_vwap() now accepts/threads reference_ms.
#  10. [Fix-20] Main bot imports safe/atr from utils.py; dead sma/_cluster_levels removed from utils.py.
# SECTION 5 (quality improvements):
#   2. [Fix-23] _get_data_driven_penalty_priority() — data-driven PENALTY_PRIORITY ordering.
#   3. [Fix-21] eqh_eql_min_touches 2->3.
#   5. [Fix-24] S/R system reconciliation — logging/visibility only, no scoring change.
#   6. [Fix-25] backtest_harness.py scaffolded, then REMOVED in a later pass
#      (2026-06-20): confirmed it has zero runtime dependency in this file (the
#      bot never imports it — only the harness imported FROM the bot, to
#      replay live scoring logic offline), so it's not needed to give signals
#      and was deleted as unused/incomplete (historical candle ingestion was
#      never wired up). The TUNABLE items below that mention "needs backtest
#      validation" still mean exactly that in spirit — those tradeoffs remain
#      unvalidated against historical data — there's just no in-repo harness
#      for it anymore.
#   7. [Fix-26] RSI band narrowing tracked via comment, no values changed (no data yet).
#   9. [Fix-27] require_multi_component_confluence opt-in toggle (default off).
#  10. [Fix-28] Win-rate staleness guard (WIN_RATE_STALE_DAYS) added to compute_win_rate_analytics.
# SECTION 6 (frequency — conditional; user explicitly confirmed wanted on 2026-06-20):
#   1. [Fix-29] TREND_HOLD_BARS 2->1.
#   2. [Fix-30] ADX_BREAK_GATE 25->22, ADX_SCORE_MIN 20->17.5, MIN_DAILY_ADX 18->16.
#   3. NOT implemented — flagged for user decision. Adding WATCHLIST symbols
#      requires real liquidity/listing judgment outside the codebase; no
#      starter list guessed. See implementation_prompt_section6.md Item 3.
#   4. [Fix-31] MIN_RR_RATIO 0.9->0.8.
#   5. [Fix-32] RS_BREAK_HARD_GATE_PCT hard-suppress (`return res`) converted to
#      a heavy score penalty (RS_BREAK_HARD_GATE_PENALTY=-3) that flows through
#      the existing [Fix-9] penalty-cap/PENALTY_PRIORITY machinery.
#   6. [Fix-33] compute_pairwise_correlation_matrix()/cluster_by_correlation()
#      added — DIAGNOSTIC LOGGING ONLY (LOG_DYNAMIC_CORR_GROUPS, default False).
#      CORR_GROUPS["layer1"] hand-picked membership left UNCHANGED — actual
#      regrouping still flagged as a decision requiring multi-scan data review
#      or manual judgment, per audit instruction not to invent it.
#   7. [Fix-34] MAX_SIGNALS_DEFAULT 4->5, MAX_SIGNALS_BULL_TREND 7->9 — flagged
#      as a capital-allocation decision, not a code-quality fix.
#   8. [Fix-35] SIGNAL_COOLDOWN_BARS_HIGHSCORE 2->1. SIGNAL_COOLDOWN_BARS left at
#      1 (not lowered to 0) — see inline comment for why 0 wasn't taken.
#   9. [Fix-36] SPREAD_DYNAMIC_AVG_THRESHOLD 0.10->0.15 (per-symbol, data-driven
#      exemption widened) chosen INSTEAD OF relaxing the global SPREAD_WARN_PCT/
#      SPREAD_SUPPRESS_PCT, which are left UNCHANGED — see inline comment.
#  10. [Fix-37] PULL_REQUIRES_4H_OVERRIDE dict + get_pull_requires_4h() added
#      (per-symbol or per-BTC-regime-label override). Empty by default — global
#      PULL_REQUIRES_4H=True fallback unchanged until a reviewer populates it.
# VERIFICATION PASS (2026-06-20, same day): the Section 6 changes above were
# present in the file but UNVERIFIED — re-checked line-by-line against the
# audit/implementation prompt rather than trusted at face value. One real bug
# found and fixed:
#   [Fix-38] get_pull_requires_4h() was CALLED (line ~2317, inside the Item 10
#     PULL-alignment check) but never DEFINED anywhere in the file — a
#     guaranteed NameError on every scan reaching that code path (i.e. every
#     scan). Added the missing function next to get_dynamic_max_signals(),
#     matching the behavior already documented in the PULL_REQUIRES_4H_OVERRIDE
#     comment (exact-symbol match takes precedence over a regime-label
#     substring match; falls back to the unchanged global PULL_REQUIRES_4H).
#     Everything else in Items 3-10 checked out against the actual code
#     (constants, gate-to-penalty conversion, penalty-cap wiring, diagnostic
#     correlation helper, spread-exemption wiring) and needed no changes.
# SECTION 7 (quality+frequency, "free wins"):
#   9. [Fix-22] SweepResult.sweep_status three-state field ("confirmed_valid" /
#      "confirmed_false" / "pending"); analyze() halves the sweep bonus when pending.
#   (All other Section 7 items map 1:1 onto Section 4/5 items above — see table
#   in delivery summary; no separate Fix-N tags since no separate code changed.)
# SECTION 8 (signal-quality pass, 2026-06-20 — driven by review of 6 consecutive
# SL losses across SUIUSDT/BTCUSDT/BNBUSDT/ETHUSDT/HYPEUSDT/SOLUSDT):
#   1. [Fix-40] liquidity_confluence.py: a volume-confirmed counter-direction
#      SWEEP now sets out.hard_suppress=True (previously only a counter-direction
#      MSS could suppress; a fresh, volume-confirmed counter-sweep — one of the
#      strongest reversal tells in the engine — was only ever a -4 score penalty
#      that got outvoted by unrelated bonuses). Gated behind the existing
#      hard_suppress_counter_mss config flag.
#   2. [Fix-41] liquidity_confluence.py: new premium_discount_adverse_weight
#      (default 4, vs the existing favorable-zone premium_discount_weight=2).
#      Longing into a premium zone / shorting into a discount zone is now
#      penalized twice as hard as the favorable case is rewarded.
#   3. [Fix-42] Main bot: severe S/R-vs-liquidity-engine disagreement (>3.0 ATR,
#      new SR_LIQUIDITY_DISAGREEMENT_SEVERE_ATR_MULT) now applies a real
#      SR_LIQUIDITY_DISAGREEMENT_PENALTY (-3) routed through the normal
#      adjs/penalty-cap path, instead of being log-only as in [Fix-24]. The
#      original >1.0 ATR threshold still just logs (kept as-is, was working
#      as visibility-only).
# All three are TUNABLE — values are reasoned defaults, not backtested. Watch
# the next batch of signals: if [LIQ-SUPPRESS] / severe-disagreement penalties
# fire on setups that would have won, loosen; if losers like the 6 above still
# slip through, tighten further.
# SECTION 9 (rollback pass, 2026-06-20 — user-directed, undoing Section 6's
# frequency-tuning constants now that Section 8's score-logic fixes are in):
#   MIN_RR_RATIO        0.8  -> 1.0
#   ADX_SCORE_MIN        17.5 -> 20.0
#   ADX_BREAK_GATE       22.0 -> 25.0
#   MIN_DAILY_ADX        16.0 -> 18.0
#   TREND_HOLD_BARS      1    -> 2
#   SIGNAL_COOLDOWN_BARS / _HIGHSCORE   1 -> 2 (both)
#   MAX_SIGNALS_DEFAULT / _BULL_TREND   5/9 -> 4/7
# This will reduce signal frequency relative to v15.7.2/15.8.0 — that's the
# intent. The Section 8 score-logic fixes (counter-sweep suppression, adverse-
# zone penalty, severe S/R disagreement penalty) are independent of these and
# remain in place.
# SECTION 10 (HTF liquidity wiring, 2026-06-20 — user request: "wire the
# liquidity engine to also analyze 4h/1h candles for HTF liquidity pools
# instead of just borrowing the EMA trend flag"):
#   1. [Fix-43] liquidity_confluence.py: SignalConfluence.analyze() now runs
#      real LiquidityAnalyzer/StructureAnalyzer passes over candles_1h/
#      candles_4h (actual 1h/4h dealing range, external BSL/SSL pools,
#      premium/discount zone, and sweep detection) instead of those params
#      always being unused. ADDITIVE/layered, not a replacement of the
#      existing 15m-only scoring: new htf_external_liquidity_weight (=2),
#      htf_zone_weight (=2), and htf_sweep_weight (=6, higher than the 15m
#      sweep_weight=4 — an HTF sweep is rarer and more decisive) score
#      independently and flow through the same existing max_bonus=10 cap, so
#      [Fix-13]'s MAX_POSITIVE_ADJUSTMENTS/max_bonus reconciliation still
#      holds with no changes needed there. Distance gating
#      (htf_external_liquidity_max_dist_atr) is computed against each
#      timeframe's OWN ATR, not 15m ATR. The htf_sweep_weight bonus only
#      fires when the 15m engine's own MSS already confirms direction (the
#      "HTF grab -> LTF structure confirms -> enter" pattern), not as an
#      independent HTF-only signal.
#   2. [Fix-44] liquidity_confluence.py: SignalConfluence._cache/_lock
#      (declared in __init__, previously never touched anywhere in the
#      file) now actually cache 1h/4h swing/level/sweep computation per
#      (symbol, timeframe), keyed by the last closed HTF candle's
#      timestamp — avoids redoing fractal detection on the same 1h/4h
#      candles on every 15m scan within an unchanged HTF bar.
#   3. Call-site wiring: _apply_scoring_and_filters() now accepts
#      candles_1h/candles_4h (threaded from compute_signals(), where both
#      were already in scope but not passed down), and the
#      _liq_engine.analyze() call no longer hardcodes candles_1h=None,
#      candles_4h=None. htf_bull_flags / htf_weight (the EMA-cross
#      alignment gate) are UNCHANGED — still a legitimate, cheap directional
#      filter, kept alongside the new genuine HTF liquidity structure as a
#      separate confluence input rather than one masquerading as the other.
#   4. New SignalResult / liquidity_confluence_log fields for visibility:
#      htf_liquidity_score, htf_1h_zone, htf_4h_zone, htf_1h_external_bsl/
#      ssl, htf_4h_external_bsl/ssl, htf_1h_sweep, htf_4h_sweep.
# All Section 10 weights are TUNABLE — reasoned defaults, not backtested.
# Compare htf_liquidity_score / [LIQ] reasons against outcomes before
# adjusting; consider promoting from "layer" to "replace" (using HTF zone/
# external levels instead of the 15m-derived ones in the existing blocks)
# only after that comparison, per the original design discussion.


# ── LIQUIDITY CONFLUENCE FEATURE FLAG ─────────────────────────
# Cross-cutting: Enable HTF liquidity confluence (audit §9)
ENABLE_LIQUIDITY_CONFLUENCE: bool = True
# [Fix-46] L2 order-book imbalance feature flag. fetch_l2_imbalance() was
# fully implemented and imported, but the call site below hardcoded
# `_l2_imb = None`, so l2_imbalance_weight/l2_imbalance_threshold never
# contributed to any score. Now wired through hl_post (the existing
# rate-limited/retrying Hyperliquid request function) when enabled.
# Default False: this is a TUNABLE, unvalidated addition — and unlike the
# other confluence inputs, it costs a synchronous network round-trip per
# candidate signal, so it's opt-in rather than on-by-default until that
# latency/rate-limit cost has been weighed against backtested edge.
ENABLE_L2_IMBALANCE: bool = False
try:
    from liquidity_confluence import (
        LiquidityConfig, SignalConfluence, ConfluenceOutput,
        fetch_l2_imbalance,
    )
    _liq_cfg = LiquidityConfig(enable=ENABLE_LIQUIDITY_CONFLUENCE)
    _liq_engine = SignalConfluence(_liq_cfg)
except Exception as _e:
    print(f"[INIT] liquidity_confluence module not available: {_e}")
    _liq_engine = None

# [AUDIT-P7-1] Tracks whether the "_liq_engine is None" operator alert has already been
# sent this process lifetime, so a long-running scanner doesn't spam Telegram with the same
# warning on every scan cycle if the module stays unavailable.
_liq_engine_missing_alerted: bool = False

_FF_TZ = ZoneInfo("America/New_York")
OI_EXPECTED_INTERVAL_S: float = 15 * 60

# ── CONFIG ───────────────────────────────────────────────────
TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN")
TG_CHAT_ID   = os.getenv("TG_CHAT_ID")
if not TG_BOT_TOKEN:
    raise RuntimeError("TG_BOT_TOKEN environment variable is required")
if not TG_CHAT_ID:
    raise RuntimeError("TG_CHAT_ID environment variable is required")

STATE_FILE            = "state.json"
SCAN_WORKERS          = int(os.getenv("SCAN_WORKERS", "2"))
HL_MIN_INTERVAL_S     = float(os.getenv("HL_MIN_INTERVAL_S", "0.18"))
HL_MIN_INTERVAL_MAX_S = float(os.getenv("HL_MIN_INTERVAL_MAX_S", "0.60"))
HL_TF_WORKERS         = int(os.getenv("HL_TF_WORKERS", "1"))

_hl_request_lock    = threading.Lock()
_hl_last_request_ts = 0.0
_hl_min_interval_s  = HL_MIN_INTERVAL_S
_hl_consecutive_successes = 0

# [AUDIT-P7-3] _hl_session is a module-level requests.Session shared across scan threads
# (SCAN_WORKERS threads call hl_post() concurrently during Phase 1/Phase 2 scanning).
# requests.Session is documented as thread-safe for concurrent requests because its
# underlying urllib3 connection pools are thread-safe. _hl_request_lock (above) guards only
# the rate-limit timing state (_hl_last_request_ts, _hl_min_interval_s, _hl_consecutive_
# successes) — the actual _hl_session.post() call in hl_post() happens OUTSIDE that lock,
# and that's fine: it's relying on Session/urllib3's own thread safety, not on
# _hl_request_lock, for the concurrent .post() calls themselves.
_hl_session = requests.Session()

# ── HARDCODED WATCHLIST ───────────────────────────────────────
# Section 6 Item 3 (FLAGGED, NOT IMPLEMENTED): the audit suggests adding
# additional symbols here, each paired with a CORR_GROUPS entry (even if a new
# singleton group like "bnb"/"hype"/"oracle" below). This requires real
# liquidity/listing judgment outside the codebase — no specific symbols are
# guessed here. This is a decision for the user/reviewer to make explicitly.
WATCHLIST = [
    "BTCUSDT", "ETHUSDT", "HYPEUSDT", "ZECUSDT", "NEARUSDT",
    "ONDOUSDT", "SUIUSDT", "PENGUUSDT", "BNBUSDT", "SOLUSDT",
    "TRXUSDT", "BCHUSDT", "DOGEUSDT", "ADAUSDT", "DOTUSDT",
    "TAOUSDT", "AVAXUSDT", "LINKUSDT", "AAVEUSDT", "XRPUSDT",
    "XLMUSDT", "UNIUSDT", "LTCUSDT", "APTUSDT", "PENDLEUSDT",
]

CORR_GROUPS: dict[str, set[str]] = {
    "layer1":    {"ETHUSDT", "SOLUSDT", "AVAXUSDT", "NEARUSDT", "APTUSDT", "ADAUSDT", "DOTUSDT", "SUIUSDT"},
    "defi":      {"AAVEUSDT", "UNIUSDT", "PENDLEUSDT", "ONDOUSDT"},
    "meme":      {"DOGEUSDT", "PENGUUSDT"},
    "btc_proxy": {"BTCUSDT", "LTCUSDT", "ZECUSDT", "BCHUSDT"},
    "xlm_xrp":   {"XLMUSDT", "XRPUSDT"},
    "l1_alt":    {"TAOUSDT", "TRXUSDT"},
    "bnb":       {"BNBUSDT"},
    "hype":      {"HYPEUSDT"},
    "oracle":    {"LINKUSDT"},
}

# ── INDICATOR LENGTHS ────────────────────────────────────────
FAST_LEN  = 21
SLOW_LEN  = 50
TREND_LEN = 200
RSI_LEN   = 14
ATR_LEN   = 14
ADX_LEN   = 14
BB_LEN    = 20
BB_MULT   = 2.0
VOL_LEN   = 20
OBV_LEN   = 10

# ── RISK / SCORE ─────────────────────────────────────────────
MIN_SCORE            = 3
MAX_SIGNALS_PER_SCAN = 3
MAX_SIGNAL_HISTORY   = 2000
MIN_OI_USD: float = 500_000.0

MAX_NEGATIVE_ADJUSTMENTS: int = 3
# [Fix-23] TUNABLE — opt-in toggle for the data-driven penalty priority ordering.
# Default True since it's a strict improvement where data exists and degrades
# gracefully to the old hand-picked order otherwise; flip to False to fully revert
# to the original static PENALTY_PRIORITY list for A/B comparison.
DATA_DRIVEN_PENALTY_PRIORITY: bool = True
# Mirrors MAX_NEGATIVE_ADJUSTMENTS. Without this, positive adjustment stacks
# (e.g. liquidity-confluence bonus up to +20) are structurally uncapped while
# negative stacks are capped at -3, biasing the system toward more signals
# firing rather than better-filtered ones.
MAX_POSITIVE_ADJUSTMENTS: int = 10  # [Fix-13] TUNABLE — reconciled with liquidity engine's max_bonus=10 (was 20; see liquidity_confluence.py LiquidityConfig.max_bonus comment)

MAX_CONCURRENT_ACTIVE_SIGNALS: int = 18

USE_1H_RSI_DIVERGENCE: bool = True
DIVERGENCE_LOOKBACK: int = 20
DIVERGENCE_BONUS: int = 2

USE_FUNDING_CARRY: bool = True
FUNDING_CARRY_POSITIVE_THRESHOLD: float = 0.0005
FUNDING_CARRY_NEGATIVE_THRESHOLD: float = -0.0005
FUNDING_CARRY_BONUS: int = 1

USE_DYNAMIC_MAX_SIGNALS: bool = True
# [Fix-34] TUNABLE — needs validation, and explicitly flagged as a
# CAPITAL-ALLOCATION decision, not a code-quality fix: raising these caps lets
# more concurrent signals fire, which only helps if downstream position-sizing
# can actually support the new concurrent-signal count. Confirm that before
# deploying — the codebase has no visibility into position-sizing/account
# capital to validate this for you. Originals: MAX_SIGNALS_BULL_TREND=7,
# MAX_SIGNALS_DEFAULT=4 (example +2/+1 bump applied here).
# [ROLLBACK 2026-06-20] Per signal-quality review: this is a sizing lever, not a
# quality lever — tightening it buys breathing room while the Section 8 score
# logic fixes are validated, without touching the actual signal logic. 9/5 -> 7/4.
MAX_SIGNALS_BULL_TREND: int = 7
MAX_SIGNALS_BEAR_TREND: int = 5  # [FIX-AUDIT-D5] separate cap for bear regime (was incorrectly using bull value=7)
MAX_SIGNALS_DEFAULT: int = 4
BREADTH_BULL_THRESHOLD: float = 0.70

USE_FALSE_BREAKOUT_DETECTION: bool = True
FALSE_BREAKOUT_LOOKBACK_BARS: int = 12
FALSE_BREAKOUT_BONUS: int = 1

# [Fix-30] TUNABLE — no backtest data yet, but user/reviewer has explicitly
# confirmed (2026-06-20) that higher signal frequency is wanted, satisfying the
# Section 6 preamble's "confirm before implementing" requirement. Section 6 Item 2:
# lowered ~12% as a starting point pending evidence that the triple-ADX-gate
# combination (this + MIN_DAILY_ADX below) is overly restrictive. Still a hypothesis
# to test against real historical data, not a confirmed improvement — the
# audit conditions the underlying tuning rationale on "if backtest data shows"
# it's warranted, and no such data exists yet (the harness that would have
# produced it, backtest_harness.py, was removed — see Fix-25 changelog entry
# above). Originals: ADX_BREAK_GATE=25.0, ADX_SCORE_MIN=20.0.
# [ROLLBACK 2026-06-20] Per signal-quality review: the original breakout-strength
# bar (in place since v15.1.0 for a documented reason, FIX-M8) was the gate that
# kept weak-trend setups from scoring as high as strong ones in the BREAK path.
# Reverted to originals.
ADX_BREAK_GATE  = 25.0
ADX_SCORE_MIN   = 20.0

# Phase 1 (Fix A) — Overextension/Blow-off Suppression for BREAK
ENABLE_OVEREXTENSION_GATE: bool = True
BREAK_OVEREXT_ATR_MULT: float = 2.0
BREAK_MA_DIST_ATR_MULT: float = 3.5
BREAK_MULTIBAR_ATR_MULT: float = 3.5
BREAK_OVEREXT_LOOKBACK: int = 4

# Phase 2 (Fix F) — Suppress Counter-4H-Trend PULL
ENABLE_COUNTERTREND_PULL_GATE: bool = True

# Phase 3 (Fix C) — Pullback Depth Gate
ENABLE_PULL_DEPTH_GATE: bool = True
PULL_MIN_DEPTH_ATR: float = 0.15

# Phase 4 (Fix D) — PULL Momentum Collapse Penalty
ENABLE_PULL_MOMENTUM_COLLAPSE_PENALTY: bool = True
PULL_RSI_COLLAPSE_THRESH: float = 5.0
PULL_RSI_LOOKBACK: int = 4

# Phase 4 (Fix E) — PULL EMA Velocity Check
ENABLE_PULL_EMA_VELOCITY_CHECK: bool = True

# Phase 5 (Fix B) — BREAK RSI ceiling expansion
ENABLE_BREAK_RSI_WIDENING: bool = True

# Phase 5 (Fix H) — PULL RSI lower-bound tightening
ENABLE_PULL_RSI_TIGHTENING: bool = True

# Phase 6 (Fix G) — VWAP Restoration for Pullback Mode
ENABLE_PULL_VWAP_RESTORATION: bool = True

# Phase 7 (Fix I) — ADX Expanding Check
ENABLE_ADX_EXPANDING_CHECK: bool = True
# [AUDIT-P5-3] 3->2 bars: 45-min lag on 15m reduced to 30-min for a more responsive
# rising-ADX check at scalp timeframe.
ADX_RISING_LOOKBACK: int = 2
ENABLE_SCORE_REBALANCE: bool = True

# Phase 8 (Fix J) — Two-Tier BREAK Engine
ENABLE_EARLY_BREAK_TIER: bool = True
EARLY_BREAK_PRIOR_HIGHS_LOOKBACK: int = 3
EARLY_BREAK_REQUIRE_ADX_RISING: bool = True

# Phase 9 — Quality-Stratified PULL Engine
ENABLE_PULL_TIERING: bool = True

# Cross-cutting items
ENABLE_SR_CLEARANCE_FIX: bool = True
ENABLE_CONTINUATION_PROB_GATE: bool = False

if ENABLE_BREAK_RSI_WIDENING:
    RSI_BREAK_LONG_MIN  = 50.0;  RSI_BREAK_LONG_MAX  = 82.0
    RSI_BREAK_SHORT_MIN = 18.0;  RSI_BREAK_SHORT_MAX = 50.0
else:
    RSI_BREAK_LONG_MIN  = 50.0;  RSI_BREAK_LONG_MAX  = 75.0
    RSI_BREAK_SHORT_MIN = 25.0;  RSI_BREAK_SHORT_MAX = 50.0
# [AUDIT-P4-1] RSI_PULL_LONG_MIN=38 looks like the floor, but ENABLE_PULL_RSI_TIGHTENING
# (Phase 5/Fix H, see _apply_scoring_and_filters) applies a -1 penalty to every PULL long
# with RSI < 44 unconditionally — the 38-43 band always eats that penalty. 44 is therefore
# the real practical floor; 38 only matters for where the penalty starts being the *only*
# gate (vs. Tier C's hard gate, which refuses entirely below 44 when also counter-trend).
# Kept soft/38 rather than hard-gated at 44: the codebase already considered and declined
# a hard-gate version here (see the commented "Hard-gate alternative" directly below
# ENABLE_PULL_RSI_TIGHTENING in _apply_scoring_and_filters) — a non-counter-trend PULL
# long at RSI 40 can still fire, just at -1, rather than being refused outright. This
# preserves the distinction between "discouraged" (soft, non-counter-trend, 38-43) and
# "prohibited" (hard, counter-trend + RSI<44, via Tier C a few hundred lines down).
RSI_PULL_LONG_MIN   = 38.0;  RSI_PULL_LONG_MAX   = 58.0  # [PATCH-7a] 65→58: RSI 59–65 is recovery momentum, not a dip
RSI_PULL_SHORT_MIN  = 38.0;  RSI_PULL_SHORT_MAX  = 62.0
# [Fix-26] NOT changed in this pass — audit Section 5/6 flags these RSI bands as
# candidates for narrowing "if backtest data supports it," but no such data exists
# yet (the harness that would have produced it was removed — see Fix-25).
# Tracked here so it isn't forgotten if that evidence becomes available another
# way. Do not guess at new values without it.
RSI_1H_PULL_LONG_MAX  = 70.0
RSI_1H_PULL_SHORT_MIN = 30.0

VOL_SCORE_MULT  = 0.75
MAX_ATR_PCT     = 10.0
MIN_ATR_PCT     = 0.2
WICK_FILTER     = 0.45
RANGE_PCT_BREAK = 0.20
PULL_ZONE_MULT  = 0.25
# [AUDIT-P5-4] 3->2 bars: tighter entry window, building on [PATCH-7b]'s earlier 5->3 cut.
PULL_TOUCH_LOOKBACK   = 2
PULL_RECOVER_ATR_MULT_TREND: float = 0.50  # [PATCH-7c] 0.25→0.50: require meaningful recovery, not a single tick
PULL_RECOVER_ATR_MULT_MIXED: float = 0.25  # [PATCH-7c] 0.10→0.25: same ratio scaling as trend
# [Fix-29] TUNABLE — needs backtest validation, but user/reviewer has explicitly
# confirmed (2026-06-20) that higher signal frequency is wanted, satisfying the
# Section 6 preamble's "confirm before implementing" requirement. Section 6 Item 1:
# lowered from 2 to 1 bar to increase signal frequency by requiring less trend-hold
# confirmation. This is a frequency/quality tradeoff — still validate against your
# own historical data before trusting the tradeoff (no in-repo harness for
# this anymore — see Fix-25 changelog entry); revert to 2 to undo.
# [ROLLBACK 2026-06-20] Per signal-quality review: single-bar confirmation is
# the easiest setting to whipsaw on. Reverted to 2.
TREND_HOLD_BARS = 2
USE_EXHAUSTION_SHORT:       bool = True
EXHAUSTION_SHORT_SCORE_ADJ: int  = -1
USE_EXHAUSTION_LONG:        bool = True
EXHAUSTION_LONG_SCORE_ADJ:  int  = -1
EXHAUSTION_SPREAD_LOOKBACK: int  = 4

USE_PULLBACK_ALIGNMENT:     bool = True
PULLBACK_SCORE_ADJ:         int  = -1
USE_ROLLING_VWAP  = True
ROLLING_VWAP_LEN  = 16

USE_D200_FILTER   = True
D200_SOFT_ADJ     = 1

USE_DAILY_ADX     = True
# [Fix-30] TUNABLE — same hypothesis-not-evidence caveat as ADX_BREAK_GATE/
# ADX_SCORE_MIN above; user/reviewer has explicitly confirmed (2026-06-20) that
# higher signal frequency is wanted. Original: 18.0.
# [ROLLBACK 2026-06-20] Per signal-quality review: the daily trend filter got
# weakened twice in a row across versions. Reverted to original 18.0.
MIN_DAILY_ADX     = 18.0
PULL_REQUIRES_4H  = True
# [Fix-37] TUNABLE — needs validation. Section 6 Item 10: per-symbol or
# per-BTC-regime-label override for PULL_REQUIRES_4H, rather than a single
# global flip of the default. Keys may be either an exact WATCHLIST symbol
# ("ETHUSDT") or a substring of the BTC regime label (e.g. "Bullish",
# "Bearish", "Mixed" — see compute_btc_regime()'s `label` values) matched via
# `in`; an exact symbol match takes precedence over a regime-label match. Left
# EMPTY by default — this is the override *mechanism*, not the tuning
# decision itself; PULL_REQUIRES_4H above remains the global fallback
# (True, unchanged) until a reviewer populates specific entries. See
# get_pull_requires_4h().
PULL_REQUIRES_4H_OVERRIDE: dict[str, bool] = {}

FUNDING_SUPPRESS_EXTREME: float = 0.0010
SR_CLEARANCE_ATR_MULT: float    = 0.15
SUPPORT_PROXIMITY_ATR: float    = 0.75

BREAK_OI_FLAT_VOL_THRESHOLD: float = 1.5
FUNDING_PULL_WARN_MIN: float        = 0.0005

OI_HISTORY_DEPTH: int        = 6
OI_CHANGE_THRESHOLD_PCT: float = 1.0

BREAK_VOL_MULT: float = 1.2
BREAK_VOL_ACCEL_BARS: int = 2

RS_TOP_PERCENTILE: float    = 0.20
RS_BOTTOM_PERCENTILE: float = 0.20
RS_BREAK_HARD_GATE_PCT: float = -6.0
# [Fix-32] TUNABLE — needs validation; this is a hard-suppress-to-soft-penalty
# CONVERSION, a real behavior change, not a pure config tweak. A BREAK signal
# with rs_pct < RS_BREAK_HARD_GATE_PCT previously got an outright early
# `return res` (fire_long/fire_short forced False, zero chance of firing).
# It now instead receives this score penalty via the normal adjs/adjusted_score
# path (see the RS GATE check below), so it discounts rather than outright
# blocks, and flows through the existing [Fix-9] PENALTY STACKING CAP /
# PENALTY_PRIORITY logic like every other adjustment.
RS_BREAK_HARD_GATE_PENALTY: int = -3
RS_BREAK_SOFT_PERCENTILE: float = 0.25

BREADTH_WEAK_LONG_THRESHOLD:  float = 0.20
BREADTH_WEAK_SHORT_THRESHOLD: float = 0.80
# [AUDIT-P6-1] These two were declared but never read anywhere in the file before this
# pass — an orphaned gate that was planned but never wired up. Tier C (Phase 9, PULL
# section) already hard-gates PULL at breadth>0.92 / <0.08, but BREAK only ever got the
# softer -2 BREADTH_RS_COMPOUND_PENALTY-style treatment via other paths; nothing hard-gated
# BREAK at extreme breadth. Now wired into _apply_scoring_and_filters' BREAK block (mirrors
# Tier C's intent for BREAK signals) instead of introducing new duplicate constants.
BREADTH_BREAK_LONG_SUPPRESS:  float = 0.95   # suppress BREAK long above this breadth
BREADTH_BREAK_SHORT_SUPPRESS: float = 0.05   # suppress BREAK short below this breadth
BREADTH_CROWDED_LONG_THRESHOLD: float = 0.75
BREADTH_CROWDED_LONG_THRESHOLD_MIXED: float = 0.80
BREADTH_EXTREME_LONG_THRESHOLD: float = 0.90
BREADTH_EXTREME_SHORT_THRESHOLD: float = 0.10
BREADTH_RS_COMPOUND_PENALTY: int   = -2
BREADTH_RS_NEGATIVE_THRESH: float  = 0.0
# [AUDIT-P6-4] Named constants for Tier C's (Phase 9, PULL hard-gate) breadth-extreme
# checks — these were previously hardcoded as bare 0.92 / 0.08 inline, inconsistent with
# (and easy to confuse for) BREADTH_EXTREME_LONG_THRESHOLD=0.90 / BREADTH_EXTREME_SHORT_
# THRESHOLD=0.10 above, which are a DIFFERENT, softer mechanism (a -2 score penalty via
# apply_breadth_adjustment, not a hard refuse). Values kept at their original 0.92/0.08 —
# this is a naming/consistency fix only, not a threshold change. Also distinct from
# BREADTH_BREAK_LONG_SUPPRESS/_SHORT_SUPPRESS (0.95/0.05), which gate BREAK signals, not PULL.
TIER_C_BREADTH_EXTREME_LONG_THRESHOLD: float = 0.92
TIER_C_BREADTH_EXTREME_SHORT_THRESHOLD: float = 0.08

PULL_VOL_FLOOR: float          = 0.40
PULL_VOL_FLOOR_OVERBOUGHT: float  = 0.50
PULL_VOL_OVERBOUGHT_BREADTH: float = 0.80
PULL_REENTRY_COOLDOWN_S: int   = 900

# [AUDIT-P6-5] -5.0 -> -2.0. Was lower than RS_PULL_HARD_GATE_PCT (-4.0, a local var inside
# the PULL RS-gate branch in _apply_scoring_and_filters, ~line 3459) — a PULL signal at e.g.
# RS=-4.5% already eats the -2 "RS strongly negative on PULL" penalty (rs_pct < -4.0) but
# was STILL eligible for the +1 Support/Resistance proximity bonus below (rs_pct >= -5.0),
# partially offsetting the penalty in the same scoring pass. Raised so the proximity bonus
# only fires on genuinely positive-ish RS momentum, not RS that's already been flagged as
# weak elsewhere. Constraint: PROXIMITY_RS_MIN must stay >= RS_PULL_HARD_GATE_PCT (currently
# -4.0) — if that local var's value changes, re-check this constant against it.
PROXIMITY_RS_MIN: float = -2.0
TP1_WALL_MIN_CLEARANCE: float = 0.40

PULL_LIMIT_TRANCHE_PCT: float  = 0.50
PULL_LIMIT_MAX_ATR_DIST: float = 1.50

# [Fix-36] Section 6 Item 9 ("relax spread checks for additional symbols") is
# implemented via SPREAD_DYNAMIC_AVG_THRESHOLD below, NOT by widening these two
# global thresholds. SPREAD_WARN_PCT/SPREAD_SUPPRESS_PCT are a risk control the
# audit otherwise validated as sound (protects against trading illiquid/wide-
# spread symbols) and apply uniformly to every non-exempt symbol regardless of
# its own actual liquidity — widening them globally would relax that
# protection even for symbols that deserve it. is_spread_exempt_dynamic()
# already gives a less risky, per-symbol, data-driven path to the same
# "more symbols get relaxed treatment" goal: a symbol earns exemption based on
# its OWN observed recent spread history, not a uniform global cutoff. Left
# UNCHANGED here.
SPREAD_WARN_PCT: float     = 0.20
SPREAD_SUPPRESS_PCT: float = 0.40
SPREAD_EXEMPT: set[str] = {
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT",
    "XRPUSDT", "ADAUSDT", "DOGEUSDT",
}
SPREAD_DYNAMIC_HISTORY_LEN: int = 10
# [Fix-36] TUNABLE — needs validation. Widened from 0.10 so more symbols can
# qualify for is_spread_exempt_dynamic()'s per-symbol exemption based on their
# own recent average spread, instead of relaxing SPREAD_WARN_PCT/
# SPREAD_SUPPRESS_PCT globally above. Still trades off against the audit's
# validated spread risk control — a wider threshold means a symbol with a
# moderately-wide-but-not-extreme average spread can now skip the warn/suppress
# checks entirely, not just get a smaller penalty. Original: 0.10.
SPREAD_DYNAMIC_AVG_THRESHOLD: float = 0.15

WIN_RATE_MIN_SAMPLE: int    = 20
WIN_RATE_HIGH_THRESH: float = 0.65
WIN_RATE_LOW_THRESH: float  = 0.45
# [AUDIT-P6-2] 80->40: lowers the threshold that activates adaptive scoring (see
# compute_win_rate_analytics, ~line 1773) sooner. 40 is statistically modest but practical —
# this is a TUNABLE, not a backtested value; revisit if adaptive adjustments start firing
# on too-thin samples.
WIN_RATE_MIN_SAMPLE_FOR_ADJ: int = 40
# [Fix-28] TUNABLE — needs validation. Suggested 14 per audit. See
# compute_win_rate_analytics' staleness guard.
WIN_RATE_STALE_DAYS: int = 14
WIN_RATE_LOOKBACK_DAYS:  int   = 30
WIN_RATE_RECENT_DAYS:    int   = 7
WIN_RATE_RECENT_WEIGHT:  float = 2.0

MAX_OI_SCALE: float = 3.0
# [Fix-24] TUNABLE — needs validation. Threshold (in ATR multiples) above which
# find_sr_levels()'s nearest level and the liquidity engine's nearest external
# level are considered to "materially disagree" for logging purposes only.
SR_LIQUIDITY_DISAGREEMENT_ATR_MULT: float = 1.0
# [Fix-42] TUNABLE — needs validation. Disagreement beyond this (much larger)
# threshold means the two structure systems aren't just using different
# clustering tolerances — they don't agree on which side of price the nearest
# real level even is. Past this point it's not noise, it's a sign the
# structure read for this symbol/scan is unreliable, so it now costs real
# score (see SR_LIQUIDITY_DISAGREEMENT_PENALTY) instead of just being logged.
SR_LIQUIDITY_DISAGREEMENT_SEVERE_ATR_MULT: float = 3.0
SR_LIQUIDITY_DISAGREEMENT_PENALTY: int = -3
# [Fix-18] TUNABLE — needs validation. Hard cutoff for OI staleness in
# compute_oi_trend(): past this many seconds since the last OI reading, treat OI
# as "unknown" instead of damped-but-nonzero. 45 minutes = 3x OI_EXPECTED_INTERVAL_S.
OI_STALE_CUTOFF_S: float = 45 * 60
# [Fix-31] TUNABLE — needs validation. Widened the R:R filter's tolerance
# modestly downward so marginally-sub-1:1 setups aren't auto-rejected.
# Original: 0.9.
# [ROLLBACK 2026-06-20] Per signal-quality review: this hits expectancy
# directly — below 1:1 R:R you need a much higher win rate just to break even.
# Held at 1.0 for 3+ versions before the 0.8 drop. Reverted to 1.0.
MIN_RR_RATIO: float = 1.0  # [FIX-AUDIT-D2] rollback comment said 1.0; value corrected to match
ATR_FALLBACK_PCT: float = 0.30

MACRO_WINDOW_BEFORE_MINS: int   = 60
MACRO_WINDOW_AFTER_MINS:  int   = 30
MACRO_HIGH_ATR_SUPPRESS_PCT: float = 3.0
MACRO_CACHE_TTL_S: int          = 3600
MACRO_ATR_PCTILE_HIGH: float = 0.70
MACRO_ATR_PCTILE_HIGH_MULT: float = 1.5

N_15M = 300
N_1H  = 200
N_4H  = 200
N_1D  = 600

# [AUDIT-P7-4] Named indices for the (candles_15m, candles_1h, candles_4h, candles_d) tuple
# returned by fetch_all_candles() / collect_market_inputs() and stored as candle_bundles[sym]
# in main(). Both call sites that index into this tuple (main()'s BTCUSDT 4H block, and
# compute_pairwise_correlation_matrix's candle_idx default) relied on the bare index 2 for
# "4H" — fragile if fetch_all_candles' return order ever changes. Used with an assertion at
# the actual unpack site to catch an order mismatch early instead of silently feeding the
# wrong timeframe into BTC-correlation calculations.
CANDLE_BUNDLE_IDX_15M, CANDLE_BUNDLE_IDX_1H, CANDLE_BUNDLE_IDX_4H, CANDLE_BUNDLE_IDX_1D = 0, 1, 2, 3

REACT_TP1           = "🔥"
REACT_TP2           = "🏆"
REACT_SL            = "😭"
SIGNAL_MAX_AGE_BARS = 48

HL_INFO_URL = "https://api.hyperliquid.xyz/info"

INTERVAL_MS = {
    "15m": 15 * 60 * 1000,
    "1h":  60 * 60 * 1000,
    "4h":  4  * 60 * 60 * 1000,
    "1d":  24 * 60 * 60 * 1000,
}

PULL_BODY_MIN_RATIO: float = 0.20
H4_STALE_AGE_FRACTION: float = 0.85
H4_STALE_SPREAD_MIN:   float = 0.15
OI_ACCEL_MIN_THRESHOLD: float = 1.0
OI_SCORE_CAP: int              = 2
SESSION_DEAD_ZONE_START_UTC: int = 3
SESSION_DEAD_ZONE_END_UTC:   int = 6
SESSION_LOW_ATR_PERCENTILE:  float = 0.10
TP1_MULT_BREAK: float = 1.2   # [v17.0.2] 1.0->1.2: widen TP1 to preserve RR after SL widening
TP2_MULT_BREAK: float = 2.5
SL_MULT_BREAK:  float = 1.2   # [v17.0.2] 0.85->1.2: wider SL to survive price spikes/wicks before reversal
TP1_MULT_PULL:  float = 1.1   # [v17.0.2] 1.0->1.1: widen TP1 to preserve RR after SL widening
TP2_MULT_PULL:  float = 2.0
SL_MULT_PULL:   float = 1.1   # [v17.0.2] 0.85->1.1: wider SL to survive price spikes/wicks before reversal
HIGH_ATR_THRESHOLD: float = 3.0
SL_HIGH_ATR_MULT:   float = 1.25  # [v17.0.2] 0.90->1.25: high-vol regime needs wider SL, not tighter
ATR_HIST_DEPTH:     int   = 48
ATR_LOW_PERCENTILE:  float = 0.10
ATR_HIGH_PERCENTILE: float = 0.90
# [AUDIT-P5-2] 4->2 bars: 60-min lag on 15m was too slow for scalp-timeframe strength
# detection; 30-min lag is more responsive. Real constant name is EMA_VELOCITY_LOOKBACK
# (no "_BARS" suffix) — used in both the Tier A pull-quality check (_apply_scoring_and_filters,
# tier_a long/short) and the EMA velocity penalty block (~line 3437 area, _ef_velocity calc).
EMA_VELOCITY_LOOKBACK:   int   = 2
EMA_VELOCITY_STRONG_MIN: float = 0.05
EMA_VELOCITY_WEAK_MAX:   float = 0.005
RSI_4H_PULL_LONG_MAX:  float = 70.0
RSI_4H_PULL_SHORT_MIN: float = 30.0
FUNDING_HISTORY_DEPTH: int = 4

FUNDING_WARN_EXTREME = 0.0010
FUNDING_WARN_HIGH    = 0.0005

USE_REGIME_AWARE_TP_SL: bool = True
REGIME_HIGH_VOL_TP1_MULT: float = 1.2
REGIME_HIGH_VOL_TP2_MULT: float = 1.15
REGIME_HIGH_VOL_SL_MULT: float  = 1.15  # [v17.0.2] 0.95->1.15: was incorrectly tightening SL in high-vol; now widens it
REGIME_LOW_VOL_TP1_MULT: float  = 0.85
REGIME_LOW_VOL_TP2_MULT: float  = 0.90
REGIME_LOW_VOL_SL_MULT: float   = 1.10
REGIME_BEAR_TP1_MULT: float = 0.9
REGIME_BULL_TP2_MULT: float = 1.1

LOW_BTC_CORR_THRESHOLD: float = 0.35
LOW_BTC_CORR_LOOKBACK_BARS: int = 42
RS_BEARISH_REGIME_EXEMPT_PCT: float = 3.0

LEVERAGE_BASE_RISK_PCT: float = 10.0
LEVERAGE_RANGE_LOW_RISK_PCT: float = 5.0
LEVERAGE_RANGE_HIGH_RISK_PCT: float = 15.0
LEVERAGE_MAX: float = 15.0

SR_LOOKBACK_BARS: int = 200

# [Fix-35] TUNABLE — needs validation. SIGNAL_COOLDOWN_BARS is already at the
# practical floor of 1 and is deliberately left there, NOT lowered to 0: 0
# would mean re-entry is allowed on the very next bar with no separation at
# all, which is a qualitatively different (and untested) behavior from "lower
# the cooldown a bit" — flagging this explicitly rather than silently zeroing
# it. SIGNAL_COOLDOWN_BARS_HIGHSCORE is lowered to match (was 2), removing the
# extra bar of cooldown previously required for high-score signals specifically.
# Original: SIGNAL_COOLDOWN_BARS_HIGHSCORE=2.
# [ROLLBACK 2026-06-20] Per signal-quality review: re-firing the same
# symbol/direction 15 min later just compounds a wrong read rather than
# confirming it. Both reverted to 2.
SIGNAL_COOLDOWN_BARS:           int = 2
SIGNAL_COOLDOWN_BARS_HIGHSCORE: int = 2
SIGNAL_HIGHSCORE_THRESHOLD:     int = 7
SIGNAL_COOLDOWN_POST_WIN_BARS:  int = 1

STATE_VERSION = 4

# ═══════════════════════════════════════════════════════════════
# HYPERLIQUID DATA FETCHING
# ═══════════════════════════════════════════════════════════════

def hl_coin(symbol: str) -> str:
    return symbol.replace("USDT", "")

def hl_post(payload: dict):
    global _hl_last_request_ts, _hl_min_interval_s, _hl_consecutive_successes
    max_attempts  = int(os.getenv("HL_MAX_ATTEMPTS", "6"))
    base_sleep_s  = float(os.getenv("HL_BASE_SLEEP_S", "0.75"))
    for attempt in range(max_attempts):
        try:
            with _hl_request_lock:
                now     = time.time()
                elapsed = now - _hl_last_request_ts
                wait_s  = _hl_min_interval_s - elapsed
                if wait_s > 0:
                    time.sleep(wait_s)
                _hl_last_request_ts = time.time()

            r = _hl_session.post(
                HL_INFO_URL,
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=15,
            )
            if r.status_code == 429:
                with _hl_request_lock:
                    _hl_min_interval_s = min(
                        HL_MIN_INTERVAL_MAX_S, _hl_min_interval_s * 1.25 + 0.02
                    )
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
        except Exception as e:
            if attempt == max_attempts - 1:
                raise
            sleep_s = min(20.0, base_sleep_s * (2 ** attempt)) + random.uniform(0.0, 0.25)
            time.sleep(sleep_s)

    raise RuntimeError("hl_post exhausted all retries (likely persistent 429)")

def current_bar_open_ms(reference_ms: int, interval: str) -> int:
    iv_ms = INTERVAL_MS.get(interval, 60 * 60 * 1000)
    return (reference_ms // iv_ms) * iv_ms

def filter_closed_candles(candles: list[dict], interval: str, reference_ms: int) -> list[dict]:
    cutoff = current_bar_open_ms(reference_ms, interval)
    return [c for c in candles if c["t"] < cutoff]

def get_candles(symbol: str, interval: str, n: int,
                start_time_ms: int | None = None,
                reference_ms: int | None = None) -> list[dict]:
    coin   = hl_coin(symbol)
    iv_ms  = INTERVAL_MS.get(interval, 60 * 60 * 1000)
    ref_ms = int(time.time() * 1000) if reference_ms is None else reference_ms
    end_ms = current_bar_open_ms(ref_ms, interval)

    computed_start_ms = start_time_ms if start_time_ms is not None else end_ms - iv_ms * (n + 10)

    payload = {
        "type": "candleSnapshot",
        "req": {
            "coin":      coin,
            "interval":  interval,
            "startTime": computed_start_ms,
            "endTime":   end_ms,
        },
    }
    raw = hl_post(payload)
    if raw is None:
        return []
    candles = []
    for c in raw:
        base_v  = float(c["v"])
        quote_v = float(c["q"]) if c.get("q") is not None else base_v
        candles.append({
            "t": int(c["t"]),   "o": float(c["o"]),
            "h": float(c["h"]), "l": float(c["l"]),
            "c": float(c["c"]), "v": base_v, "qv": quote_v,
        })
    candles = filter_closed_candles(candles, interval, ref_ms)
    return candles[-n:]

def fetch_all_candles(symbol: str, reference_ms: int | None = None) -> tuple[list, list, list, list] | None:
    candles_15m = get_candles(symbol, "15m", N_15M, reference_ms=reference_ms)
    if len(candles_15m) < 50:
        return None
    results = {}
    with ThreadPoolExecutor(max_workers=max(1, HL_TF_WORKERS)) as ex:
        futures = {
            ex.submit(get_candles, symbol, "1h", N_1H, None, reference_ms): "1h",
            ex.submit(get_candles, symbol, "4h", N_4H, None, reference_ms): "4h",
            ex.submit(get_candles, symbol, "1d", N_1D, None, reference_ms): "1d",
        }
        for fut in as_completed(futures):
            tf = futures[fut]
            try:
                results[tf] = fut.result()
            except Exception as e:
                print(f"  [CANDLES] {symbol} {tf} fetch failed: {e} — skipping symbol")
                return None
    if not all(k in results for k in ("1h", "4h", "1d")):
        return None
    return candles_15m, results["1h"], results["4h"], results["1d"]

# ═══════════════════════════════════════════════════════════════
# FUNDING RATE + OPEN INTEREST
# ═══════════════════════════════════════════════════════════════

_meta_cache: dict | None = None
_meta_cache_lock = threading.Lock()
_meta_cache_fetched_at: float = 0.0
META_CACHE_TTL_S: float = 55.0

def get_meta_and_asset_ctxs() -> dict | None:
    global _meta_cache, _meta_cache_fetched_at

    with _meta_cache_lock:
        age = time.time() - _meta_cache_fetched_at
        if _meta_cache is not None and age < META_CACHE_TTL_S:
            return _meta_cache

    try:
        data       = hl_post({"type": "metaAndAssetCtxs"})
        if data is None:
            return _meta_cache
        meta       = data[0]
        asset_ctxs = data[1]
        universe   = meta.get("universe", [])
        cache = {}
        for i, asset in enumerate(universe):
            name = asset.get("name", "")
            if not name:
                continue
            ctx      = asset_ctxs[i]
            funding  = float(ctx["funding"])       if ctx.get("funding")      is not None else None
            oi_coins = float(ctx["openInterest"])  if ctx.get("openInterest") is not None else None
            mark     = float(ctx["markPx"])        if ctx.get("markPx")       is not None else None
            cache[name] = {"funding": funding, "open_interest_coins": oi_coins, "mark_px": mark}

        with _meta_cache_lock:
            _meta_cache = cache
            _meta_cache_fetched_at = time.time()
        return _meta_cache
    except Exception as e:
        print(f"  [META CACHE] fetch failed: {e}")
        with _meta_cache_lock:
            return _meta_cache

def get_market_context(symbol: str) -> dict | None:
    coin  = hl_coin(symbol)
    cache = get_meta_and_asset_ctxs()
    if cache is None or coin not in cache:
        return None
    entry    = cache[coin]
    funding  = entry.get("funding")
    oi_coins = entry.get("open_interest_coins")
    mark     = entry.get("mark_px")
    oi_usd   = oi_coins * mark if (oi_coins is not None and mark is not None) else None
    return {"funding": funding, "open_interest": oi_usd, "open_interest_coins": oi_coins}

# ═══════════════════════════════════════════════════════════════
# STATE MANAGEMENT
# ═══════════════════════════════════════════════════════════════

_state_lock = threading.RLock()

def update_funding_history(state: dict, symbol: str, rate: float | None) -> None:
    if rate is None:
        return
    with _state_lock:
        hist = state.setdefault("funding_history", {}).setdefault(symbol, [])
        hist.append({"ts": int(time.time()), "rate": rate})
        state["funding_history"][symbol] = hist[-FUNDING_HISTORY_DEPTH:]

def get_funding_trend(state: dict, symbol: str) -> str:
    with _state_lock:
        hist = list(state.get("funding_history", {}).get(symbol, []))
    if len(hist) < 2:
        return "stable"
    recent = hist[-1]["rate"]
    prior  = hist[-2]["rate"]
    delta  = recent - prior
    if delta > 0.0001:
        return "rising"
    if delta < -0.0001:
        return "falling"
    return "stable"

def update_oi_history(state: dict, symbol: str, oi_usd: float | None) -> None:
    if oi_usd is None:
        return
    with _state_lock:
        oi_hist     = state.setdefault("oi_history", {})
        symbol_hist = oi_hist.setdefault(symbol, [])
        symbol_hist.append({"ts": int(time.time()), "oi": oi_usd})
        oi_hist[symbol] = symbol_hist[-OI_HISTORY_DEPTH:]

def compute_oi_trend(state: dict, symbol: str, current_price: float,
                     price_direction: str, trade_direction: str) -> dict:
    with _state_lock:
        oi_hist = list(state.get("oi_history", {}).get(symbol, []))
    if len(oi_hist) < 2:
        return {
            "oi_trend": "unknown", "oi_change_pct": None,
            "oi_acceleration": None, "score_adj": 0,
            "label": "OI Trend: Unknown", "breakdown_tag": "OI?",
            "condition": "Unknown",
        }
    recent = oi_hist[-1]["oi"]
    prior  = oi_hist[-2]["oi"]
    if prior == 0:
        return {
            "oi_trend": "unknown", "oi_change_pct": None,
            "oi_acceleration": None, "score_adj": 0,
            "label": "OI Trend: Unknown", "breakdown_tag": "OI?",
            "condition": "Unknown",
        }
    raw_change_pct = (recent - prior) / prior * 100.0
    elapsed_s      = max(1.0, oi_hist[-1]["ts"] - oi_hist[-2]["ts"])
    # [Fix-18] TUNABLE — needs validation. The scale factor below damps stale OI
    # readings (elapsed_s much larger than OI_EXPECTED_INTERVAL_S) but never zeroes
    # them out, no matter how old — a reading from hours ago could still produce a
    # nonzero, non-"unknown" score_adj. Add a hard cutoff: past OI_STALE_CUTOFF_S,
    # treat it the same as having insufficient history at all.
    if elapsed_s > OI_STALE_CUTOFF_S:
        return {
            "oi_trend": "unknown", "oi_change_pct": None,
            "oi_acceleration": None, "score_adj": 0,
            "label": "OI Trend: Unknown (stale)", "breakdown_tag": "OI?",
            "condition": "Unknown",
        }
    # [FIX-AUDIT-D12] floor elapsed to at least 1/3 of expected interval to limit amplification
    MIN_ELAPSED_SCALE_FLOOR = OI_EXPECTED_INTERVAL_S / MAX_OI_SCALE   # = 300s when MAX_OI_SCALE=3
    effective_elapsed = max(elapsed_s, MIN_ELAPSED_SCALE_FLOOR)
    scale          = min(MAX_OI_SCALE, OI_EXPECTED_INTERVAL_S / effective_elapsed)
    oi_change_pct  = raw_change_pct * scale

    oi_acceleration = None
    if len(oi_hist) >= 3:
        prior2 = oi_hist[-3]["oi"]
        if prior2 != 0:
            prev_elapsed = max(1.0, oi_hist[-2]["ts"] - oi_hist[-3]["ts"])
            prev_raw     = (prior - prior2) / prior2 * 100.0
            prev_scale   = min(MAX_OI_SCALE, OI_EXPECTED_INTERVAL_S / prev_elapsed)
            prev_change  = prev_raw * prev_scale
            oi_acceleration = oi_change_pct - prev_change

    oi_rising  = oi_change_pct >  OI_CHANGE_THRESHOLD_PCT
    oi_falling = oi_change_pct < -OI_CHANGE_THRESHOLD_PCT

    if oi_rising:
        oi_trend = "rising"
    elif oi_falling:
        oi_trend = "falling"
    else:
        oi_trend = "flat"

    bullish_confirm = price_direction == "up"   and oi_rising
    bearish_confirm = price_direction == "down" and oi_rising
    bullish_div     = price_direction == "up"   and oi_falling
    bearish_div     = price_direction == "down" and oi_falling

    if trade_direction == "long":
        if bullish_confirm:
            score_adj, condition, breakdown_tag = 1,  "Bullish Confirmation",                     "OI↑"
        elif bearish_confirm:
            score_adj, condition, breakdown_tag = -1, "Counter vs Long (OI rising on down move)", "OI Divergence"
        elif bullish_div:
            score_adj, condition, breakdown_tag = 0,  "Neutral (longs unwinding)",                "OI→"
        elif bearish_div:
            score_adj, condition, breakdown_tag = 0,  "Neutral (long-covering)",                  "OI→"
        else:
            score_adj, condition, breakdown_tag = 0,  "Neutral",                                  "OI→"
    else:
        if bearish_confirm:
            score_adj, condition, breakdown_tag = 1,  "Bearish Confirmation",                      "OI↑"
        elif bullish_confirm:
            score_adj, condition, breakdown_tag = -1, "Counter vs Short (OI rising on up move)",   "OI Divergence"
        elif bullish_div:
            score_adj, condition, breakdown_tag = -1, "Shorts covering (OI falling on up move)",   "OI↓"
        elif bearish_div:
            score_adj, condition, breakdown_tag = 0,  "Neutral (long-covering)",                   "OI→"
        else:
            score_adj, condition, breakdown_tag = 0,  "Neutral",                                   "OI→"

    label = (
        f"OI Trend: {oi_trend.capitalize()}  "
        f"OI Change: {oi_change_pct:+.1f}% (norm)"
    )
    return {
        "oi_trend":        oi_trend,
        "oi_change_pct":   oi_change_pct,
        "oi_acceleration": oi_acceleration,
        "score_adj":       score_adj,
        "label":           label,
        "breakdown_tag":   breakdown_tag,
        "condition":       condition,
    }

def format_funding(rate: float | None, direction: str) -> str:
    if rate is None:
        return "Funding: n/a"
    pct    = rate * 100
    per_8h = f"{pct:+.4f}%/8h"
    headwind = (rate > 0 and direction == "long") or (rate < 0 and direction == "short")
    tailwind = (rate < 0 and direction == "long") or (rate > 0 and direction == "short")
    abs_rate = abs(rate)
    if headwind and abs_rate >= FUNDING_WARN_EXTREME:
        tag = "⚠️ EXTREME — longs paying heavily" if direction == "long" else "⚠️ EXTREME — shorts paying heavily"
    elif headwind and abs_rate >= FUNDING_WARN_HIGH:
        tag = "⚡ elevated against trade"
    elif tailwind and abs_rate >= FUNDING_WARN_HIGH:
        tag = "✅ in your favour"
    else:
        tag = "✅ neutral"
    return f"Funding: {per_8h}  {tag}"

def format_oi(oi_usd: float | None) -> str:
    if oi_usd is None:
        return "OI: n/a"
    if oi_usd >= 1_000_000_000:
        return f"OI: ${oi_usd / 1_000_000_000:.2f}B"
    if oi_usd >= 1_000_000:
        return f"OI: ${oi_usd / 1_000_000:.1f}M"
    return f"OI: ${oi_usd:,.0f}"

# ── S/R LEVELS ──────────────────────────────────────────────

SR_PIVOT_LEFT_BARS  = 3
SR_PIVOT_RIGHT_BARS = 3
SR_CLUSTER_ATR_MULT = 0.3

def _cluster_levels(pivots: list[float], atr_val: float, tolerance: float = 0.3) -> list[float]:
    if not pivots or atr_val <= 0:
        return pivots
    zones: list[float] = []
    zone_members: list[list[float]] = []
    for p in sorted(pivots):
        if zones and abs(p - zones[-1]) < atr_val * tolerance:
            zone_members[-1].append(p)
            members = zone_members[-1]
            zones[-1] = sorted(members)[len(members) // 2]
        else:
            zones.append(p)
            zone_members.append([p])
    return zones

def find_sr_levels(candles_15m: list[dict], n_levels: int = 2,
                   atr_val: float | None = None) -> tuple[list[float], list[float]]:
    cur          = candles_15m[-1]["c"]
    pivots_high  = []
    pivots_low   = []
    lb = SR_PIVOT_LEFT_BARS
    rb = SR_PIVOT_RIGHT_BARS
    avail  = len(candles_15m) - 1
    window = candles_15m[max(0, avail - SR_LOOKBACK_BARS): avail]
    for i in range(lb, len(window) - rb):
        h = window[i]["h"]
        l = window[i]["l"]
        if all(h > window[i - k]["h"] for k in range(1, lb + 1)) and \
           all(h > window[i + k]["h"] for k in range(1, rb + 1)):
            pivots_high.append(h)
        if all(l < window[i - k]["l"] for k in range(1, lb + 1)) and \
           all(l < window[i + k]["l"] for k in range(1, rb + 1)):
            pivots_low.append(l)

    eff_atr = atr_val if atr_val and atr_val > 0 else (cur * 0.005)
    pivots_high = _cluster_levels(pivots_high, eff_atr, SR_CLUSTER_ATR_MULT)
    pivots_low  = _cluster_levels(pivots_low,  eff_atr, SR_CLUSTER_ATR_MULT)

    resistances = sorted([p for p in pivots_high if p > cur], key=lambda x: x - cur)[:n_levels]
    supports    = sorted([p for p in pivots_low  if p < cur], key=lambda x: cur - x)[:n_levels]
    return supports, resistances

# ═══════════════════════════════════════════════════════════════
# BTC MARKET REGIME FILTER
# ═══════════════════════════════════════════════════════════════

_btc_regime_cache: dict | None = None
_btc_regime_lock  = threading.Lock()

def compute_btc_regime(candles_1h: list[dict], candles_4h: list[dict],
                       candles_1d: list[dict] | None = None) -> dict:
    def _arr(candles):
        return [c["c"] for c in candles]

    c4h = _arr(candles_4h)
    c1h = _arr(candles_1h)

    ef4h = safe(ema(c4h, FAST_LEN)[-1])
    es4h = safe(ema(c4h, SLOW_LEN)[-1])
    ef1h = safe(ema(c1h, FAST_LEN)[-1])
    es1h = safe(ema(c1h, SLOW_LEN)[-1])

    btc_4h_momentum = len(c4h) >= 6 and c4h[-2] > c4h[-5]

    btc_bullish = (ef4h > es4h) and (ef1h > es1h) and btc_4h_momentum
    btc_bearish_intraday = (ef4h < es4h) and (ef1h < es1h)

    # Daily confirmation: only treat as confirmed bearish if daily EMA21 < EMA50 as well
    btc_daily_bearish = False
    if candles_1d and len(candles_1d) >= SLOW_LEN:
        c1d = _arr(candles_1d)
        ef1d = safe(ema(c1d, FAST_LEN)[-1])
        es1d = safe(ema(c1d, SLOW_LEN)[-1])
        btc_daily_bearish = ef1d < es1d

    # Bearish requires both intraday structure AND daily trend confirmation
    btc_bearish = btc_bearish_intraday and btc_daily_bearish
    # Intraday-only bearish (4H/1H bear but daily still bullish) → treat as Mixed
    btc_intraday_only_bearish = btc_bearish_intraday and not btc_daily_bearish

    if btc_bullish:
        label = "BTC Regime: Bullish"
    elif btc_bearish:
        label = "BTC Regime: Bearish (confirmed)"
    elif btc_intraday_only_bearish:
        label = "BTC Regime: Mixed (4H dip, daily intact)"
    else:
        label = "BTC Regime: Mixed"

    return {
        "bullish": btc_bullish,
        "bearish": btc_bearish,
        "intraday_bearish": btc_bearish_intraday,
        "label": label,
    }

def set_btc_regime(regime: dict):
    global _btc_regime_cache
    with _btc_regime_lock:
        _btc_regime_cache = regime

def get_btc_regime() -> dict | None:
    with _btc_regime_lock:
        return _btc_regime_cache

LOW_BTC_CORR_BASELINE: set[str] = {"TAOUSDT", "TRXUSDT", "PENDLEUSDT", "ONDOUSDT", "HYPEUSDT"}
_dynamic_low_btc_corr: set[str] = set()
_dynamic_corr_lock = threading.Lock()

def update_dynamic_btc_correlation(symbol: str, candles_4h: list[dict], btc_candles_4h: list[dict] | None) -> None:
    if btc_candles_4h is None or len(btc_candles_4h) < LOW_BTC_CORR_LOOKBACK_BARS + 2:
        return
    if len(candles_4h) < LOW_BTC_CORR_LOOKBACK_BARS + 2:
        return

    n = LOW_BTC_CORR_LOOKBACK_BARS
    sym_closes = [c["c"] for c in candles_4h[-(n + 1):]]
    btc_closes = [c["c"] for c in btc_candles_4h[-(n + 1):]]

    sym_returns = [(sym_closes[i] - sym_closes[i - 1]) / sym_closes[i - 1]
                   for i in range(1, len(sym_closes)) if sym_closes[i - 1] != 0]
    btc_returns = [(btc_closes[i] - btc_closes[i - 1]) / btc_closes[i - 1]
                   for i in range(1, len(btc_closes)) if btc_closes[i - 1] != 0]

    if len(sym_returns) < 10 or len(btc_returns) < 10:
        return

    min_len = min(len(sym_returns), len(btc_returns))
    sym_returns = sym_returns[:min_len]
    btc_returns = btc_returns[:min_len]

    mean_s = sum(sym_returns) / min_len
    mean_b = sum(btc_returns) / min_len

    cov = sum((s - mean_s) * (b - mean_b) for s, b in zip(sym_returns, btc_returns)) / min_len
    var_s = sum((s - mean_s) ** 2 for s in sym_returns) / min_len
    var_b = sum((b - mean_b) ** 2 for b in btc_returns) / min_len

    if var_s <= 0 or var_b <= 0:
        return

    corr = cov / (math.sqrt(var_s) * math.sqrt(var_b))

    with _dynamic_corr_lock:
        if abs(corr) < LOW_BTC_CORR_THRESHOLD:
            _dynamic_low_btc_corr.add(symbol)
        else:
            _dynamic_low_btc_corr.discard(symbol)

def get_low_btc_corr_set() -> set[str]:
    with _dynamic_corr_lock:
        if _dynamic_low_btc_corr:
            return _dynamic_low_btc_corr.copy()
    return LOW_BTC_CORR_BASELINE.copy()

# ═══════════════════════════════════════════════════════════════
# [Fix-33] DATA-DRIVEN CORRELATION MATRIX — Section 6 Item 6
# DIAGNOSTIC LOGGING ONLY. Does NOT replace CORR_GROUPS["layer1"]'s
# hand-picked membership, and is NOT consulted by deduplicate_correlated().
# CORR_GROUPS gates concurrent correlated-signal exposure (a risk control,
# not just a cosmetic grouping), and a single scan's pairwise-correlation
# snapshot is too thin a sample to auto-drive that without a reviewer looking
# at it across multiple scans first. This generalizes the existing
# update_dynamic_btc_correlation() (vs.-BTC-only) pairwise-correlation
# approach above to all-pairs-within-a-group, using the same 4H candle
# history already fetched per symbol in Phase 1 of main() — no new data
# fetching required. Audit Section 6 Item 6 explicitly calls for NOT
# inventing the actual "layer1" sub-group membership; this hands a reviewer
# the data to make that call across a few scans, it does not make the call.
# ═══════════════════════════════════════════════════════════════
LAYER1_CLUSTER_CORR_THRESHOLD: float = 0.75  # [Fix-33] TUNABLE — needs validation; documented clustering threshold, not derived from data.
CORR_MATRIX_MIN_SAMPLE: int = 20             # [Fix-33] TUNABLE — minimum overlapping-return sample size before a pair is scored at all.
LOG_DYNAMIC_CORR_GROUPS: bool = False        # [Fix-33] opt-in diagnostic; off by default to avoid log noise/cost when unused.

def compute_pairwise_correlation_matrix(
    symbols: list[str],
    candle_bundles: dict[str, tuple],
    candle_idx: int = CANDLE_BUNDLE_IDX_4H,  # [AUDIT-P7-4] was bare 2
    lookback: int = LOW_BTC_CORR_LOOKBACK_BARS,
) -> dict[tuple[str, str], float]:
    """[Fix-33] Pairwise Pearson correlation of 4H close-to-close returns
    across `symbols`, generalizing update_dynamic_btc_correlation() (vs-BTC
    only) to all pairs. `candle_bundles[sym][candle_idx]` is expected to be
    the 4H candle list — matches the convention used at the
    update_dynamic_btc_correlation() call site in main(), now both keyed off
    CANDLE_BUNDLE_IDX_4H ([AUDIT-P7-4] named index, was bare bundle[2] in both places).
    Pairs with insufficient overlapping history are simply omitted from the result
    rather than raising. Diagnostic helper only — see module-level comment
    above for why this isn't wired into CORR_GROUPS directly."""
    returns_by_symbol: dict[str, list[float]] = {}
    for sym in symbols:
        bundle = candle_bundles.get(sym)
        if not bundle or len(bundle) <= candle_idx:
            continue
        candles = bundle[candle_idx]
        if len(candles) < lookback + 2:
            continue
        closes = [c["c"] for c in candles[-(lookback + 1):]]
        rets = [(closes[i] - closes[i - 1]) / closes[i - 1]
                for i in range(1, len(closes)) if closes[i - 1] != 0]
        if len(rets) >= CORR_MATRIX_MIN_SAMPLE:
            returns_by_symbol[sym] = rets

    matrix: dict[tuple[str, str], float] = {}
    syms = sorted(returns_by_symbol.keys())
    for i, a in enumerate(syms):
        for b in syms[i + 1:]:
            ra, rb = returns_by_symbol[a], returns_by_symbol[b]
            n = min(len(ra), len(rb))
            if n < CORR_MATRIX_MIN_SAMPLE:
                continue
            ra_, rb_ = ra[:n], rb[:n]
            mean_a = sum(ra_) / n
            mean_b = sum(rb_) / n
            cov   = sum((x - mean_a) * (y - mean_b) for x, y in zip(ra_, rb_)) / n
            var_a = sum((x - mean_a) ** 2 for x in ra_) / n
            var_b = sum((y - mean_b) ** 2 for y in rb_) / n
            if var_a <= 0 or var_b <= 0:
                continue
            matrix[(a, b)] = cov / (math.sqrt(var_a) * math.sqrt(var_b))
    return matrix

def cluster_by_correlation(
    symbols: list[str],
    corr_matrix: dict[tuple[str, str], float],
    threshold: float = LAYER1_CLUSTER_CORR_THRESHOLD,
) -> list[set[str]]:
    """[Fix-33] Greedy union-find clustering: symbols end up in the same
    cluster if a chain of pairwise correlations >= `threshold` connects them.
    This is a simple, reviewable clustering choice, not a claim that it's the
    "right" algorithm for this purpose (e.g. it can chain a long transitive
    sequence of pairs together even if not all pairs in the resulting cluster
    are mutually correlated above threshold). Symbols with no qualifying edge
    end up as their own singleton cluster."""
    parent = {s: s for s in symbols}

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(x, y):
        rx, ry = find(x), find(y)
        if rx != ry:
            parent[rx] = ry

    for (a, b), corr in corr_matrix.items():
        if a in parent and b in parent and corr >= threshold:
            union(a, b)

    clusters: dict[str, set[str]] = {}
    for s in symbols:
        clusters.setdefault(find(s), set()).add(s)
    return list(clusters.values())

def log_dynamic_layer1_clustering(candle_bundles: dict[str, tuple]) -> None:
    """[Fix-33] Diagnostic-only: compute a data-driven clustering of the
    current CORR_GROUPS["layer1"] members from this scan's 4H candles and
    print it for manual comparison against the hand-picked grouping. Does NOT
    mutate CORR_GROUPS and is NOT read by deduplicate_correlated(). Gated by
    LOG_DYNAMIC_CORR_GROUPS (default False)."""
    if not LOG_DYNAMIC_CORR_GROUPS:
        return
    layer1_members = sorted(CORR_GROUPS.get("layer1", set()))
    if len(layer1_members) < 2:
        return
    try:
        matrix   = compute_pairwise_correlation_matrix(layer1_members, candle_bundles)
        clusters = cluster_by_correlation(layer1_members, matrix)
        print(f"  [CORR MATRIX] layer1 data-driven clusters this scan "
              f"(threshold={LAYER1_CLUSTER_CORR_THRESHOLD}, NOT auto-applied — "
              f"diagnostic only): {[sorted(c) for c in clusters]}")
    except Exception as e:
        print(f"  [CORR MATRIX] clustering diagnostic failed: {e}")

def check_btc_regime_filter(direction: str, symbol: str,
                            signal_type: str = "") -> tuple[int, str]:
    if hl_coin(symbol) == "BTC":
        return 0, "BTC Regime: N/A (BTC itself)"

    regime = get_btc_regime()
    if regime is None:
        return 0, "BTC Regime: Unknown"

    label = regime["label"]
    low_corr_set = get_low_btc_corr_set()

    if direction == "long" and regime["bearish"]:
        if symbol in low_corr_set:
            return 0, f"{label} — counter-trend (exempt, decorrelated)"
        # RS exemption: coin actively outperforming BTC → treat as decorrelated this scan
        with _rs_lock:
            rs_snap = dict(_rs_snapshot if _rs_snapshot is not None else _rs_scores)
        btc_ret  = rs_snap.get("BTCUSDT")
        coin_ret = rs_snap.get(symbol)
        if btc_ret is not None and coin_ret is not None:
            rs_vs_btc = coin_ret - btc_ret
            if rs_vs_btc >= RS_BEARISH_REGIME_EXEMPT_PCT:
                return 0, f"{label} — counter-trend (exempt, RS +{rs_vs_btc:.1f}% vs BTC)"
        return -1, f"{label} — counter-trend (-1)"

    if direction == "short" and regime["bullish"]:
        if symbol in low_corr_set:
            return 0, f"{label} — counter-trend (exempt, decorrelated)"
        return -1, f"{label} — counter-trend (-1)"

    if direction == "long" and regime["bullish"]:
        return +1, f"{label} — tailwind (+1)"
    if direction == "short" and regime["bearish"]:
        return +1, f"{label} — tailwind (+1)"

    if not regime["bullish"] and not regime["bearish"]:
        if signal_type == "BREAK":
            return 0, f"{label} — Mixed (neutral for BREAK)"
        else:
            return 0, f"{label} — Mixed (0)"

    return 0, f"{label} — Mixed (0)"

# ═══════════════════════════════════════════════════════════════
# MARKET BREADTH FILTER
# ═══════════════════════════════════════════════════════════════

_breadth_ema50_above: dict[str, bool] = {}
_breadth_snapshot:   dict[str, bool] | None = None
_breadth_lock = threading.Lock()

def reset_breadth_cache() -> None:
    global _breadth_snapshot
    with _breadth_lock:
        _breadth_ema50_above.clear()
        _breadth_snapshot = None

def record_breadth_result(symbol: str, price_above_ema50: bool):
    with _breadth_lock:
        _breadth_ema50_above[symbol] = price_above_ema50

def finalize_breadth_cache() -> None:
    global _breadth_snapshot
    with _breadth_lock:
        _breadth_snapshot = dict(_breadth_ema50_above)

def compute_market_breadth() -> dict:
    with _breadth_lock:
        results = dict(_breadth_snapshot if _breadth_snapshot is not None else _breadth_ema50_above)
    if not results:
        return {"breadth_50_pct": 0.5, "label": "Market Breadth: Unknown"}
    above = sum(1 for v in results.values() if v)
    pct   = above / len(results)
    if pct < BREADTH_WEAK_LONG_THRESHOLD:
        label = f"Market Breadth: {pct*100:.0f}% > EMA50 (Weak)"
    elif pct > BREADTH_WEAK_SHORT_THRESHOLD:
        label = f"Market Breadth: {pct*100:.0f}% > EMA50 (Overbought)"
    else:
        label = f"Market Breadth: {pct*100:.0f}% > EMA50 (Healthy)"
    return {"breadth_50_pct": pct, "label": label}

def apply_breadth_adjustment(direction: str, rs_pct: float | None = None,
                              btc_regime: dict | None = None) -> tuple[int, str]:
    breadth = compute_market_breadth()
    pct     = breadth["breadth_50_pct"]
    label   = breadth["label"]
    adj     = 0

    _is_mixed = (btc_regime is not None
                 and not btc_regime.get("bullish")
                 and not btc_regime.get("bearish"))
    _crowded_thresh = (BREADTH_CROWDED_LONG_THRESHOLD_MIXED
                       if _is_mixed
                       else BREADTH_CROWDED_LONG_THRESHOLD)

    if direction == "long":
        if pct > BREADTH_EXTREME_LONG_THRESHOLD:
            adj    = -2
            label += " (-2)"
        elif pct > _crowded_thresh:
            rs_weak = rs_pct is not None and rs_pct <= BREADTH_RS_NEGATIVE_THRESH
            if rs_weak:
                adj    = -2
                label += " (-2, crowded+weak RS)"
            else:
                adj    = -1
                label += " (-1, crowded)"
        elif pct < BREADTH_WEAK_LONG_THRESHOLD:
            adj    = -1
            label += " (-1)"
    elif direction == "short":
        if pct < BREADTH_EXTREME_SHORT_THRESHOLD:
            adj    = -2
            label += " (-2)"
        elif pct > BREADTH_WEAK_SHORT_THRESHOLD:
            adj    = -1
            label += " (-1)"
    return adj, label

# ═══════════════════════════════════════════════════════════════
# RELATIVE STRENGTH RANKING
# ═══════════════════════════════════════════════════════════════

_rs_scores:   dict[str, float] = {}
_rs_snapshot: dict[str, float] | None = None
_rs_lock = threading.Lock()

def reset_rs_cache() -> None:
    global _rs_snapshot
    with _rs_lock:
        _rs_scores.clear()
        _rs_snapshot = None

def record_rs_return(symbol: str, return_7d_pct: float):
    with _rs_lock:
        _rs_scores[symbol] = return_7d_pct

def finalize_rs_cache() -> None:
    global _rs_snapshot
    with _rs_lock:
        _rs_snapshot = dict(_rs_scores)

def compute_relative_strength(symbol: str) -> dict:
    with _rs_lock:
        scores = dict(_rs_snapshot if _rs_snapshot is not None else _rs_scores)

    btc_return  = scores.get("BTCUSDT")
    coin_return = scores.get(symbol)

    if btc_return is None or coin_return is None:
        return {"rs_pct": None, "percentile": None, "score_adj": 0, "label": "Relative Strength: N/A"}

    rs     = coin_return - btc_return
    other_scores = {k: v for k, v in scores.items() if k != "BTCUSDT"}
    all_rs = sorted([v - btc_return for v in other_scores.values()])
    n      = len(all_rs)
    if n == 0:
        return {"rs_pct": rs, "percentile": 0.5, "score_adj": 0,
                "label": f"Relative Strength: {rs:+.1f}%"}
    try:
        rank       = next(i for i, v in enumerate(all_rs) if v >= rs)
        percentile = rank / max(n - 1, 1)
    except StopIteration:
        percentile = 1.0

    if percentile >= (1.0 - RS_TOP_PERCENTILE):
        score_adj = 1
    elif percentile <= RS_BOTTOM_PERCENTILE:
        score_adj = -1
    else:
        score_adj = 0

    return {"rs_pct": rs, "percentile": percentile, "score_adj": score_adj,
            "label": f"Relative Strength: {rs:+.1f}%"}

# ═══════════════════════════════════════════════════════════════
# HISTORICAL PERFORMANCE ANALYTICS
# ═══════════════════════════════════════════════════════════════

def record_signal_history(state: dict, symbol: str, direction: str,
                           signal_type: str, score: int,
                           funding_rate: float | None, atr_pct: float,
                           oi_change_pct: float | None,
                           alignment_mode: str = "full",
                           sent: bool = True,
                           liquidity_bonus: int = 0,
                           liquidity_score: int = 0,
                           structure_score: int = 0) -> str:
    with _state_lock:
        hist     = state.setdefault("signal_history", [])
        # [AUDIT-P7-7] Milliseconds instead of whole seconds — two signals for the same
        # symbol recorded within the same second (e.g. a dropped+fired pair, or rapid
        # dedupe/re-record in the same scan cycle) previously collided on entry_id, with
        # the second append silently overwriting any later lookup-by-id of the first
        # (update_signal_result matches on "id", so a collision means the wrong entry's
        # result gets updated).
        entry_id = f"{symbol}_{int(time.time() * 1000)}"
        hist.append({
            "id":              entry_id,
            "symbol":          symbol,
            "direction":       direction,
            "signal_type":     signal_type,
            "score":           score,
            "alignment_mode":  alignment_mode,
            "funding_rate":    funding_rate,
            "atr_pct":         atr_pct,
            "oi_change_pct":   oi_change_pct,
            "result":          None,
            "sent":            sent,
            "timestamp":       int(time.time()),
            "liquidity_bonus": liquidity_bonus,
            "liquidity_score": liquidity_score,
            "structure_score": structure_score,
        })
        if len(hist) > MAX_SIGNAL_HISTORY:
            state["signal_history"] = hist[-MAX_SIGNAL_HISTORY:]
    return entry_id

def update_signal_result(state: dict, signal_id: str, result: str) -> None:
    with _state_lock:
        for entry in state.get("signal_history", []):
            if entry.get("id") == signal_id:
                entry["result"] = result
                return

def compute_win_rates(state: dict) -> dict:
    now_ts       = int(time.time())
    lookback_cut = now_ts - WIN_RATE_LOOKBACK_DAYS * 86400
    recent_cut   = now_ts - WIN_RATE_RECENT_DAYS   * 86400

    with _state_lock:
        raw_hist = [e for e in state.get("signal_history", [])
                    if e.get("result") in ("tp1", "tp2", "sl")
                    and e.get("timestamp", 0) >= lookback_cut
                    and e.get("sent", True)]

    weight_int = max(1, int(WIN_RATE_RECENT_WEIGHT))
    hist = []
    for e in raw_hist:
        if e.get("timestamp", 0) >= recent_cut:
            hist.extend([e] * weight_int)
        else:
            hist.append(e)

    wrs: dict = {"by_symbol": {}, "by_type": {}, "by_score": {}, "by_direction": {},
                 "by_alignment_mode": {}}

    def wr_for(entries):
        n = len(entries)
        # tp1_then_sl is gone — SL-after-TP1 now resolves as plain "tp1".
        wins = sum(
            1.0 if e["result"] in ("tp1", "tp2") else 0.0
            for e in entries
        )
        # [Fix-28] Track the most recent contributing entry's timestamp so callers
        # can apply a staleness check on top of the sample-size floor (see
        # compute_win_rate_analytics / WIN_RATE_STALE_DAYS).
        most_recent_ts = max((e.get("timestamp", 0) for e in entries), default=0)
        return (wins / n if n > 0 else 0.0), n, most_recent_ts

    for sym in set(e["symbol"] for e in hist):
        subset = [e for e in hist if e["symbol"] == sym]
        wr, n, most_recent_ts  = wr_for(subset)
        if n >= WIN_RATE_MIN_SAMPLE:
            wrs["by_symbol"][sym] = {"wr": wr, "n": n, "most_recent_ts": most_recent_ts}

    for st in ("BREAK", "PULL"):
        subset = [e for e in hist if e.get("signal_type") == st]
        wr, n, most_recent_ts  = wr_for(subset)
        if n >= WIN_RATE_MIN_SAMPLE:
            wrs["by_type"][st] = {"wr": wr, "n": n, "most_recent_ts": most_recent_ts}

    for sc in range(4, 8):
        subset = [e for e in hist if e.get("score") == sc]
        wr, n, most_recent_ts  = wr_for(subset)
        if n >= WIN_RATE_MIN_SAMPLE:
            wrs["by_score"][str(sc)] = {"wr": wr, "n": n, "most_recent_ts": most_recent_ts}

    for d in ("long", "short"):
        subset = [e for e in hist if e.get("direction") == d]
        wr, n, most_recent_ts  = wr_for(subset)
        if n >= WIN_RATE_MIN_SAMPLE:
            wrs["by_direction"][d] = {"wr": wr, "n": n, "most_recent_ts": most_recent_ts}

    for mode in ("full", "exhaustion", "pullback"):
        subset = [e for e in hist if e.get("alignment_mode", "full") == mode]
        wr, n, most_recent_ts  = wr_for(subset)
        if n >= WIN_RATE_MIN_SAMPLE:
            wrs["by_alignment_mode"][mode] = {"wr": wr, "n": n, "most_recent_ts": most_recent_ts}

    return wrs

_win_rates_cache:      dict | None = None
_win_rates_cache_lock  = threading.Lock()

def reset_win_rates_cache() -> None:
    global _win_rates_cache
    with _win_rates_cache_lock:
        _win_rates_cache = None

def get_cached_win_rates(state: dict) -> dict:
    global _win_rates_cache
    with _win_rates_cache_lock:
        if _win_rates_cache is None:
            _win_rates_cache = compute_win_rates(state)
        return _win_rates_cache

# [Fix-23] Known, honest mapping from a PENALTY_PRIORITY label substring to a
# get_cached_win_rates() bucket that actually measures something related to it.
# Most penalty labels (e.g. "BTC Regime", "OI", "RS negative") have no dedicated
# win-rate bucket in compute_win_rate_analytics() — signal_history doesn't record which
# adjustment labels fired on each entry, only symbol/type/direction/alignment_mode/
# score. Rather than fabricate a spurious correlation for those, this mapping only
# covers the one case where a real bucket exists ("Exhaustion" <-> the
# alignment_mode="exhaustion" bucket, since SignalResult.alignment_mode is set to
# "exhaustion" precisely when an exhaustion-style counter-signal fires). Extending
# this to the other penalty types would require instrumenting record_signal_history
# to also store which adjustment labels were applied — flagged as a follow-up, not
# done here to keep this a minimal, targeted change.
# [AUDIT-P6-3] Reviewed against both audit-suggested options:
#   Option A (expand the map) as literally proposed — adding "Counter-4H"/"Trap"/"Macro"
#   entries — does NOT apply here: none of those are real PENALTY_PRIORITY labels in this
#   file (the actual _HANDPICKED_PENALTY_PRIORITY list is ["BTC Regime", "OI", "Exhaustion",
#   "RS negative"]), and compute_win_rates()'s by_alignment_mode dict only ever populates
#   "full"/"exhaustion"/"pullback" — there is no win-rate bucket those labels could map to
#   without inventing one. Adding them would fabricate a correlation, which is exactly what
#   this comment block above already says not to do.
#   Option B (disable DATA_DRIVEN_PENALTY_PRIORITY) also doesn't fit: the feature isn't
#   broken or unsupported — it correctly covers the one label ("Exhaustion") where
#   signal_history really does carry the needed metadata (alignment_mode), and disabling it
#   would regress a working, narrowly-scoped feature for no reason.
# Conclusion: single-entry map is already the correct, complete state given what
# signal_history currently records. No code change made; this note documents that the
# P6-3 options were considered and neither applied as written.
_PENALTY_LABEL_TO_ALIGNMENT_MODE = {"Exhaustion": "exhaustion"}

def _get_data_driven_penalty_priority(state: dict, fallback: list[str]) -> list[str]:
    """
    [Fix-23] TUNABLE / genuine algorithm change — needs validation. Reorders
    `fallback` (the hand-picked PENALTY_PRIORITY list) by ascending historical win
    rate where a real win-rate bucket exists for a given label (lower win rate =
    that adjustment type has historically correlated with losses = higher priority
    to RETAIN when the penalty cap trims). Labels with no mapped bucket, or with a
    mapped bucket that hasn't cleared WIN_RATE_MIN_SAMPLE yet, fall back to their
    original hand-picked relative order, appended after the data-backed ones.
    Isolated on purpose so it can be reviewed/toggled independently of the rest of
    the scoring pipeline (see DATA_DRIVEN_PENALTY_PRIORITY flag at call site).
    """
    wrs = get_cached_win_rates(state)
    scored: list[tuple[float, str]] = []
    unscored: list[str] = []
    for label in fallback:
        mode = _PENALTY_LABEL_TO_ALIGNMENT_MODE.get(label)
        bucket = wrs.get("by_alignment_mode", {}).get(mode) if mode else None
        if bucket and bucket["n"] >= WIN_RATE_MIN_SAMPLE:
            scored.append((bucket["wr"], label))
        else:
            unscored.append(label)
    scored.sort(key=lambda t: t[0])
    return [lbl for _, lbl in scored] + unscored

def compute_win_rate_analytics(state: dict, symbol: str, direction: str,
                                signal_type: str, score: int,
                                alignment_mode: str = "full") -> dict:
    wrs       = get_cached_win_rates(state)
    mode_data = wrs.get("by_alignment_mode", {}).get(alignment_mode)
    if mode_data:
        wr, n, most_recent_ts = mode_data["wr"], mode_data["n"], mode_data.get("most_recent_ts", 0)
    else:
        sym_data = wrs["by_symbol"].get(symbol)
        if sym_data:
            wr, n, most_recent_ts = sym_data["wr"], sym_data["n"], sym_data.get("most_recent_ts", 0)
        else:
            dir_data = wrs["by_direction"].get(direction)
            if dir_data:
                wr, n, most_recent_ts = dir_data["wr"], dir_data["n"], dir_data.get("most_recent_ts", 0)
            else:
                return {"win_rate": None, "sample_size": 0, "score_adj": 0,
                        "label": "Win Rate: Insufficient data"}
    if wr >= WIN_RATE_HIGH_THRESH:
        score_adj = 1
    elif wr <= WIN_RATE_LOW_THRESH:
        score_adj = -1
    else:
        score_adj = 0
    if n < WIN_RATE_MIN_SAMPLE_FOR_ADJ:
        return {"win_rate": wr, "sample_size": n, "score_adj": 0,
                "label": f"Win Rate: {wr*100:.0f}% (n={n}, insufficient for adj)"}
    # [Fix-28] TUNABLE — needs validation. Overfitting guard on sample recency.
    # WIN_RATE_LOOKBACK_DAYS=30 combined with low signal frequency (audit Part 7
    # estimate: 3-7 signals/day) means a bucket can nominally clear
    # WIN_RATE_MIN_SAMPLE_FOR_ADJ while its most recent contributing entry is
    # already weeks stale. Treat that as insufficient too, even though `n` looks fine.
    stale_cutoff = int(time.time()) - WIN_RATE_STALE_DAYS * 86400
    if most_recent_ts < stale_cutoff:
        return {"win_rate": wr, "sample_size": n, "score_adj": 0,
                "label": f"Win Rate: {wr*100:.0f}% (n={n}, stale — most recent sample "
                         f"{(int(time.time()) - most_recent_ts) // 86400}d old)"}
    return {"win_rate": wr, "sample_size": n, "score_adj": score_adj,
            "label": f"Win Rate: {wr*100:.0f}%  Sample: {n}  (30d weighted)"}

# ═══════════════════════════════════════════════════════════════
# ECONOMIC CALENDAR FILTER
# ═══════════════════════════════════════════════════════════════

MACRO_EVENT_KEYWORDS = [
    "fomc", "federal funds", "interest rate decision",
    "cpi", "consumer price index",
    "ppi", "producer price index",
    "nfp", "nonfarm payroll", "non-farm payroll",
    "gdp", "gross domestic product",
    "ecb", "bank of england", "boe",
]

MACRO_CALENDAR_URL = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"

def parse_forexfactory_event_utc(ev_date: str, ev_time: str) -> datetime | None:
    try:
        if "day" in ev_time.lower() or ev_time.strip() == "":
            dt_str   = f"{ev_date} 14:00"
            dt_local = datetime.strptime(dt_str, "%Y-%m-%d %H:%M").replace(tzinfo=_FF_TZ)
        else:
            dt_str   = f"{ev_date} {ev_time}"
            dt_local = datetime.strptime(dt_str, "%Y-%m-%d %I:%M%p").replace(tzinfo=_FF_TZ)
        return dt_local.astimezone(timezone.utc)
    except Exception:
        return None

def fetch_macro_calendar(state: dict) -> list[dict]:
    with _state_lock:
        cache_entry = state.get("macro_calendar_cache", {})
        cached_at   = cache_entry.get("fetched_at", 0)
    now_ts      = int(time.time())
    if now_ts - cached_at < MACRO_CACHE_TTL_S:
        return cache_entry.get("events", [])
    _macro_max_attempts = 3
    _macro_base_sleep   = 1.0
    raw_events = []
    for _attempt in range(_macro_max_attempts):
        try:
            resp = requests.get(MACRO_CALENDAR_URL, timeout=10)
            resp.raise_for_status()
            raw_events = resp.json()
            break
        except Exception as e:
            if _attempt == _macro_max_attempts - 1:
                print(f"  [MACRO CAL] fetch failed after {_macro_max_attempts} attempts: {e} — using cached/empty")
                return cache_entry.get("events", [])
            _sleep = min(10.0, _macro_base_sleep * (2 ** _attempt)) + random.uniform(0.0, 0.25)
            print(f"  [MACRO CAL] attempt {_attempt + 1} failed: {e} — retrying in {_sleep:.1f}s")
            time.sleep(_sleep)
    events = []
    for ev in raw_events:
        impact  = str(ev.get("impact", "")).lower()
        title   = str(ev.get("title",  "")).lower()
        ev_date = ev.get("date", "")
        ev_time = ev.get("time", "")
        if impact not in ("high",):
            continue
        if not any(kw in title for kw in MACRO_EVENT_KEYWORDS):
            continue
        dt_utc = parse_forexfactory_event_utc(ev_date, ev_time)
        if dt_utc is None:
            continue
        events.append({"name": ev.get("title", "Unknown event"),
                       "datetime_utc": dt_utc.isoformat(), "impact": impact})
    with _state_lock:
        state["macro_calendar_cache"] = {"fetched_at": now_ts, "events": events}
    print(f"  [MACRO CAL] Loaded {len(events)} high-impact events")
    return events

def apply_macro_filter(state: dict, atr_pct: float,
                       reference_ms: int | None = None,
                       atr_pctile: float | None = None) -> dict:
    events      = fetch_macro_calendar(state)
    _ref_ts     = (reference_ms / 1000) if reference_ms is not None else time.time()
    now_utc     = datetime.fromtimestamp(_ref_ts, tz=timezone.utc)
    nearest_event = None
    nearest_mins  = None
    for ev in events:
        try:
            ev_dt = datetime.fromisoformat(ev["datetime_utc"])
        except Exception:
            continue
        mins_delta = (ev_dt - now_utc).total_seconds() / 60.0
        if -MACRO_WINDOW_AFTER_MINS <= mins_delta <= MACRO_WINDOW_BEFORE_MINS:
            if nearest_mins is None or abs(mins_delta) < abs(nearest_mins):
                nearest_event = ev["name"]
                nearest_mins  = mins_delta
    if nearest_event is None:
        return {"in_window": False, "event_name": None, "mins_to_event": None,
                "score_adj": 0, "label": "Macro Risk: None", "hard_suppress": False}

    effective_threshold = MACRO_HIGH_ATR_SUPPRESS_PCT
    if atr_pctile is not None and atr_pctile > MACRO_ATR_PCTILE_HIGH:
        effective_threshold *= MACRO_ATR_PCTILE_HIGH_MULT

    score_adj     = -1
    hard_suppress = atr_pct >= effective_threshold
    if nearest_mins >= 0:
        label = f"⚠️ Macro Event Nearby\n{nearest_event} in {int(nearest_mins)} minutes"
    else:
        label = f"⚠️ Macro Event Nearby\n{nearest_event} {int(abs(nearest_mins))} minutes ago"
    return {"in_window": True, "event_name": nearest_event, "mins_to_event": nearest_mins,
            "score_adj": score_adj, "label": label, "hard_suppress": hard_suppress}

# ═══════════════════════════════════════════════════════════════
# INDICATOR MATH
# ═══════════════════════════════════════════════════════════════

def ema(values: list[float], period: int) -> list[float]:
    if len(values) < period:
        return [float("nan")] * len(values)
    k      = 2.0 / (period + 1)
    result = [float("nan")] * len(values)
    result[period - 1] = sum(values[:period]) / period
    for i in range(period, len(values)):
        result[i] = values[i] * k + result[i - 1] * (1 - k)
    return result

def rsi(closes: list[float], period: int) -> list[float]:
    if len(closes) < period + 1:
        return [float("nan")] * len(closes)
    result = [float("nan")] * len(closes)
    gains, losses = [], []
    for i in range(1, len(closes)):
        d = closes[i] - closes[i - 1]
        gains.append(max(d, 0))
        losses.append(max(-d, 0))
    avg_g = sum(gains[:period]) / period
    avg_l = sum(losses[:period]) / period
    rs    = avg_g / avg_l if avg_l != 0 else float("inf")
    result[period] = 100 - 100 / (1 + rs)
    for i in range(period, len(gains)):
        avg_g = (avg_g * (period - 1) + gains[i])  / period
        avg_l = (avg_l * (period - 1) + losses[i]) / period
        rs    = avg_g / avg_l if avg_l != 0 else float("inf")
        result[i + 1] = 100 - 100 / (1 + rs)
    return result

def bollinger(closes, period: int, mult: float):
    n = len(closes)
    basis_arr = [float("nan")] * n
    upper_arr = [float("nan")] * n
    lower_arr = [float("nan")] * n
    if n < period:
        return basis_arr, upper_arr, lower_arr

    rolling_sum = sum(closes[:period])
    rolling_sq_sum = sum(x * x for x in closes[:period])

    for i in range(period - 1, n):
        if i > period - 1:
            rolling_sum += closes[i] - closes[i - period]
            rolling_sq_sum += closes[i] * closes[i] - closes[i - period] * closes[i - period]

        m = rolling_sum / period
        var = max(0.0, rolling_sq_sum / period - m * m)
        sd = math.sqrt(var)
        basis_arr[i] = m
        upper_arr[i] = m + mult * sd
        lower_arr[i] = m - mult * sd

    return basis_arr, upper_arr, lower_arr

def adx_dmi(highs, lows, closes, period: int):
    n        = len(closes)
    plus_dm  = [0.0] * n
    minus_dm = [0.0] * n
    tr_arr   = [0.0] * n
    for i in range(1, n):
        up   = highs[i]    - highs[i - 1]
        down = lows[i - 1] - lows[i]
        plus_dm[i]  = up   if (up > down   and up > 0)   else 0
        minus_dm[i] = down if (down > up   and down > 0) else 0
        tr_arr[i]   = max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i - 1]),
            abs(lows[i]  - closes[i - 1]),
        )

    def wilder_smooth(arr):
        res = [0.0] * n
        if n <= period:
            return res
        res[period] = sum(arr[1:period + 1])
        for i in range(period + 1, n):
            res[i] = res[i - 1] - res[i - 1] / period + arr[i]
        return res

    sm_tr   = wilder_smooth(tr_arr)
    sm_plus = wilder_smooth(plus_dm)
    sm_min  = wilder_smooth(minus_dm)

    di_plus  = [float("nan")] * n
    di_minus = [float("nan")] * n
    dx_arr   = [float("nan")] * n
    adx_arr  = [float("nan")] * n

    for i in range(period, n):
        if sm_tr[i] == 0:
            continue
        dp = 100 * sm_plus[i] / sm_tr[i]
        dm = 100 * sm_min[i]  / sm_tr[i]
        di_plus[i]  = dp
        di_minus[i] = dm
        dsum = dp + dm
        dx_arr[i] = 100 * abs(dp - dm) / dsum if dsum != 0 else 0

    first = next((i for i in range(period, n) if not math.isnan(dx_arr[i])), None)
    if first is None:
        return di_plus, di_minus, adx_arr
    seed_end = first + period
    if seed_end > n:
        return di_plus, di_minus, adx_arr
    valid = [dx_arr[i] for i in range(first, seed_end) if not math.isnan(dx_arr[i])]
    if len(valid) < period:
        return di_plus, di_minus, adx_arr
    adx_arr[seed_end - 1] = sum(valid) / period
    for i in range(seed_end, n):
        if not math.isnan(adx_arr[i - 1]) and not math.isnan(dx_arr[i]):
            adx_arr[i] = (adx_arr[i - 1] * (period - 1) + dx_arr[i]) / period
    return di_plus, di_minus, adx_arr

def obv(closes, volumes) -> list[float]:
    result = [0.0] * len(closes)
    for i in range(1, len(closes)):
        if closes[i] > closes[i - 1]:
            result[i] = result[i - 1] + volumes[i]
        elif closes[i] < closes[i - 1]:
            result[i] = result[i - 1] - volumes[i]
        else:
            result[i] = result[i - 1]
    return result

def detect_rsi_divergence(closes: list[float], rsi_values: list[float],
                          highs: list[float], lows: list[float],
                          lookback: int = 20) -> dict:
    min_len = lookback + 2
    if (len(closes) < min_len or len(rsi_values) < min_len
            or len(highs) < min_len or len(lows) < min_len):
        return {"type": None, "strength": 0}

    recent_highs = highs[-(lookback + 2):]
    recent_lows  = lows[-(lookback + 2):]
    recent_rsi   = rsi_values[-(lookback + 2):]

    price_highs = []
    price_lows = []
    rsi_highs = []
    rsi_lows = []

    for i in range(1, len(recent_highs) - 1):
        if recent_highs[i] > recent_highs[i - 1] and recent_highs[i] > recent_highs[i + 1]:
            price_highs.append((i, recent_highs[i]))
            rsi_highs.append((i, recent_rsi[i]))
        if recent_lows[i] < recent_lows[i - 1] and recent_lows[i] < recent_lows[i + 1]:
            price_lows.append((i, recent_lows[i]))
            rsi_lows.append((i, recent_rsi[i]))

    bearish_div = False
    if len(price_highs) >= 2 and len(rsi_highs) >= 2:
        if (price_highs[-1][1] > price_highs[-2][1] and
            rsi_highs[-1][1] < rsi_highs[-2][1]):
            bearish_div = True

    bullish_div = False
    if len(price_lows) >= 2 and len(rsi_lows) >= 2:
        if (price_lows[-1][1] < price_lows[-2][1] and
            rsi_lows[-1][1] > rsi_lows[-2][1]):
            bullish_div = True

    if bearish_div:
        return {"type": "bearish", "strength": 1}
    elif bullish_div:
        return {"type": "bullish", "strength": 1}
    else:
        return {"type": None, "strength": 0}

def sma(values, period: int) -> list[float]:
    result = [float("nan")] * len(values)
    for i in range(period - 1, len(values)):
        result[i] = sum(values[i - period + 1: i + 1]) / period
    return result

def rolling_vwap(closes, volumes, period: int) -> list[float]:
    pv  = [c * v for c, v in zip(closes, volumes)]
    spv = sma(pv,      period)
    sv  = sma(volumes, period)
    result = [float("nan")] * len(closes)
    for i in range(len(closes)):
        if not math.isnan(spv[i]) and sv[i] != 0:
            result[i] = spv[i] / sv[i]
    return result

def daily_vwap(candles_15m: list[dict], reference_ms: int | None = None) -> float:
    if not candles_15m:
        return 0.0
    # [Fix-19] Thread reference_ms through, matching the pattern used elsewhere in
    # this file (e.g. apply_macro_filter) — previously called datetime.now() directly,
    # which breaks reproducibility (e.g. for backtesting/replay against historical data).
    _ref_ts     = (reference_ms / 1000) if reference_ms is not None else time.time()
    today_start = datetime.fromtimestamp(_ref_ts, tz=timezone.utc).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    start_ms = int(today_start.timestamp() * 1000)
    today_candles = [c for c in candles_15m if c["t"] >= start_ms]
    if not today_candles:
        return safe(candles_15m[-1]["c"])
    total_vol = sum(c["v"] for c in today_candles)
    if total_vol == 0:
        return safe(candles_15m[-1]["c"])
    total_pv = sum(((c["h"] + c["l"] + c["c"]) / 3.0) * c["v"] for c in today_candles)  # [FIX-AUDIT-D16] use (H+L+C)/3 typical price per standard VWAP definition
    return total_pv / total_vol

# ═══════════════════════════════════════════════════════════════
# INDICATOR CACHE
# ═══════════════════════════════════════════════════════════════

_indicator_cache: "OrderedDict[str, dict]" = OrderedDict()  # [AUDIT-P7-6] was a plain dict (FIFO eviction)
_indicator_cache_lock = threading.Lock()
INDICATOR_CACHE_MAX_SIZE: int = 200  # [AUDIT-P7-6] named constant, was a bare 200 inline

def _compute_all_indicators(candles: list[dict]) -> dict:
    o = [c["o"] for c in candles]
    h = [c["h"] for c in candles]
    l = [c["l"] for c in candles]
    c = [c["c"] for c in candles]
    v = [c["v"] for c in candles]

    return {
        "o": o, "h": h, "l": l, "c": c, "v": v,
        "ema_fast": ema(c, FAST_LEN),
        "ema_slow": ema(c, SLOW_LEN),
        "rsi": rsi(c, RSI_LEN),
        "atr": atr(h, l, c, ATR_LEN),
        "adx": adx_dmi(h, l, c, ADX_LEN),
        "bb_basis": bollinger(c, BB_LEN, BB_MULT)[0],
        "bb_upper": bollinger(c, BB_LEN, BB_MULT)[1],
        "bb_lower": bollinger(c, BB_LEN, BB_MULT)[2],
        "vol_ma": sma(v, VOL_LEN),
        "obv": obv(c, v),
    }

def get_cached_indicators(symbol: str, timeframe: str, candles: list[dict]) -> dict:
    if not candles:
        return _compute_all_indicators(candles)

    cache_key = f"{symbol}_{timeframe}"
    first = candles[0]
    last = candles[-1]
    cache_sig = (
        len(candles),
        last["t"], last["c"],
        first["t"], first["c"],
    )

    # [AUDIT-P7-6] True LRU read: a cache hit moves the key to the end of the OrderedDict
    # so it's marked as recently used. Previously this was a plain dict, and eviction below
    # removed next(iter(cache)) — the oldest-INSERTED key (FIFO), not the least-recently-USED
    # one, so a key that's read constantly but inserted long ago could still get evicted
    # while a rarely-read-but-recently-inserted key survived.
    with _indicator_cache_lock:
        cached = _indicator_cache.get(cache_key)
        if cached and cached.get("sig") == cache_sig:
            _indicator_cache.move_to_end(cache_key)
            return cached["data"]

    data = _compute_all_indicators(candles)

    with _indicator_cache_lock:
        if cache_key in _indicator_cache:
            _indicator_cache.move_to_end(cache_key)
        _indicator_cache[cache_key] = {"sig": cache_sig, "data": data}
        if len(_indicator_cache) > INDICATOR_CACHE_MAX_SIZE:
            _indicator_cache.popitem(last=False)   # evict least-recently-used (LRU), not oldest-inserted (FIFO)

    return data

def clear_indicator_cache() -> None:
    with _indicator_cache_lock:
        _indicator_cache.clear()

def _selftest_indicator_cache_isolation():
    a = [{"t": i, "o": 100+i, "h": 101+i, "l": 99+i, "c": 100+i, "v": 1.0, "qv": 100.0} for i in range(60)]
    b = [{"t": i, "o": 1+i*0.01, "h": 1.01+i*0.01, "l": 0.99+i*0.01, "c": 1+i*0.01, "v": 1.0, "qv": 1.0} for i in range(60)]
    ia = get_cached_indicators("SELFA", "15m", a)
    ib = get_cached_indicators("SELFB", "15m", b)
    assert ia["c"][-1] != ib["c"][-1], "Indicator cache is leaking across symbols!"

# ═══════════════════════════════════════════════════════════════
# STATE HELPERS
# ═══════════════════════════════════════════════════════════════

def update_atr_history(state: dict, symbol: str, atr_pct: float) -> None:
    with _state_lock:
        hist = state.setdefault("atr_history", {}).setdefault(symbol, [])
        hist.append({"ts": int(time.time()), "atr_pct": atr_pct})
        if len(hist) > ATR_HIST_DEPTH:
            state["atr_history"][symbol] = hist[-ATR_HIST_DEPTH:]

def get_atr_percentile(state: dict, symbol: str, atr_pct: float) -> float | None:
    with _state_lock:
        hist = list(state.get("atr_history", {}).get(symbol, []))
    vals = sorted(e["atr_pct"] for e in hist)
    if len(vals) < 10:
        return None
    below = sum(1 for v in vals if v < atr_pct)
    return below / len(vals)

def prune_state(state: dict) -> None:
    now = int(time.time())
    with _state_lock:
        # [Fix-11 cleanup] One-time discard of the stale "" bucket left behind by the
        # pre-fix ATR-history key bug (update_atr_history was called with res.symbol,
        # which was always "" at that point). Safe no-op once the bucket is gone.
        state.get("atr_history", {}).pop("", None)

        for sym in list(state.get("oi_history", {}).keys()):
            state["oi_history"][sym] = [e for e in state["oi_history"][sym]
                                        if now - e["ts"] < 86400]
            if not state["oi_history"][sym]:
                del state["oi_history"][sym]

        cutoff_cooldown = now - PULL_REENTRY_COOLDOWN_S * 3
        state["post_loss_cooldown"] = {
            k: v for k, v in state.get("post_loss_cooldown", {}).items()
            if v > cutoff_cooldown
        }

        for sym in list(state.get("atr_history", {}).keys()):
            state["atr_history"][sym] = [
                e for e in state["atr_history"][sym]
                if now - e["ts"] < 86400
            ]
            if not state["atr_history"][sym]:
                del state["atr_history"][sym]

        state["resolved_signals"] = [
            e for e in state.get("resolved_signals", [])
            if now - e.get("resolved_at", 0) < 259200
        ]

        for sym in list(state.get("funding_history", {}).keys()):
            state["funding_history"][sym] = [
                e for e in state["funding_history"][sym]
                if now - e["ts"] < 7200
            ]
            if not state["funding_history"][sym]:
                del state["funding_history"][sym]

        current_bar = int(time.time() * 1000) // (15 * 60 * 1000)
        state["signal_cooldowns"] = {
            k: v for k, v in state.get("signal_cooldowns", {}).items()
            if current_bar - v < 96
        }

        cutoff_bar = current_bar - 96
        for sym in list(state.get("failed_breakouts", {}).keys()):
            state["failed_breakouts"][sym] = [
                fb for fb in state["failed_breakouts"][sym]
                if fb.get("bar_index", 0) > cutoff_bar
            ]
            if not state["failed_breakouts"][sym]:
                del state["failed_breakouts"][sym]

        state["active_signals"] = [
            s for s in state.get("active_signals", [])
            if current_bar - s.get("bar_index", current_bar) < SIGNAL_MAX_AGE_BARS
            and not s.get("resolved", False)
        ]

# ═══════════════════════════════════════════════════════════════
# SIGNAL LOGIC
# ═══════════════════════════════════════════════════════════════

class SignalResult:
    def __init__(self):
        self.fire_long   = False
        self.fire_short  = False
        self.signal_type = ""
        self.score       = 0
        self.final_score = 0
        self.entry = self.tp1 = self.tp2 = self.sl = 0.0
        self.entry_low = self.entry_high = 0.0
        self.atr_pct = 0.0
        self.atr_val  = 0.0
        self.breakdown = ""
        self.v10_gates = ""
        self.funding_rate:  float | None = None
        self.open_interest: float | None = None
        self.supports:    list[float] = []
        self.resistances: list[float] = []
        self.oi_trend_data:    dict = {}
        self.btc_regime_label: str  = ""
        self.breadth_label:    str  = ""
        self.rs_data:          dict = {}
        self.win_rate_data:    dict = {}
        self.macro_data:       dict = {}
        self.d200_label:       str  = ""
        self.vol_ratio:        float | None = None
        self.alignment_mode:   str  = "full"
        self.score_adjustments: list[tuple[str, int]] = []
        self.limit_entry:       float | None = None
        self.limit_entry_dist:  float | None = None
        self.spread_pct:        float | None = None
        self.symbol:            str  = ""

        # ── Liquidity confluence output ──
        self.liquidity_bonus: int = 0
        self.liquidity_score: int = 0
        self.structure_score: int = 0
        self.nearest_external_bsl: float | None = None
        self.nearest_external_ssl: float | None = None
        self.liquidity_sweep: dict = {}
        self.mss: dict = {}
        self.bos: dict = {}
        self.fvg: dict = {}
        self.premium_discount: str = "neutral"
        self.htf_alignment: bool = False
        self.liquidity_reasons: list[str] = []
        self.liquidity_hard_suppress: bool = False
        # [Fix-24] Logging/visibility only — see SR_LIQUIDITY_DISAGREEMENT_ATR_MULT.
        # Not used in scoring; reconciling which system is "right" is a separate
        # tuning decision the audit didn't make for us.
        self.sr_liquidity_disagreement: str | None = None
        self.continuation_prob_score:   int         = 0
        self.break_tier:                str         = "standard"
        self.pull_tier:                 str         = "B"

        # [Fix-43] Real 1h/4h liquidity structure (separate from
        # htf_alignment above, which is the borrowed EMA-cross boolean).
        self.htf_liquidity_score: int = 0
        self.htf_1h_zone: str = "neutral"
        self.htf_4h_zone: str = "neutral"
        self.htf_1h_external_bsl: float | None = None
        self.htf_1h_external_ssl: float | None = None
        self.htf_4h_external_bsl: float | None = None
        self.htf_4h_external_ssl: float | None = None
        self.htf_1h_sweep: dict = {}
        self.htf_4h_sweep: dict = {}

def record_market_inputs_from_candles(symbol: str,
                                      candles_15m: list[dict],
                                      candles_4h: list[dict]) -> None:
    c15 = [c["c"] for c in candles_15m]
    c4h = [c["c"] for c in candles_4h]
    ema_s4h = ema(c4h, SLOW_LEN)
    price_above_ema50_4h = c4h[-1] > safe(ema_s4h[-1])
    record_breadth_result(symbol, price_above_ema50_4h)
    if len(c4h) >= 42:
        ret_7d = (c4h[-1] - c4h[-42]) / c4h[-42] * 100.0 if c4h[-42] != 0 else 0.0
    elif len(c15) >= 672:
        ret_7d = (c15[-1] - c15[-672]) / c15[-672] * 100.0 if c15[-672] != 0 else 0.0
    else:
        ret_7d = 0.0
    record_rs_return(symbol, ret_7d)

def get_pull_recover_mult(btc_regime: dict | None) -> float:
    if btc_regime is None:
        return PULL_RECOVER_ATR_MULT_TREND
    bullish = btc_regime.get("bullish", False)
    bearish = btc_regime.get("bearish", False)
    mixed   = not bullish and not bearish
    if mixed:
        return PULL_RECOVER_ATR_MULT_MIXED
    return PULL_RECOVER_ATR_MULT_TREND

def get_effective_min_score(btc_regime: dict | None, breadth_pct: float) -> int:
    if btc_regime is None:
        return MIN_SCORE
    bearish = btc_regime.get("bearish", False)
    # [PATCH-6] Original condition (bearish AND breadth > 75%) was inverted for the
    # real risk case: a confirmed bear regime with near-capitulation breadth (<10%)
    # is the common loss environment and should raise the bar most. The old condition
    # never fired in observed data (breadth was 4%). Both branches now preserved.
    if bearish and breadth_pct < 0.10:
        return MIN_SCORE + 2   # confirmed bear + near-capitulation breadth
    if bearish and breadth_pct > 0.75:
        return MIN_SCORE + 1   # original condition preserved
    return MIN_SCORE

def get_dynamic_max_signals(btc_regime: dict | None, breadth_pct: float) -> int:
    if not USE_DYNAMIC_MAX_SIGNALS:
        return MAX_SIGNALS_PER_SCAN
    if btc_regime is None:
        return MAX_SIGNALS_DEFAULT
    bullish = btc_regime.get("bullish", False)
    bearish = btc_regime.get("bearish", False)
    if bullish and breadth_pct > BREADTH_BULL_THRESHOLD:
        return MAX_SIGNALS_BULL_TREND
    if bearish and breadth_pct < (1.0 - BREADTH_BULL_THRESHOLD):
        return MAX_SIGNALS_BEAR_TREND   # [FIX-AUDIT-D5]
    return MAX_SIGNALS_DEFAULT

# [Fix-37] Section 6 Item 10 — per-symbol/per-BTC-regime-label override of
# PULL_REQUIRES_4H. See PULL_REQUIRES_4H_OVERRIDE's definition for the key
# convention (exact WATCHLIST symbol, or a substring of compute_btc_regime()'s
# `label` field matched via `in`). An exact symbol match takes precedence over
# a regime-label match; if neither is present (the default — the override dict
# starts empty), this falls back to the global PULL_REQUIRES_4H unchanged.
def get_pull_requires_4h(symbol: str, btc_regime: dict | None) -> bool:
    if symbol in PULL_REQUIRES_4H_OVERRIDE:
        return PULL_REQUIRES_4H_OVERRIDE[symbol]
    if btc_regime is not None:
        label = btc_regime.get("label", "")
        for key, val in PULL_REQUIRES_4H_OVERRIDE.items():
            if key in label:
                return val
    return PULL_REQUIRES_4H

def check_false_breakout_pattern(state: dict, symbol: str, direction: str,
                                 bar_index: int) -> tuple[bool, str]:
    if not USE_FALSE_BREAKOUT_DETECTION:
        return False, ""
    with _state_lock:
        failed_breakouts_dict = state.get("failed_breakouts", {})
        symbol_breakouts = list(failed_breakouts_dict.get(symbol, []))
    for fb in symbol_breakouts:
        fb_direction = fb.get("direction")
        if fb_direction != direction:
            bars_since = bar_index - fb.get("bar_index", 0)
            if 1 <= bars_since <= FALSE_BREAKOUT_LOOKBACK_BARS:
                desc = f"False breakout re-entry ({fb_direction.upper()} BREAK failed {bars_since} bars ago)"
                return True, desc
    return False, ""

def record_failed_breakout(state: dict, symbol: str, direction: str,
                          signal_type: str, bar_index: int) -> None:
    if not USE_FALSE_BREAKOUT_DETECTION:
        return
    if signal_type != "BREAK":
        return
    with _state_lock:
        failed_breakouts = state.setdefault("failed_breakouts", {})
        symbol_breakouts = failed_breakouts.setdefault(symbol, [])
        symbol_breakouts.append({
            "direction": direction,
            "bar_index": bar_index,
            "timestamp": int(time.time()),
        })
        cutoff_bar = bar_index - 96
        failed_breakouts[symbol] = [
            fb for fb in symbol_breakouts if fb.get("bar_index", 0) > cutoff_bar
        ]

# ═══════════════════════════════════════════════════════════════
# REGIME-AWARE TP/SL MULTIPLIERS
# ═══════════════════════════════════════════════════════════════

def get_regime_aware_multipliers(btc_regime: dict | None,
                                  atr_pctile: float | None,
                                  signal_type: str) -> tuple[float, float, float]:
    if signal_type == "BREAK":
        tp1_mult = TP1_MULT_BREAK
        tp2_mult = TP2_MULT_BREAK
        sl_mult  = SL_MULT_BREAK
    else:
        tp1_mult = TP1_MULT_PULL
        tp2_mult = TP2_MULT_PULL
        sl_mult  = SL_MULT_PULL

    if not USE_REGIME_AWARE_TP_SL:
        return tp1_mult, tp2_mult, sl_mult

    if atr_pctile is not None and atr_pctile > ATR_HIGH_PERCENTILE:
        tp1_mult *= REGIME_HIGH_VOL_TP1_MULT
        tp2_mult *= REGIME_HIGH_VOL_TP2_MULT
        sl_mult  *= REGIME_HIGH_VOL_SL_MULT

    if atr_pctile is not None and atr_pctile < ATR_LOW_PERCENTILE:
        tp1_mult *= REGIME_LOW_VOL_TP1_MULT
        tp2_mult *= REGIME_LOW_VOL_TP2_MULT
        sl_mult  *= REGIME_LOW_VOL_SL_MULT

    if btc_regime is not None:
        if btc_regime.get("bearish"):
            tp1_mult *= REGIME_BEAR_TP1_MULT
        if btc_regime.get("bullish"):
            tp2_mult *= REGIME_BULL_TP2_MULT

    return tp1_mult, tp2_mult, sl_mult

# ═══════════════════════════════════════════════════════════════
# DECOMPOSED SIGNAL COMPUTATION
# ═══════════════════════════════════════════════════════════════

def _compute_signal_indicators(symbol: str, candles_15m, candles_1h, candles_4h, candles_d) -> dict:
    ind = {}

    i15 = get_cached_indicators(symbol, "15m", candles_15m)
    ind["o15"], ind["h15"], ind["l15"], ind["c15"], ind["v15"] = i15["o"], i15["h"], i15["l"], i15["c"], i15["v"]
    ind["ema_f15"] = i15["ema_fast"]
    ind["ema_s15"] = i15["ema_slow"]
    ind["rsi15"]   = i15["rsi"]
    ind["atr15"]   = i15["atr"]
    ind["adx15_arr"] = i15["adx"][2]
    ind["bb_b15"], ind["bb_u15"], ind["bb_l15"] = i15["bb_basis"], i15["bb_upper"], i15["bb_lower"]
    ind["vol_ma15"] = i15["vol_ma"]
    ind["obv15"]    = i15["obv"]

    i1h = get_cached_indicators(symbol, "1h", candles_1h)
    ind["ema_f1h"] = i1h["ema_fast"]
    ind["ema_s1h"] = i1h["ema_slow"]
    ind["rsi1h"]   = i1h["rsi"]
    ind["h1h"], ind["l1h"], ind["c1h"], ind["v1h"] = i1h["h"], i1h["l"], i1h["c"], i1h["v"]
    # [AUDIT-P4-3] ind["v1h"] was already wired above (this isn't new). vol_ma1h was the
    # missing piece — added here for completeness alongside ind["vol_ma4h"] (Prompt 1 /
    # FIX-AUDIT-D3) so any future multi-TF volume check has 1H volume-MA available without
    # needing another _compute_signal_indicators pass.
    ind["vol_ma1h"] = i1h["vol_ma"]

    i4h = get_cached_indicators(symbol, "4h", candles_4h)
    ind["ema_f4h"] = i4h["ema_fast"]
    ind["ema_s4h"] = i4h["ema_slow"]
    ind["adx4h_arr"] = i4h["adx"][2]
    ind["h4h"], ind["l4h"], ind["c4h"] = i4h["h"], i4h["l"], i4h["c"]
    ind["rsi4h"]   = i4h["rsi"]
    # [FIX-AUDIT-D3] Wire 4H volume data into ind so Phase 7 h4_vol_expansion fires correctly
    ind["v4h"]      = i4h["v"]
    ind["vol_ma4h"] = i4h["vol_ma"]

    if candles_d and len(candles_d) >= TREND_LEN:
        o_d = [c["o"] for c in candles_d]
        h_d = [c["h"] for c in candles_d]
        l_d = [c["l"] for c in candles_d]
        c_d = [c["c"] for c in candles_d]
        v_d = [c["v"] for c in candles_d]
        ind["ema_d200"] = ema(c_d, TREND_LEN)
        ind["adx_daily"] = safe(adx_dmi(h_d, l_d, c_d, ADX_LEN)[2][-1], 25.0)
    else:
        ind["ema_d200"] = None
        ind["adx_daily"] = 25.0

    return ind

def _detect_raw_signals(ind: dict, state: dict, reference_ms: int | None,
                         funding_rate: float | None, symbol: str = "?") -> SignalResult:
    res = SignalResult()

    btc_regime = get_btc_regime()
    breadth_snap = compute_market_breadth()
    breadth_pct = breadth_snap["breadth_50_pct"]

    o15, h15, l15, c15, v15 = ind["o15"], ind["h15"], ind["l15"], ind["c15"], ind["v15"]
    ema_f15, ema_s15 = ind["ema_f15"], ind["ema_s15"]
    rsi15_arr = ind["rsi15"]
    atr15_arr = ind["atr15"]
    adx15_arr = ind["adx15_arr"]
    bb_b15, bb_u15, bb_l15 = ind["bb_b15"], ind["bb_u15"], ind["bb_l15"]
    vol_ma15 = ind["vol_ma15"]
    obv15 = ind["obv15"]

    ef15 = safe(ema_f15[-1])
    es15 = safe(ema_s15[-1])
    r15  = safe(rsi15_arr[-1], 50.0)   # [Fix-ZEC-1] neutral fallback; prevents None >= float crash when RSI arr is short
    a15  = safe(atr15_arr[-1], 25.0)
    adx15 = safe(adx15_arr[-1], 25.0)
    bb_basis   = safe(bb_b15[-1])
    bb_upper_v = safe(bb_u15[-1])
    bb_lower_v = safe(bb_l15[-1])
    vm15 = safe(vol_ma15[-1])

    cur_c = c15[-1]; cur_o = o15[-1]
    cur_h = h15[-1]; cur_l = l15[-1]
    cur_v = v15[-1]
    prev_l = l15[-2]; prev_h = h15[-2]

    atr_val  = a15 if a15 > 0 else (cur_c * ATR_FALLBACK_PCT / 100)
    atr_pct  = atr_val / cur_c * 100
    market_ok = MIN_ATR_PCT <= atr_pct <= MAX_ATR_PCT

    # [Fix-11] Use the `symbol` parameter, not `res.symbol` (still "" here — only
    # set by compute_signals() after this function returns). Previously this wrote
    # every ATR reading into state["atr_history"][""], corrupting per-symbol
    # volatility percentiles for every symbol, silently.
    update_atr_history(state, symbol, atr_pct)

    rv         = daily_vwap(candles_15m_from_ind(ind), reference_ms=reference_ms)

    rsi_break_long  = RSI_BREAK_LONG_MIN  <= r15 <= RSI_BREAK_LONG_MAX
    rsi_break_short = RSI_BREAK_SHORT_MIN <= r15 <= RSI_BREAK_SHORT_MAX
    rsi_pull_long   = RSI_PULL_LONG_MIN   <= r15 <= RSI_PULL_LONG_MAX
    rsi_pull_short  = RSI_PULL_SHORT_MIN  <= r15 <= RSI_PULL_SHORT_MAX

    bb_mid_upper = (bb_basis + bb_upper_v) / 2 if bb_upper_v > 0 else bb_basis
    bb_mid_lower = (bb_basis + bb_lower_v) / 2 if bb_lower_v > 0 else bb_basis
    bb_long  = cur_c >= bb_mid_upper
    bb_short = cur_c <= bb_mid_lower

    obv_slope_long  = obv15[-1] > obv15[-(1 + OBV_LEN)]
    obv_slope_short = obv15[-1] < obv15[-(1 + OBV_LEN)]

    adx_score_ok = adx15 >= ADX_SCORE_MIN
    vol_score_ok = True if vm15 == 0 else (cur_v >= vm15 * VOL_SCORE_MULT)
    vol_ratio    = (cur_v / vm15) if vm15 > 0 else None

    _ref_ts_vol = (reference_ms / 1000) if reference_ms is not None else time.time()
    _is_weekend = datetime.fromtimestamp(_ref_ts_vol, tz=timezone.utc).weekday() >= 5
    _vol_accel_bars = 1 if _is_weekend else BREAK_VOL_ACCEL_BARS
    vol_accel_ok = all(
        cur_v > v15[-(i + 2)]
        for i in range(_vol_accel_bars)
    ) if len(v15) > _vol_accel_bars + 1 else False

    # [PATCH-2] Fix dead branch. For PULL signals, healthy volume is LOW
    # (declining into the EMA is the classic retracement signature). Accept
    # any vol_ratio in [0.10, 1.30] as "healthy" for PULL display purposes.
    # Values > 1.30 on a pullback candle suggest distribution or reversal.
    # The base score still uses vol_score_ok (BREAK threshold, >= 0.75x) —
    # the PULL low-vol correction in _apply_scoring_and_filters (PATCH-3)
    # restores the missed base-score point for the 0.10–0.75x band.
    PULL_VOL_RATIO_MIN: float = 0.10
    PULL_VOL_RATIO_MAX: float = 1.30
    vol_score_ok_pull = True if vm15 == 0 else (
        PULL_VOL_RATIO_MIN <= (cur_v / vm15) <= PULL_VOL_RATIO_MAX
    )

    ef1h = safe(ind["ema_f1h"][-1]); es1h = safe(ind["ema_s1h"][-1])
    h1_bull = ef1h > es1h
    h1_bear = ef1h < es1h
    r1h = safe(ind["rsi1h"][-1], 50.0)  # [Fix-ZEC-2] neutral fallback; prevents None >= float crash when 1H RSI arr is short

    rsi_divergence = {"type": None, "strength": 0}
    if USE_1H_RSI_DIVERGENCE:
        rsi_divergence = detect_rsi_divergence(
            ind["c1h"], ind["rsi1h"], ind["h1h"], ind["l1h"], DIVERGENCE_LOOKBACK
        )

    ef4h = safe(ind["ema_f4h"][-1]); es4h = safe(ind["ema_s4h"][-1])
    adx4h = safe(ind["adx4h_arr"][-1], 25.0)
    h4_bull = ef4h > es4h
    h4_bear = ef4h < es4h

    adx4h_score_ok = adx4h >= ADX_SCORE_MIN

    def trend_held(ef_arr, es_arr, bull: bool) -> bool:
        # [AUDIT-P5-5] With TREND_HOLD_BARS=2, offset ranges 1..2 so idx = -2, -3 only —
        # this checks bars [-2] and [-3], NEVER the current/most-recent closed bar [-1].
        # The current bar's EMA relationship is checked separately via h4_bull/h4_bear
        # (computed above from ef4h/es4h, i.e. ef_arr[-1]/es_arr[-1]) and combined at call
        # sites as e.g. "h4_bull and h4_trend_held_bull". Practical effect: if the 4H EMA
        # pair just crossed on the current bar, h4_trend_held_* can still read True for up
        # to TREND_HOLD_BARS bars after the cross, because it's only looking at the bars
        # BEFORE the cross, not confirming the new direction has itself persisted yet. The
        # combined h4_bull-and-h4_trend_held_bull check is what actually requires the new
        # direction (current bar) AND a prior hold (bars -2/-3) — trend_held() alone does
        # not guard against a freshly-reversed pair.
        for offset in range(1, TREND_HOLD_BARS + 1):
            idx = -(offset + 1)
            if len(ef_arr) < offset + 2:
                return False
            ef_v = safe(ef_arr[idx])
            es_v = safe(es_arr[idx])
            if bull and ef_v <= es_v:
                return False
            if not bull and ef_v >= es_v:
                return False
        return True

    h4_trend_held_bull = trend_held(ind["ema_f4h"], ind["ema_s4h"], True)
    h4_trend_held_bear = trend_held(ind["ema_f4h"], ind["ema_s4h"], False)

    if ind["ema_d200"] is not None:
        d200 = safe(ind["ema_d200"][-1])
    else:
        d200 = cur_c
    adx_daily = ind["adx_daily"]

    d200_above = cur_c > d200
    d200_below = cur_c < d200
    daily_adx_ok = adx_daily >= MIN_DAILY_ADX if USE_DAILY_ADX else True

    full_long_align  = h4_bull and h4_trend_held_bull and h1_bull
    full_short_align = h4_bear and h4_trend_held_bear and h1_bear

    _f4h_back = safe(ind["ema_f4h"][-(EXHAUSTION_SPREAD_LOOKBACK + 1)])
    _s4h_back = safe(ind["ema_s4h"][-(EXHAUSTION_SPREAD_LOOKBACK + 1)])
    h4_spread_now  = ef4h - es4h
    h4_spread_prev = _f4h_back - _s4h_back

    if USE_EXHAUSTION_SHORT:
        spread_abs_narrowing_bull = (abs(h4_spread_now) < abs(h4_spread_prev)) and (h4_spread_now > 0)
        exhaustion_short_align = (
            h4_bull
            and not h4_trend_held_bull
            and spread_abs_narrowing_bull
            and h1_bear
        )
    else:
        exhaustion_short_align = False

    if USE_EXHAUSTION_LONG:
        spread_abs_narrowing_bear = (abs(h4_spread_now) < abs(h4_spread_prev)) and (h4_spread_now < 0)
        exhaustion_long_align = (
            h4_bear
            and not h4_trend_held_bear
            and spread_abs_narrowing_bear
            and h1_bull
        )
    else:
        exhaustion_long_align = False

    if USE_PULLBACK_ALIGNMENT:
        pullback_long_align  = h4_bull and h4_trend_held_bull and h1_bear
        pullback_short_align = h4_bear and h4_trend_held_bear and h1_bull
    else:
        pullback_long_align  = False
        pullback_short_align = False

    # [Fix-37] Section 6 Item 10 — per-symbol/per-regime override of
    # PULL_REQUIRES_4H instead of a single global flip. See
    # PULL_REQUIRES_4H_OVERRIDE/get_pull_requires_4h() near PULL_REQUIRES_4H's
    # definition; falls back to the global PULL_REQUIRES_4H (True) when no
    # override is set for this symbol/regime, so behavior is unchanged until a
    # reviewer populates PULL_REQUIRES_4H_OVERRIDE.
    _pull_requires_4h = get_pull_requires_4h(symbol, btc_regime)
    pull_long_align  = (h4_bull and h4_trend_held_bull and h1_bull) \
                        if _pull_requires_4h else (h4_trend_held_bull and h1_bull)
    pull_short_align = (
        (h4_bear and h4_trend_held_bear and h1_bear)
        if _pull_requires_4h
        else (h4_trend_held_bear and h1_bear)
    )
    if USE_EXHAUSTION_SHORT and exhaustion_short_align:
        pull_short_align = True
    if USE_EXHAUSTION_LONG and exhaustion_long_align:
        pull_long_align = True
    if USE_PULLBACK_ALIGNMENT and pullback_long_align:
        pull_long_align = True
    if USE_PULLBACK_ALIGNMENT and pullback_short_align:
        pull_short_align = True

    _is_pullback_long  = USE_PULLBACK_ALIGNMENT and pullback_long_align
    _is_pullback_short = USE_PULLBACK_ALIGNMENT and pullback_short_align
    # Phase 6 (Fix G) — VWAP Restoration for Pullback Mode
    if ENABLE_PULL_VWAP_RESTORATION:
        vwap_long  = (cur_c > rv if USE_ROLLING_VWAP else True)
        vwap_short = (cur_c < rv if USE_ROLLING_VWAP else True)
    else:
        vwap_long  = True if _is_pullback_long  else (cur_c > rv if USE_ROLLING_VWAP else True)
        vwap_short = True if _is_pullback_short else (cur_c < rv if USE_ROLLING_VWAP else True)
        # Soft-penalty fallback (commented):
        # if res.signal_type == "PULL" and USE_ROLLING_VWAP:
        #     if direction == "long" and cur_c < rv * 0.999:
        #         adjusted_score -= 1
        #         adjs.append(("PULL long below VWAP (adverse entry zone)", -1))
        #     elif direction == "short" and cur_c > rv * 1.001:
        #         adjusted_score -= 1
        #         adjs.append(("PULL short above VWAP (adverse entry zone)", -1))

    rng = cur_h - cur_l + 1e-10

    def clean_bull_bar():
        return (cur_h - max(cur_o, cur_c)) / rng <= WICK_FILTER
    def clean_bear_bar():
        return (min(cur_o, cur_c) - cur_l) / rng <= WICK_FILTER
    def close_in_top_range():
        return (cur_c - cur_l) / rng >= (1.0 - RANGE_PCT_BREAK)
    def close_in_bot_range():
        return (cur_c - cur_l) / rng <= RANGE_PCT_BREAK

    # Phase 7 (Fix I) — ADX Expanding Check
    if ENABLE_ADX_EXPANDING_CHECK:
        _adx15_now  = adx15
        _adx15_prev = (safe(adx15_arr[-(ADX_RISING_LOOKBACK + 1)], adx15)
                       if len(adx15_arr) > ADX_RISING_LOOKBACK + 1 else adx15)
        adx15_rising = _adx15_now > _adx15_prev + 1.0
        adx_break_ok = (adx15 >= ADX_BREAK_GATE) or (adx15 >= 20.0 and adx15_rising)
    else:
        adx15_rising = False
        adx_break_ok = adx15 >= ADX_BREAK_GATE

    break_bull_bar = cur_c > cur_o and clean_bull_bar() and close_in_top_range()
    break_bear_bar = cur_c < cur_o and clean_bear_bar() and close_in_bot_range()

    pull_zone          = atr_val * PULL_ZONE_MULT
    pull_touched_long  = any(
        l15[-(i + 1)] <= safe(ema_f15[-(i + 1)]) + pull_zone
        for i in range(0, PULL_TOUCH_LOOKBACK + 1)
    )
    pull_touched_short = any(
        h15[-(i + 1)] >= safe(ema_f15[-(i + 1)]) - pull_zone
        for i in range(0, PULL_TOUCH_LOOKBACK + 1)
    )
    # Phase 3 (Fix C) — Pullback Depth Gate
    if ENABLE_PULL_DEPTH_GATE:
        pull_depth_long = any(
            safe(ema_f15[-(i + 1)]) - l15[-(i + 1)] >= atr_val * PULL_MIN_DEPTH_ATR
            for i in range(0, PULL_TOUCH_LOOKBACK + 1)
        )
        pull_depth_short = any(
            h15[-(i + 1)] - safe(ema_f15[-(i + 1)]) >= atr_val * PULL_MIN_DEPTH_ATR
            for i in range(0, PULL_TOUCH_LOOKBACK + 1)
        )
    else:
        pull_depth_long = pull_depth_short = True
    _pull_recover_mult  = get_pull_recover_mult(btc_regime)
    pull_recover_long   = (cur_c > ef15 + atr_val * _pull_recover_mult) and cur_c > cur_o
    pull_recover_short  = (cur_c < ef15 - atr_val * _pull_recover_mult) and cur_c < cur_o
    pull_bull_bar      = cur_c > cur_o and clean_bull_bar()
    pull_bear_bar      = cur_c < cur_o and clean_bear_bar()

    # Phase 7 (Fix I) — Score rebalance: replace adx4h_score_ok with h4_vol_expansion
    if ENABLE_SCORE_REBALANCE:
        h4_vol_ma = ind.get("vol_ma4h")
        v4h = ind.get("v4h", [])
        h4_vol_expansion = (h4_vol_ma is not None and len(v4h) > 0
                             and v4h[-1] > h4_vol_ma[-1] * 1.2)
    else:
        h4_vol_expansion = adx4h_score_ok

    long_score = sum([
        bb_long,
        h4_bull and h4_trend_held_bull,
        adx_score_ok,
        obv_slope_long,
        vol_score_ok,
        h4_vol_expansion,
    ])
    short_score = sum([
        bb_short,
        h4_bear and h4_trend_held_bear,
        adx_score_ok,
        obv_slope_short,
        vol_score_ok,
        h4_vol_expansion,
    ])

    long_break  = (daily_adx_ok and (full_long_align or exhaustion_long_align) and break_bull_bar
                   and adx_break_ok and rsi_break_long
                   and long_score  >= MIN_SCORE and market_ok)
    short_break = (daily_adx_ok
                   and (full_short_align or exhaustion_short_align)
                   and break_bear_bar
                   and adx_break_ok and rsi_break_short
                   and short_score >= MIN_SCORE and market_ok)

    # Phase 8 (Fix J) — Two-Tier BREAK Engine: Early (Tier 2) BREAK detection
    if ENABLE_EARLY_BREAK_TIER:
        prior_highs_max = max(h15[-(i + 2)] for i in range(EARLY_BREAK_PRIOR_HIGHS_LOOKBACK))
        prior_lows_min  = min(l15[-(i + 2)] for i in range(EARLY_BREAK_PRIOR_HIGHS_LOOKBACK))

        # Check if overextended (reuse Phase 1 logic)
        _bar_range_mult = (cur_h - cur_l) / atr_val if atr_val > 0 else 0.0
        _ema_dist_mult  = abs(cur_c - safe(ema_f15[-1])) / atr_val if atr_val > 0 else 0.0
        _lb = BREAK_OVEREXT_LOOKBACK
        _multibar_move  = (abs(cur_c - ind["c15"][-(_lb + 1)]) / atr_val
                           if len(ind["c15"]) > _lb and atr_val > 0 else 0.0)
        OVEREXT_CHECKS = [
            _bar_range_mult >= BREAK_OVEREXT_ATR_MULT,
            _ema_dist_mult  >= BREAK_MA_DIST_ATR_MULT,
            _multibar_move  >= BREAK_MULTIBAR_ATR_MULT,
        ]
        break_overextended = sum(OVEREXT_CHECKS) >= 2

        early_break_long = (
            cur_h > prior_highs_max
            and cur_c > cur_o
            and (not EARLY_BREAK_REQUIRE_ADX_RISING or adx15_rising)
            and full_long_align
            and rsi_break_long
            and not break_overextended
            and market_ok
        )
        early_break_short = (
            cur_l < prior_lows_min
            and cur_c < cur_o
            and (not EARLY_BREAK_REQUIRE_ADX_RISING or adx15_rising)
            and full_short_align
            and rsi_break_short
            and not break_overextended
            and market_ok
        )
    else:
        early_break_long = early_break_short = False

    long_pull  = (daily_adx_ok and pull_long_align and pull_touched_long
                  and pull_depth_long and pull_recover_long  and pull_bull_bar and vwap_long  and rsi_pull_long
                  and long_score  >= MIN_SCORE and market_ok)
    short_pull = (daily_adx_ok
                  and pull_short_align
                  and pull_touched_short
                  and pull_depth_short and pull_recover_short and pull_bear_bar and vwap_short and rsi_pull_short
                  and short_score >= MIN_SCORE and market_ok)

    long_sig  = long_break  or long_pull or early_break_long
    short_sig = short_break or short_pull or early_break_short

    res._ind = ind
    res._ctx = {
        "btc_regime": btc_regime, "breadth_pct": breadth_pct,
        "r15": r15, "r1h": r1h, "adx15": adx15, "atr_val": atr_val, "atr_pct": atr_pct,
        "market_ok": market_ok, "d200": d200, "d200_above": d200_above, "d200_below": d200_below,
        "daily_adx_ok": daily_adx_ok, "vol_ratio": vol_ratio, "vol_accel_ok": vol_accel_ok,
        "vol_score_ok": vol_score_ok, "vol_score_ok_pull": vol_score_ok_pull,
        "h1_bull": h1_bull, "h1_bear": h1_bear, "h4_bull": h4_bull, "h4_bear": h4_bear,
        "h4_trend_held_bull": h4_trend_held_bull, "h4_trend_held_bear": h4_trend_held_bear,
        "full_long_align": full_long_align, "full_short_align": full_short_align,
        "exhaustion_short_align": exhaustion_short_align, "exhaustion_long_align": exhaustion_long_align,
        "pullback_long_align": pullback_long_align, "pullback_short_align": pullback_short_align,
        "rsi_divergence": rsi_divergence,
        "bb_long": bb_long, "bb_short": bb_short,
        "obv_slope_long": obv_slope_long, "obv_slope_short": obv_slope_short,
        "adx_score_ok": adx_score_ok, "adx4h": adx4h, "adx4h_score_ok": adx4h_score_ok,
        "ef15": ef15, "es15": es15, "ef1h": ef1h, "es1h": es1h, "ef4h": ef4h, "es4h": es4h,
        "candles_15m_len": len(c15),
        "adx15_rising": adx15_rising,
        "pull_depth_long": pull_depth_long,
        "pull_depth_short": pull_depth_short,
    }

    if long_sig:
        res.fire_long   = True
        if early_break_long:
            res.signal_type = "BREAK"
            res.break_tier = "early"
        elif long_break and long_pull and pull_touched_long:
            res.signal_type = "BREAK"    # [FIX-AUDIT-D9] BREAK has higher R:R; prefer it over PULL when both fire
            res.break_tier = "standard"
            print(f"[D9] Dual BREAK+PULL long on {res.symbol} — classified as BREAK (higher R:R)")
        elif long_break:
            res.signal_type = "BREAK"
            res.break_tier = "standard"
        else:
            res.signal_type = "PULL"
            res.break_tier = "standard"
        res.score       = long_score
        _setup_signal_entry(res, cur_c, atr_val)
    elif short_sig:
        res.fire_short  = True
        if early_break_short:
            res.signal_type = "BREAK"
            res.break_tier = "early"
        elif short_break and short_pull and pull_touched_short:
            res.signal_type = "BREAK"    # [FIX-AUDIT-D9] BREAK has higher R:R; prefer it over PULL when both fire
            res.break_tier = "standard"
            print(f"[D9] Dual BREAK+PULL short on {res.symbol} — classified as BREAK (higher R:R)")
        elif short_break:
            res.signal_type = "BREAK"
            res.break_tier = "standard"
        else:
            res.signal_type = "PULL"
            res.break_tier = "standard"
        res.score       = short_score
        _setup_signal_entry(res, cur_c, atr_val)

    # [AUDIT-P4-2] This recalculation is NOT redundant with the vol_score_ok_pull computed
    # earlier (pre-classification, ~line 2563-ish, stored into res._ctx below at "vol_score_ok_pull":
    # vol_score_ok_pull). That earlier value always uses the PULL-style symmetric band
    # (PULL_VOL_RATIO_MIN <= cur_v/vm15 <= PULL_VOL_RATIO_MAX, i.e. [0.10, 1.30]) because
    # signal_type hasn't been decided yet at that point. Here, signal_type IS known, and a
    # BREAK signal needs HIGH volume confirmation (cur_v >= vm15 * VOL_SCORE_MULT, i.e. >=
    # 0.75x with no upper cap) rather than the PULL-style "not too high" band — those two
    # conditions diverge for any BREAK signal with cur_v/vm15 outside [0.75, 1.30] (e.g. a
    # legitimate high-volume breakout at 2.0x would be vol_score_ok_pull=True pre-classification
    # under the old PULL band's upper bound check, but the signal-type-aware band here is the
    # one actually consumed downstream by format_signal's VOL✓/VOL✗ display (ctx.get(
    # "vol_score_ok_pull", True) at ~line 3879). Confirmed intentional override, not a bug —
    # kept as-is, no removal.
    if res.fire_long or res.fire_short:
        res._ctx["vol_score_ok_pull"] = True if vm15 == 0 else (
            cur_v >= vm15 * VOL_SCORE_MULT if res.signal_type == "BREAK"
            else cur_v <= vm15 * 1.3
        )

    return res

def candles_15m_from_ind(ind: dict) -> list[dict]:
    candles = []
    for i in range(len(ind["c15"])):
        candles.append({"t": 0, "o": ind["o15"][i], "h": ind["h15"][i],
                        "l": ind["l15"][i], "c": ind["c15"][i], "v": ind["v15"][i]})
    return candles

def _setup_signal_entry(res: SignalResult, cur_c: float, atr_val: float):
    res.entry        = cur_c
    zone_half_width  = atr_val * PULL_ZONE_MULT
    res.entry_low    = cur_c - zone_half_width
    res.entry_high   = cur_c + zone_half_width
    res.atr_val      = atr_val

def _apply_scoring_and_filters(res: SignalResult, state: dict,
                                symbol: str, reference_ms: int | None,
                                funding_rate: float | None,
                                candles_15m: list[dict],
                                # [Fix-43] Threaded through so the liquidity
                                # engine's analyze() call below can run real
                                # HTF liquidity analysis instead of always
                                # being passed None — see call site for the
                                # previous candles_1h=None/candles_4h=None.
                                candles_1h: list[dict] | None = None,
                                candles_4h: list[dict] | None = None) -> SignalResult:
    def _adj(score: float, adjs: list, label: str, delta: float) -> float:
        """Apply a score adjustment and record it in adjs atomically. [FIX-AUDIT-D4]"""
        adjs.append((label, delta))
        return score + delta
    ind = res._ind
    ctx = res._ctx
    rsi_divergence = ctx.get("rsi_divergence")   # [FIX-AUDIT-D1] moved here — was used before assignment at ~line 3495
    btc_regime = ctx["btc_regime"]
    breadth_pct = ctx["breadth_pct"]

    if not (res.fire_long or res.fire_short):
        return res

    direction = "long" if res.fire_long else "short"
    cur_c = res.entry
    cur_o = ind["o15"][-1]
    cur_h = ind["h15"][-1]
    cur_l = ind["l15"][-1]
    cur_v = ind["v15"][-1]
    price_dir = "up" if cur_c > cur_o else "down"
    atr_val = res.atr_val
    # [Fix-39] res.atr_pct was never populated before this point (only atr_val is
    # copied onto res by _setup_signal_entry) — reading it here pulled the class
    # default of 0.0 on every single signal, which made recommended_leverage()'s
    # `sl_pct <= 0` guard fire unconditionally downstream, so leverage always
    # displayed "1x" regardless of real volatility. The correct value was sitting
    # unused in ctx["atr_pct"] the whole time (set by _detect_raw_signals).
    atr_pct = ctx["atr_pct"]

    oi_data = compute_oi_trend(state, symbol, cur_c, price_dir, direction)
    res.oi_trend_data = oi_data
    adjusted_score = res.score + oi_data["score_adj"]
    adjs = res.score_adjustments
    if oi_data["score_adj"] != 0:
        adjs.append((f"OI ({oi_data['breakdown_tag']})", oi_data["score_adj"]))

    oi_acceleration = oi_data.get("oi_acceleration")
    oi_trend        = oi_data.get("oi_trend", "unknown")
    oi_contribution = oi_data["score_adj"]

    if oi_acceleration is not None:
        if (oi_acceleration > OI_ACCEL_MIN_THRESHOLD
                and oi_trend == "rising"
                and oi_data["score_adj"] > 0
                and oi_contribution < OI_SCORE_CAP):
            adjusted_score += 1
            oi_contribution += 1
            adjs.append(("OI Acceleration (confirming↑)", 1))
        elif (oi_acceleration < -OI_ACCEL_MIN_THRESHOLD
                and oi_trend == "falling"
                and oi_data["score_adj"] < 0
                and oi_contribution > -OI_SCORE_CAP):
            adjusted_score -= 1
            oi_contribution -= 1
            adjs.append(("OI Acceleration (diverging↓)", -1))

    btc_adj, btc_label = check_btc_regime_filter(direction, symbol, res.signal_type)
    res.btc_regime_label = btc_label
    adjusted_score += btc_adj
    if btc_adj != 0:
        adjs.append((btc_label, btc_adj))

    rs_data      = compute_relative_strength(symbol)
    res.rs_data  = rs_data

    breadth_adj, breadth_label = apply_breadth_adjustment(
        direction, rs_pct=rs_data.get("rs_pct"), btc_regime=btc_regime
    )
    res.breadth_label = breadth_label
    adjusted_score += breadth_adj
    if breadth_adj != 0:
        adjs.append((breadth_label, breadth_adj))

    if (direction == "short"
            and USE_EXHAUSTION_SHORT
            and ctx["exhaustion_short_align"]
            and not ctx["full_short_align"]):
        if oi_data.get("oi_trend") == "falling":
            adjs.append(("Exhaustion + OI confirms (no penalty)", 0))
        else:
            adjusted_score = _adj(adjusted_score, adjs, "Exhaustion short mode (4H spread narrowing, not yet confirmed)", EXHAUSTION_SHORT_SCORE_ADJ)  # [FIX-AUDIT-D4]

    if (direction == "long"
            and USE_EXHAUSTION_LONG
            and ctx["exhaustion_long_align"]
            and not ctx["full_long_align"]):
        if oi_data.get("oi_trend") == "rising":
            adjs.append(("Exhaustion + OI confirms (no penalty)", 0))
        else:
            adjusted_score = _adj(adjusted_score, adjs, "Exhaustion long mode (4H spread narrowing, not yet confirmed)", EXHAUSTION_LONG_SCORE_ADJ)  # [FIX-AUDIT-D4]

    if (direction == "long" and USE_PULLBACK_ALIGNMENT
            and ctx["pullback_long_align"] and not ctx["full_long_align"]):
        adjusted_score = _adj(adjusted_score, adjs, "Pullback mode (1H dip inside held 4H uptrend)", PULLBACK_SCORE_ADJ)  # [FIX-AUDIT-D4]

    if (direction == "short" and USE_PULLBACK_ALIGNMENT
            and ctx["pullback_short_align"] and not ctx["full_short_align"]):
        adjusted_score = _adj(adjusted_score, adjs, "Pullback mode (1H bounce inside held 4H downtrend)", PULLBACK_SCORE_ADJ)  # [FIX-AUDIT-D4]

    if direction == "short":
        if ctx["full_short_align"]:
            res.alignment_mode = "full"
        elif USE_EXHAUSTION_SHORT and ctx["exhaustion_short_align"]:
            res.alignment_mode = "exhaustion"
        elif USE_PULLBACK_ALIGNMENT and ctx["pullback_short_align"]:
            res.alignment_mode = "pullback"
        else:
            res.alignment_mode = "full"
    else:
        if ctx["full_long_align"]:
            res.alignment_mode = "full"
        elif USE_EXHAUSTION_LONG and ctx["exhaustion_long_align"]:
            res.alignment_mode = "exhaustion"
        elif USE_PULLBACK_ALIGNMENT and ctx["pullback_long_align"]:
            res.alignment_mode = "pullback"
        else:
            res.alignment_mode = "full"

    _ref_ms       = reference_ms if reference_ms is not None else int(time.time() * 1000)
    iv_4h_ms      = 4 * 60 * 60 * 1000
    bar_open_4h_ms = (_ref_ms // iv_4h_ms) * iv_4h_ms
    bar_age_frac  = (_ref_ms - bar_open_4h_ms) / iv_4h_ms
    atr1h_val = safe(atr(ctx.get("h1h", ind["h1h"]), ctx.get("l1h", ind["l1h"]),
                         ctx.get("c1h", ind["c1h"]), ATR_LEN)[-1]) if len(ind["h1h"]) > ATR_LEN else 0.0
    _h1_spread    = (ctx["ef1h"] - ctx["es1h"]) / atr1h_val if atr1h_val > 0 else 0.0
    h4_stale_bias = (
        bar_age_frac >= H4_STALE_AGE_FRACTION
        and ((direction == "long"  and _h1_spread < H4_STALE_SPREAD_MIN)
          or (direction == "short" and _h1_spread > -H4_STALE_SPREAD_MIN))
    )
    _is_mixed_local = (btc_regime is not None
                       and not btc_regime.get("bullish")
                       and not btc_regime.get("bearish"))
    if h4_stale_bias and not _is_mixed_local:
        # Cross-cutting: Increase penalty for PULL signals (Low/L1)
        penalty = -2 if res.signal_type == "PULL" else -1
        adjusted_score = _adj(adjusted_score, adjs, f"4H bias stale ({bar_age_frac*100:.0f}% into bar, 1H spread {_h1_spread:+.2f}x ATR)", penalty)  # [FIX-AUDIT-D4]

    adjusted_score += rs_data["score_adj"]
    if rs_data["score_adj"] != 0:
        adjs.append((rs_data["label"], rs_data["score_adj"]))

    wr_data = compute_win_rate_analytics(
        state, symbol, direction, res.signal_type, res.score, res.alignment_mode
    )
    res.win_rate_data = wr_data
    adjusted_score += wr_data["score_adj"]
    if wr_data["score_adj"] != 0:
        adjs.append((wr_data["label"], wr_data["score_adj"]))

    _atr_pctile = get_atr_percentile(state, symbol, atr_pct)
    macro_data = apply_macro_filter(state, atr_pct, reference_ms=reference_ms,
                                     atr_pctile=_atr_pctile)
    res.macro_data = macro_data
    if macro_data["hard_suppress"]:
        print(f"  [MACRO FILTER] {symbol} hard suppressed — elevated ATR + macro risk window")
        res.fire_long  = False
        res.fire_short = False
        return res
    adjusted_score += macro_data["score_adj"]
    if macro_data["score_adj"] != 0:
        adjs.append(("Macro Risk Window", macro_data["score_adj"]))

    res.supports, res.resistances = find_sr_levels(candles_15m, atr_val=atr_val)
    vol_ratio = ctx["vol_ratio"]
    vol_accel_ok = ctx["vol_accel_ok"]

    if res.signal_type == "PULL":
        # Phase 2 (Fix F) — Suppress Counter-4H-Trend PULL
        if ENABLE_COUNTERTREND_PULL_GATE and direction == "long" \
                and ctx.get("h4_bear") and ctx.get("h4_trend_held_bear"):
            _oi_confirms  = oi_data.get("oi_trend") == "rising"
            _div_confirms = (rsi_divergence.get("type") == "bullish"
                             if isinstance(rsi_divergence, dict) else False)
            if not (_oi_confirms and _div_confirms):
                adjusted_score = _adj(adjusted_score, adjs, "PULL long into active 4H downtrend (no OI+divergence confirmation)", -2)  # [FIX-AUDIT-D4]
                # Hard-gate alternative (use if -2 still leaks losing trades after backtest):
                # res.fire_long = False; return res

        if ENABLE_COUNTERTREND_PULL_GATE and direction == "short" \
                and ctx.get("h4_bull") and ctx.get("h4_trend_held_bull"):
            _oi_confirms  = oi_data.get("oi_trend") == "rising"
            _div_confirms = (rsi_divergence.get("type") == "bearish"
                             if isinstance(rsi_divergence, dict) else False)
            if not (_oi_confirms and _div_confirms):
                adjusted_score = _adj(adjusted_score, adjs, "PULL short into active 4H uptrend (no OI+divergence confirmation)", -2)  # [FIX-AUDIT-D4]
                # Hard-gate alternative (use if -2 still leaks losing trades after backtest):
                # res.fire_short = False; return res

        # [PATCH-8] Removed price_dir == "down" guard. On a PULL entry bar price
        # is always "up" (green recovery bar is required by pull_recover_long), so
        # the original condition was structurally always False — OI falling on PULL
        # was never penalized. OI falling on the recovery bar means no institutional
        # longs are being added as price recovers: genuinely low conviction.
        _oi_falling_bearish = (
            oi_data.get("oi_trend") == "falling"
            and oi_data.get("score_adj", 0) == 0
        )
        if _oi_falling_bearish:
            adjusted_score = _adj(adjusted_score, adjs, "OI falling on PULL entry (no new longs on recovery)", -1)  # [FIX-AUDIT-D4]

        if vol_ratio is not None:
            _vol_floor = (PULL_VOL_FLOOR_OVERBOUGHT
                            if breadth_pct > PULL_VOL_OVERBOUGHT_BREADTH
                            else PULL_VOL_FLOOR)
            if vol_ratio <= _vol_floor:
                # Very low volume — lack of buyer conviction.
                adjusted_score = _adj(adjusted_score, adjs, f"Vol floor (ratio {vol_ratio:.2f}x <= {_vol_floor:.2f}x"                              f"{' overbought' if breadth_pct > PULL_VOL_OVERBOUGHT_BREADTH else ''})", -1)  # [FIX-AUDIT-D4]
            elif vol_ratio < VOL_SCORE_MULT:
                # [PATCH-3] Restored from v15.5.5.1. Vol is between the floor and
                # the BREAK threshold (0.40x–0.75x): classic declining-volume pullback
                # signature. Base score penalised it (vol_score_ok failed), restore +1
                # here. Mutually exclusive with floor check: floor fires ≤ 0.40x,
                # this fires 0.40x < vol < 0.75x.
                adjusted_score = _adj(adjusted_score, adjs, "PULL low-vol correction (healthy pullback volume)", 1)  # [FIX-AUDIT-D4]

        candle_range = cur_h - cur_l
        body_size    = abs(cur_c - cur_o)
        body_ratio   = body_size / candle_range if candle_range > 0 else 1.0
        pull_body_ok = body_ratio >= PULL_BODY_MIN_RATIO
        if not pull_body_ok:
            adjusted_score = _adj(adjusted_score, adjs, f"Weak candle body (ratio {body_ratio:.2f} < {PULL_BODY_MIN_RATIO})", -1)  # [FIX-AUDIT-D4]

        # Phase 4 (Fix D) — PULL Momentum Collapse Penalty
        if ENABLE_PULL_MOMENTUM_COLLAPSE_PENALTY:
            _rsi15 = ind["rsi15"]
            if len(_rsi15) > PULL_RSI_LOOKBACK + 1:
                _rsi_now   = safe(_rsi15[-1])
                _rsi_prior = safe(_rsi15[-(PULL_RSI_LOOKBACK + 1)])
                _rsi_drop  = _rsi_prior - _rsi_now
                if direction == "long" and _rsi_drop > PULL_RSI_COLLAPSE_THRESH:
                    adjusted_score = _adj(adjusted_score, adjs, f"PULL RSI momentum collapsing ({_rsi_drop:.1f} pts drop over {PULL_RSI_LOOKBACK}bars)", -1)  # [FIX-AUDIT-D4]
                elif direction == "short" and (-_rsi_drop) > PULL_RSI_COLLAPSE_THRESH:
                    adjusted_score = _adj(adjusted_score, adjs, f"PULL RSI momentum reversing ({-_rsi_drop:.1f} pts rise over {PULL_RSI_LOOKBACK}bars)", -1)  # [FIX-AUDIT-D4]

        # Phase 5 (Fix H) — PULL RSI lower-bound tightening (soft penalty version)
        if ENABLE_PULL_RSI_TIGHTENING:
            if direction == "long" and ctx["r15"] < 44.0:
                adjusted_score = _adj(adjusted_score, adjs, f"PULL long RSI near oversold territory ({ctx['r15']:.0f}) — momentum may have broken", -1)  # [FIX-AUDIT-D4]
            elif direction == "short" and ctx["r15"] > 56.0:
                adjusted_score = _adj(adjusted_score, adjs, f"PULL short RSI near overbought territory ({ctx['r15']:.0f}) — momentum may have broken", -1)  # [FIX-AUDIT-D4]
            # Hard-gate alternative (commented):
            # if ENABLE_PULL_RSI_TIGHTENING:
            #     RSI_PULL_LONG_MIN = 42.0
            #     RSI_PULL_SHORT_MAX = 58.0
            #     RSI_PULL_SHORT_MIN = 42.0

        # Phase 9 — Quality-Stratified PULL Engine
        if ENABLE_PULL_TIERING:
            # Tier C — Prohibited PULL (hard gates)
            if (res.signal_type == "PULL" and direction == "long"
                    and ctx.get("h4_bear") and ctx.get("h4_trend_held_bear") and ctx["r15"] < 44.0):
                res.fire_long = False
                if hasattr(res, "_ind"): del res._ind  # [FIX-AUDIT-D10]
                if hasattr(res, "_ctx"): del res._ctx  # [FIX-AUDIT-D10]
                return res
            if (res.signal_type == "PULL" and direction == "short"
                    and ctx.get("h4_bull") and ctx.get("h4_trend_held_bull") and ctx["r15"] > 56.0):
                res.fire_short = False
                if hasattr(res, "_ind"): del res._ind  # [FIX-AUDIT-D10]
                if hasattr(res, "_ctx"): del res._ctx  # [FIX-AUDIT-D10]
                return res
            if res.signal_type == "PULL" and direction == "long" and breadth_pct > TIER_C_BREADTH_EXTREME_LONG_THRESHOLD:
                res.fire_long = False
                if hasattr(res, "_ind"): del res._ind  # [FIX-AUDIT-D10]
                if hasattr(res, "_ctx"): del res._ctx  # [FIX-AUDIT-D10]
                return res
            if res.signal_type == "PULL" and direction == "short" and breadth_pct < TIER_C_BREADTH_EXTREME_SHORT_THRESHOLD:
                res.fire_short = False
                if hasattr(res, "_ind"): del res._ind  # [FIX-AUDIT-D10]
                if hasattr(res, "_ctx"): del res._ctx  # [FIX-AUDIT-D10]
                return res

            # Structural support requirement
            if res.signal_type == "PULL" and direction == "long":
                has_structural_backing = False
                if res.supports:
                    nearest_sup = res.supports[0]
                    sup_dist_atr = (res.entry - nearest_sup) / atr_val if atr_val > 0 else 999
                    has_structural_backing = sup_dist_atr <= 1.0
                if not has_structural_backing:
                    adjusted_score = _adj(adjusted_score, adjs, "PULL long: no structural support within 1 ATR (weak zone entry)", -1)  # [FIX-AUDIT-D4]
            elif res.signal_type == "PULL" and direction == "short":
                has_structural_backing = False
                if res.resistances:
                    nearest_res = res.resistances[0]
                    res_dist_atr = (nearest_res - res.entry) / atr_val if atr_val > 0 else 999
                    has_structural_backing = res_dist_atr <= 1.0
                if not has_structural_backing:
                    adjusted_score = _adj(adjusted_score, adjs, "PULL short: no structural resistance within 1 ATR (weak zone entry)", -1)  # [FIX-AUDIT-D4]

            # Pullback volume-quality filter
            PULL_VOL_COMPARE_LOOKBACK = 3
            _vol_during_pullback = [v15[-(i + 1)] for i in range(1, PULL_VOL_COMPARE_LOOKBACK + 1)]
            _vol_before_pullback = [v15[-(i + 1)] for i in range(PULL_VOL_COMPARE_LOOKBACK + 1,
                                                              PULL_VOL_COMPARE_LOOKBACK * 2 + 1)]
            if _vol_before_pullback:
                avg_pullback_vol  = sum(_vol_during_pullback) / len(_vol_during_pullback)
                avg_pre_break_vol = sum(_vol_before_pullback) / len(_vol_before_pullback)
                if avg_pullback_vol > avg_pre_break_vol * 1.2:
                    adjusted_score = _adj(adjusted_score, adjs, f"PULL volume during pullback ({avg_pullback_vol:.0f}) not lower than "                                   f"impulse ({avg_pre_break_vol:.0f}) — possible distribution", -1)  # [FIX-AUDIT-D4]

            # Tier A — High-Conviction PULL (bonus)
            pull_depth_long = ctx.get("pull_depth_long", True)
            pull_depth_short = ctx.get("pull_depth_short", True)
            pull_recover_long = (cur_c > ctx["ef15"] + atr_val * get_pull_recover_mult(btc_regime)) and cur_c > cur_o
            pull_recover_short = (cur_c < ctx["ef15"] - atr_val * get_pull_recover_mult(btc_regime)) and cur_c < cur_o
            rv = daily_vwap(candles_15m, reference_ms=reference_ms)  # [FIX-AUDIT-D20] pass real closed candles — candles_15m_from_ind had t=0

            if direction == "long":
                tier_a = (
                    ctx.get("h4_bull") and ctx.get("h4_trend_held_bull") and ctx.get("h1_bull")
                    and 44.0 <= ctx["r15"] <= 58.0
                    and pull_depth_long
                    and pull_recover_long
                    and oi_data.get("oi_trend") != "falling"
                    and cur_c > rv
                    and not (direction == "long" and (safe(ind["ema_f15"][-1]) - safe(ind["ema_f15"][-(1 + EMA_VELOCITY_LOOKBACK)])) / atr_val < EMA_VELOCITY_WEAK_MAX)
                )
            else:
                tier_a = (
                    ctx.get("h4_bear") and ctx.get("h4_trend_held_bear") and ctx.get("h1_bear")
                    and 44.0 <= ctx["r15"] <= 58.0
                    and pull_depth_short
                    and pull_recover_short
                    and oi_data.get("oi_trend") != "falling"
                    and cur_c < rv
                    and not (direction == "short" and (safe(ind["ema_f15"][-1]) - safe(ind["ema_f15"][-(1 + EMA_VELOCITY_LOOKBACK)])) / atr_val > -EMA_VELOCITY_WEAK_MAX)
                )

            if tier_a:
                adjusted_score = _adj(adjusted_score, adjs, "Tier A PULL — full quality confirmation", 1)  # [FIX-AUDIT-D4]
                res.pull_tier = "A"
            else:
                res.pull_tier = "B"

    if res.signal_type == "BREAK":
        # [AUDIT-P6-1] BREAK hard gate at extreme breadth — mirrors Tier C's PULL intent
        # (hard refuse, not just a soft penalty) for the same extreme-breadth condition.
        # Placed first in the BREAK block, before any BREAK scoring/penalties and well
        # before TP/SL calculation (_tp1_m/_tp2_m/_sl_m, get_regime_aware_multipliers call
        # further down) so a gated signal never gets entry/TP/SL computed at all.
        if direction == "long" and breadth_pct >= BREADTH_BREAK_LONG_SUPPRESS:
            res.fire_long = False
            if hasattr(res, "_ind"): del res._ind
            if hasattr(res, "_ctx"): del res._ctx
            return res
        if direction == "short" and breadth_pct <= BREADTH_BREAK_SHORT_SUPPRESS:
            res.fire_short = False
            if hasattr(res, "_ind"): del res._ind
            if hasattr(res, "_ctx"): del res._ctx
            return res
        if vol_ratio is not None and vol_ratio < BREAK_VOL_MULT:
            adjusted_score = _adj(adjusted_score, adjs, f"Break vol slightly low ({vol_ratio:.2f}x < {BREAK_VOL_MULT:.2f}x)", -1)  # [FIX-AUDIT-D4]
        if not vol_accel_ok:
            adjusted_score = _adj(adjusted_score, adjs, "Vol acceleration low", -1)  # [FIX-AUDIT-D4]

        if oi_data.get("oi_trend") == "flat" and (vol_ratio is None or vol_ratio < BREAK_OI_FLAT_VOL_THRESHOLD):
            adjusted_score = _adj(adjusted_score, adjs, "OI flat + low vol on BREAK (low conviction)", -1)  # [FIX-AUDIT-D4]

        # Phase 1 (Fix A) — Overextension/Blow-off Suppression for BREAK
        if ENABLE_OVEREXTENSION_GATE:   # [FIX-AUDIT-D13] outer guard already ensures signal_type == "BREAK"
            _bar_range_mult = (cur_h - cur_l) / atr_val if atr_val > 0 else 0.0
            _ema_dist_mult  = abs(cur_c - safe(ind["ema_f15"][-1])) / atr_val if atr_val > 0 else 0.0
            _lb = BREAK_OVEREXT_LOOKBACK
            _multibar_move  = (abs(cur_c - ind["c15"][-(_lb + 1)]) / atr_val
                               if len(ind["c15"]) > _lb and atr_val > 0 else 0.0)

            OVEREXT_CHECKS = [
                _bar_range_mult >= BREAK_OVEREXT_ATR_MULT,
                _ema_dist_mult  >= BREAK_MA_DIST_ATR_MULT,
                _multibar_move  >= BREAK_MULTIBAR_ATR_MULT,
                _atr_pctile     >= 0.95,
            ]
            _n_overext = sum(OVEREXT_CHECKS)
            if _n_overext >= 2:
                adjusted_score = _adj(adjusted_score, adjs, "Multiple overextension signals — likely exhaustion", -3)  # [FIX-AUDIT-D4]
            elif _n_overext == 1:
                adjusted_score = _adj(adjusted_score, adjs, "Single overextension signal — BREAK quality reduced", -1)  # [FIX-AUDIT-D4]

            # Breakout-trap detection (liquidity grab + close-back-below-level, weak OI)
            TRAP_SWH_MULT: float = 0.3
            if direction == "long":
                _prior_high_5 = max(ind["h15"][-(i + 2)] for i in range(5))
                _wick_above_prior = cur_h > _prior_high_5 and cur_c < _prior_high_5
                _wick_ratio = (cur_h - max(cur_o, cur_c)) / (cur_h - cur_l) if (cur_h - cur_l) > 0 else 0
                _trap_signal = (
                    _wick_above_prior
                    and _wick_ratio > 0.30
                    and (vol_ratio or 0) > 1.5
                    and oi_data.get("oi_trend") != "rising"
                )
                if _trap_signal:
                    adjusted_score = _adj(adjusted_score, adjs, "Possible breakout trap — wick above prior high with closing fail", -3)  # [FIX-AUDIT-D4]
            elif direction == "short":
                _prior_low_5 = min(ind["l15"][-(i + 2)] for i in range(5))
                _wick_below_prior = cur_l < _prior_low_5 and cur_c > _prior_low_5
                _wick_ratio = (min(cur_o, cur_c) - cur_l) / (cur_h - cur_l) if (cur_h - cur_l) > 0 else 0
                _trap_signal = (
                    _wick_below_prior
                    and _wick_ratio > 0.30
                    and (vol_ratio or 0) > 1.5
                    and oi_data.get("oi_trend") != "rising"
                )
                if _trap_signal:
                    adjusted_score = _adj(adjusted_score, adjs, "Possible breakout trap — wick below prior low with closing fail", -3)  # [FIX-AUDIT-D4]

        # Phase 5 (Fix B) — BREAK RSI extended zone discount
        if res.signal_type == "BREAK" and direction == "long" and ctx["r15"] > 75:
            adjusted_score = _adj(adjusted_score, adjs, f"BREAK RSI elevated ({ctx['r15']:.0f} > 75, momentum potentially exhausted)", -1)  # [FIX-AUDIT-D4]
        elif res.signal_type == "BREAK" and direction == "short" and ctx["r15"] < 25:
            adjusted_score = _adj(adjusted_score, adjs, f"BREAK RSI elevated ({ctx['r15']:.0f} < 25, momentum potentially exhausted)", -1)  # [FIX-AUDIT-D4]

        # Phase 7 (Fix I) — ADX expanding reward
        if res.signal_type == "BREAK":
            adx15_rising = ctx.get("adx15_rising", False)
            if adx15_rising and ctx["adx15"] < ADX_BREAK_GATE:
                adjusted_score = _adj(adjusted_score, adjs, f"ADX expanding ({ctx['adx15']:.0f}, rising trend energy)", 1)  # [FIX-AUDIT-D4]
            elif ctx["adx15"] >= 30.0:
                adjusted_score = _adj(adjusted_score, adjs, f"ADX strong ({ctx['adx15']:.0f})", 1)  # [FIX-AUDIT-D4]

        # Phase 8 (Fix J) — Early BREAK tier scoring
        # [Fix-J-defer] _sl_m is assigned later (get_regime_aware_multipliers); reading it
        # here caused UnboundLocalError whenever break_tier=="early" fired. Defer the widen
        # via a flag and apply it after _sl_m is defined below.
        _early_break_sl_widen = False
        if hasattr(res, 'break_tier') and res.break_tier == "early":
            # [AUDIT-P5-1] Renamed: "early break" uses prior closed-bar high/low (cur_h/cur_l
            # vs prior_highs_max/prior_lows_min over EARLY_BREAK_PRIOR_HIGHS_LOOKBACK bars),
            # not live intra-candle/tick data. Detection fires on a close that breaks the
            # prior N-bar high/low without requiring the full BREAK quality gates.
            adjusted_score = _adj(adjusted_score, adjs, "Early BREAK (short lookback close-break, no full BREAK gate confirmation)", -1)  # [FIX-AUDIT-D4]
            _early_break_sl_widen = True   # SL widened 10% below after get_regime_aware_multipliers()

        rs_pct = rs_data.get("rs_pct")
        if rs_pct is not None:
            if rs_pct < RS_BREAK_HARD_GATE_PCT:
                # [Fix-32] Previously: print + early `return res` (hard
                # suppress — fire_long/fire_short forced False, zero chance of
                # firing). Now: heavy score penalty instead, via the normal
                # adjs/adjusted_score path so it flows through the [Fix-9]
                # PENALTY STACKING CAP / PENALTY_PRIORITY logic below like any
                # other adjustment, rather than bypassing it with an early
                # return. The original print is preserved (reworded) so the
                # behavior change stays visible in logs.
                print(f"  [RS GATE] {symbol} BREAK heavily penalized (was hard-suppressed pre-Fix-32) — "
                      f"RS {rs_pct:+.1f}% < {RS_BREAK_HARD_GATE_PCT:.1f}%")
                adjusted_score = _adj(adjusted_score, adjs, f"RS negative — hard-gate breach on BREAK ({rs_pct:+.1f}% < "                              f"{RS_BREAK_HARD_GATE_PCT:.1f}%)", RS_BREAK_HARD_GATE_PENALTY)  # [FIX-AUDIT-D4]
            elif rs_pct < 0:
                rs_percentile = rs_data.get("percentile")
                if rs_percentile is not None and rs_percentile <= RS_BREAK_SOFT_PERCENTILE:
                    adjusted_score = _adj(adjusted_score, adjs, f"RS negative on BREAK ({rs_pct:+.1f}%, "                                  f"{rs_percentile*100:.0f}th pct)", -1)  # [FIX-AUDIT-D4]

    elif res.signal_type == "PULL":
        # [PATCH-4] PULL RS gate. BREAK has hard + soft gates; PULL had none.
        # A coin underperforming BTC on a PULL entry suggests structural weakness,
        # not healthy retracement. Softer thresholds than BREAK to preserve frequency.
        RS_PULL_SOFT_GATE_PCT: float = -1.0   # softer than BREAK's 0% bottom-pct check
        RS_PULL_HARD_GATE_PCT: float = -4.0   # softer than BREAK's -6.0%
        rs_pct = rs_data.get("rs_pct")
        if rs_pct is not None:
            if rs_pct < RS_PULL_HARD_GATE_PCT:
                adjusted_score = _adj(adjusted_score, adjs, f"RS strongly negative on PULL ({rs_pct:+.1f}% < "                              f"{RS_PULL_HARD_GATE_PCT:.1f}%)", -2)  # [FIX-AUDIT-D4]
            elif rs_pct < RS_PULL_SOFT_GATE_PCT:
                adjusted_score = _adj(adjusted_score, adjs, f"RS negative on PULL ({rs_pct:+.1f}% < "                              f"{RS_PULL_SOFT_GATE_PCT:.1f}%)", -1)  # [FIX-AUDIT-D4]

    if res.signal_type == "BREAK":
        if direction == "long" and res.resistances:
            nearest_res = res.resistances[0]
            if res.entry < nearest_res < res.tp1:
                tp_range = res.tp1 - res.entry
                if tp_range > 0:
                    blocked_pct = (nearest_res - res.entry) / tp_range
                    if blocked_pct >= TP1_WALL_MIN_CLEARANCE:
                        adjusted_score = _adj(adjusted_score, adjs, f"Resistance blocks TP1 ({blocked_pct*100:.0f}%)", -1)  # [FIX-AUDIT-D4]
        elif direction == "short" and res.supports:
            nearest_sup = res.supports[0]
            if res.tp1 < nearest_sup < res.entry:
                tp_range = res.entry - res.tp1
                if tp_range > 0:
                    blocked_pct = (res.entry - nearest_sup) / tp_range
                    if blocked_pct >= TP1_WALL_MIN_CLEARANCE:
                        adjusted_score = _adj(adjusted_score, adjs, f"Support blocks TP1 ({blocked_pct*100:.0f}%)", -1)  # [FIX-AUDIT-D4]

    if res.signal_type == "PULL":
        # [PATCH-5] PULL TP1 resistance wall check. Mirrors the BREAK check above.
        # PULL entries sit closer to recent swing lows, making resistance overhead
        # more likely to be within TP1 range. Uses the same TP1_WALL_MIN_CLEARANCE
        # threshold (0.40): resistance blocking ≥ 40% of path to TP1 degrades R:R.
        if direction == "long" and res.resistances:
            nearest_res = res.resistances[0]
            if res.entry < nearest_res < res.tp1:
                tp_range = res.tp1 - res.entry
                if tp_range > 0:
                    blocked_pct = (nearest_res - res.entry) / tp_range
                    if blocked_pct >= TP1_WALL_MIN_CLEARANCE:
                        adjusted_score = _adj(adjusted_score, adjs, f"Resistance blocks PULL TP1 ({blocked_pct*100:.0f}%)", -1)  # [FIX-AUDIT-D4]
        elif direction == "short" and res.supports:
            nearest_sup = res.supports[0]
            if res.tp1 < nearest_sup < res.entry:
                tp_range = res.entry - res.tp1
                if tp_range > 0:
                    blocked_pct = (res.entry - nearest_sup) / tp_range
                    if blocked_pct >= TP1_WALL_MIN_CLEARANCE:
                        adjusted_score = _adj(adjusted_score, adjs, f"Support blocks PULL TP1 ({blocked_pct*100:.0f}%)", -1)  # [FIX-AUDIT-D4]

    if USE_D200_FILTER:
        if direction == "long" and ctx["d200_above"]:
            adjusted_score = _adj(adjusted_score, adjs, f"D200: Price above (+{D200_SOFT_ADJ})", D200_SOFT_ADJ)  # [FIX-AUDIT-D4]
            res.d200_label = f"D200: Price above (+{D200_SOFT_ADJ})"
        elif direction == "long" and ctx["d200_below"]:
            adjusted_score = _adj(adjusted_score, adjs, f"D200: Price below (-{D200_SOFT_ADJ})", -D200_SOFT_ADJ)  # [FIX-AUDIT-D4]
            res.d200_label = f"D200: Price below (-{D200_SOFT_ADJ})"
        elif direction == "short" and ctx["d200_below"]:
            adjusted_score = _adj(adjusted_score, adjs, f"D200: Price below (+{D200_SOFT_ADJ})", D200_SOFT_ADJ)  # [FIX-AUDIT-D4]
            res.d200_label = f"D200: Price below (+{D200_SOFT_ADJ})"
        elif direction == "short" and ctx["d200_above"]:
            adjusted_score = _adj(adjusted_score, adjs, f"D200: Price above (-{D200_SOFT_ADJ})", -D200_SOFT_ADJ)  # [FIX-AUDIT-D4]
            res.d200_label = f"D200: Price above (-{D200_SOFT_ADJ})"
        else:
            res.d200_label = "D200: Neutral"
    else:
        res.d200_label = "D200: Disabled"

    _ref_ts_s   = (reference_ms / 1000) if reference_ms is not None else time.time()
    _now_utc    = datetime.fromtimestamp(_ref_ts_s, tz=timezone.utc)
    _in_dead = SESSION_DEAD_ZONE_START_UTC <= _now_utc.hour < SESSION_DEAD_ZONE_END_UTC
    _atr_vals = [v for v in ind["atr15"][-(48 + ATR_LEN):] if not math.isnan(v)]
    if _atr_vals:
        _atr_pct_thresh = sorted(_atr_vals)[int(len(_atr_vals) * SESSION_LOW_ATR_PERCENTILE)]
    else:
        _atr_pct_thresh = atr_val
    _low_atr        = atr_val <= _atr_pct_thresh
    session_penalty = _in_dead and _low_atr
    if session_penalty:
        adjusted_score = _adj(adjusted_score, adjs, f"Low-liquidity session ({_now_utc.hour:02d}:xx UTC, thin ATR)", -1)  # [FIX-AUDIT-D4]

    ema_f15 = ind["ema_f15"]
    if len(ema_f15) >= EMA_VELOCITY_LOOKBACK + 2:
        _ef_velocity = (safe(ema_f15[-1]) - safe(ema_f15[-(1 + EMA_VELOCITY_LOOKBACK)])) / atr_val
        if res.signal_type == "BREAK":
            if _ef_velocity > EMA_VELOCITY_STRONG_MIN and direction == "long":
                adjusted_score = _adj(adjusted_score, adjs, f"EMA accelerating up ({_ef_velocity:+.3f}×ATR)", 1)  # [FIX-AUDIT-D4]
            elif _ef_velocity < -EMA_VELOCITY_STRONG_MIN and direction == "short":
                adjusted_score = _adj(adjusted_score, adjs, f"EMA accelerating down ({_ef_velocity:+.3f}×ATR)", 1)  # [FIX-AUDIT-D4]
            elif abs(_ef_velocity) < EMA_VELOCITY_WEAK_MAX:
                adjusted_score = _adj(adjusted_score, adjs, f"EMA flattening ({_ef_velocity:+.3f}×ATR) on BREAK", -1)  # [FIX-AUDIT-D4]
        # Phase 4 (Fix E) — PULL EMA Velocity Check
        elif ENABLE_PULL_EMA_VELOCITY_CHECK and res.signal_type == "PULL":
            # [FIX-AUDIT-D8] only penalize actively declining EMA, not normal pullback dips
            if direction == "long" and _ef_velocity < -EMA_VELOCITY_WEAK_MAX:   # must be actively declining
                adjusted_score = _adj(adjusted_score, adjs, f"EMA velocity weakening on PULL long ({_ef_velocity:+.3f}×ATR)", -1)  # [FIX-AUDIT-D4]
            elif direction == "short" and _ef_velocity > EMA_VELOCITY_WEAK_MAX:  # must be actively rising
                adjusted_score = _adj(adjusted_score, adjs, f"EMA velocity weakening on PULL short ({_ef_velocity:+.3f}×ATR)", -1)  # [FIX-AUDIT-D4]

    rsi4h_arr = ind["rsi4h"]
    _r4h_raw  = rsi4h_arr[-1] if len(rsi4h_arr) >= 1 else float("nan")
    r4h_valid = not math.isnan(_r4h_raw)
    r4h       = _r4h_raw if r4h_valid else 50.0
    r15 = ctx["r15"]
    r1h = ctx["r1h"]

    # rsi_divergence already assigned at top of function [FIX-AUDIT-D1]
    if USE_1H_RSI_DIVERGENCE and rsi_divergence["type"] is not None:
        div_type = rsi_divergence["type"]
        if res.signal_type == "PULL":
            if direction == "short" and div_type == "bearish":
                adjusted_score = _adj(adjusted_score, adjs, "1H RSI bearish divergence (PULL short)", DIVERGENCE_BONUS)  # [FIX-AUDIT-D4]
            elif direction == "long" and div_type == "bullish":
                adjusted_score = _adj(adjusted_score, adjs, "1H RSI bullish divergence (PULL long)", DIVERGENCE_BONUS)  # [FIX-AUDIT-D4]
        elif res.alignment_mode == "exhaustion":
            if direction == "short" and div_type == "bearish":
                adjusted_score = _adj(adjusted_score, adjs, "1H RSI bearish divergence (exhaustion short)", DIVERGENCE_BONUS)  # [FIX-AUDIT-D4]
            elif direction == "long" and div_type == "bullish":
                adjusted_score = _adj(adjusted_score, adjs, "1H RSI bullish divergence (exhaustion long)", DIVERGENCE_BONUS)  # [FIX-AUDIT-D4]

    if USE_FALSE_BREAKOUT_DETECTION and res.signal_type == "PULL":
        _ref_ms_for_pattern = reference_ms if reference_ms is not None else int(time.time() * 1000)
        _bar_index_pattern = _ref_ms_for_pattern // (15 * 60 * 1000)
        is_false_breakout, fb_desc = check_false_breakout_pattern(state, symbol, direction, _bar_index_pattern)
        if is_false_breakout:
            adjusted_score = _adj(adjusted_score, adjs, fb_desc, FALSE_BREAKOUT_BONUS)  # [FIX-AUDIT-D4]

    if res.signal_type == "BREAK":
        rsi_15m_ok_long  = RSI_BREAK_LONG_MIN  <= r15 <= RSI_BREAK_LONG_MAX
        rsi_15m_ok_short = RSI_BREAK_SHORT_MIN <= r15 <= RSI_BREAK_SHORT_MAX
    else:
        rsi_15m_ok_long  = RSI_PULL_LONG_MIN   <= r15 <= RSI_PULL_LONG_MAX
        rsi_15m_ok_short = RSI_PULL_SHORT_MIN  <= r15 <= RSI_PULL_SHORT_MAX

    rsi_1h_ok_long   = r1h <= RSI_1H_PULL_LONG_MAX
    rsi_4h_ok_long   = r4h <= RSI_4H_PULL_LONG_MAX
    rsi_1h_ok_short  = r1h >= RSI_1H_PULL_SHORT_MIN
    rsi_4h_ok_short  = r4h >= RSI_4H_PULL_SHORT_MIN
    rsi_confluence_long  = rsi_1h_ok_long  and rsi_4h_ok_long
    rsi_confluence_short = rsi_1h_ok_short and rsi_4h_ok_short

    if direction == "long":
        if rsi_confluence_long:
            adjusted_score = _adj(adjusted_score, adjs, "RSI confluence 1H/4H", 1)  # [FIX-AUDIT-D4]
        elif r4h_valid and r4h > RSI_4H_PULL_LONG_MAX:
            adjusted_score = _adj(adjusted_score, adjs, f"4H RSI overbought ({r4h:.0f})", -1)  # [FIX-AUDIT-D4]

    if direction == "short":
        if rsi_confluence_short:
            adjusted_score = _adj(adjusted_score, adjs, "RSI confluence 1H/4H", 1)  # [FIX-AUDIT-D4]
        elif r4h_valid and r4h < RSI_4H_PULL_SHORT_MIN:
            adjusted_score = _adj(adjusted_score, adjs, f"4H RSI oversold ({r4h:.0f})", -1)  # [FIX-AUDIT-D4]

    if _atr_pctile is not None:
        if res.signal_type == "BREAK" and _atr_pctile < ATR_LOW_PERCENTILE:
            adjusted_score = _adj(adjusted_score, adjs, f"ATR low vs symbol history ({_atr_pctile*100:.0f}th pct)", -1)  # [FIX-AUDIT-D4]
        elif res.signal_type == "PULL" and _atr_pctile > ATR_HIGH_PERCENTILE:
            adjusted_score = _adj(adjusted_score, adjs, f"ATR high vs symbol history ({_atr_pctile*100:.0f}th pct)", -1)  # [FIX-AUDIT-D4]

    if res.signal_type == "PULL" and res.supports and direction == "long":
        nearest_sup = res.supports[0]
        if (res.entry - nearest_sup) < atr_val * SUPPORT_PROXIMITY_ATR:
            _rs_pct = rs_data.get("rs_pct")
            if _rs_pct is None or _rs_pct >= PROXIMITY_RS_MIN:
                adjusted_score = _adj(adjusted_score, adjs, "Support proximity", 1)  # [FIX-AUDIT-D4]
            else:
                adjs.append((f"Support proximity skipped (RS {_rs_pct:+.1f}% < {PROXIMITY_RS_MIN:.0f}%)", 0))

    if res.signal_type == "PULL" and res.resistances and direction == "short":
        nearest_res = res.resistances[0]
        if (nearest_res - res.entry) < atr_val * SUPPORT_PROXIMITY_ATR:
            _rs_pct = rs_data.get("rs_pct")
            if _rs_pct is None or _rs_pct >= PROXIMITY_RS_MIN:
                adjusted_score = _adj(adjusted_score, adjs, "Resistance proximity", 1)  # [FIX-AUDIT-D4]
            else:
                adjs.append((f"Resistance proximity skipped (RS {_rs_pct:+.1f}% < {PROXIMITY_RS_MIN:.0f}%)", 0))

    if funding_rate is not None:
        if USE_FUNDING_CARRY:
            rate = funding_rate
            tailwind = (rate < 0 and direction == "long") or (rate > 0 and direction == "short")
            if tailwind:
                if direction == "short" and rate >= FUNDING_CARRY_POSITIVE_THRESHOLD:
                    adjusted_score = _adj(adjusted_score, adjs, f"Funding tailwind carry on {res.signal_type} ({rate*100:+.4f}%/8h)", FUNDING_CARRY_BONUS)  # [FIX-AUDIT-D4]
                elif direction == "long" and rate <= FUNDING_CARRY_NEGATIVE_THRESHOLD:
                    adjusted_score = _adj(adjusted_score, adjs, f"Funding tailwind carry on {res.signal_type} ({rate*100:+.4f}%/8h)", FUNDING_CARRY_BONUS)  # [FIX-AUDIT-D4]

        if FUNDING_PULL_WARN_MIN is not None:
            rate = funding_rate
            headwind = (rate > 0 and direction == "long") or (rate < 0 and direction == "short")
            if headwind and FUNDING_PULL_WARN_MIN <= abs(rate) < FUNDING_SUPPRESS_EXTREME:
                adjusted_score = _adj(adjusted_score, adjs, f"Funding headwind on {res.signal_type} ({rate*100:+.4f}%/8h)", -1)  # [FIX-AUDIT-D4]

        rate = funding_rate
        headwind = (rate > 0 and direction == "long") or (rate < 0 and direction == "short")
        if headwind and get_funding_trend(state, symbol) == "rising":
            adjusted_score = _adj(adjusted_score, adjs, f"Funding headwind rising on {res.signal_type} ({rate*100:+.4f}%/8h ↑)", -1)  # [FIX-AUDIT-D4]

    _tp1_m, _tp2_m, _sl_m = get_regime_aware_multipliers(
        btc_regime, _atr_pctile, res.signal_type
    )
    if atr_pct > HIGH_ATR_THRESHOLD:
        _sl_m = SL_HIGH_ATR_MULT
    if _early_break_sl_widen:
        _sl_m *= 1.10   # [AUDIT-P5-1] widen SL for early-tier entry (short-lookback close-break, not full BREAK gates) — was mislabeled "intra-candle" (Phase 8 / Fix J)

    if res.fire_long:
        res.tp1 = cur_c + atr_val * _tp1_m
        res.tp2 = cur_c + atr_val * _tp2_m
        res.sl  = cur_c - atr_val * _sl_m
        if res.signal_type == "PULL":
            _limit_px  = safe(ind["ema_f15"][-1])
            _limit_dist = (cur_c - _limit_px) / atr_val if atr_val > 0 else 0.0
            if _limit_px > 0 and 0 < _limit_dist <= PULL_LIMIT_MAX_ATR_DIST:
                res.limit_entry      = _limit_px
                res.limit_entry_dist = _limit_dist
    elif res.fire_short:
        res.tp1 = cur_c - atr_val * _tp1_m
        res.tp2 = cur_c - atr_val * _tp2_m
        res.sl  = cur_c + atr_val * _sl_m
        if res.signal_type == "PULL":
            _limit_px  = safe(ind["ema_f15"][-1])
            _limit_dist = (_limit_px - cur_c) / atr_val if atr_val > 0 else 0.0
            if _limit_px > 0 and 0 < _limit_dist <= PULL_LIMIT_MAX_ATR_DIST:
                res.limit_entry      = _limit_px
                res.limit_entry_dist = _limit_dist

    if res.signal_type == "BREAK":
        if res.fire_long and res.resistances:
            sr_tp1  = res.resistances[0]
            atr_tp1 = res.entry + atr_val * _tp1_m
            if res.entry < sr_tp1 < atr_tp1:
                sr_dist = (sr_tp1 - res.entry) / atr_val
                if sr_dist >= SR_CLEARANCE_ATR_MULT:
                    res.tp1 = sr_tp1
        elif res.fire_short and res.supports:
            sr_tp1  = res.supports[0]
            atr_tp1 = res.entry - atr_val * _tp1_m
            if atr_tp1 < sr_tp1 < res.entry:
                sr_dist = (res.entry - sr_tp1) / atr_val
                if sr_dist >= SR_CLEARANCE_ATR_MULT:
                    res.tp1 = sr_tp1

    if res.signal_type == "PULL":
        if res.fire_long and res.resistances:
            sr_tp1  = res.resistances[0]
            atr_tp1 = res.entry + atr_val * _tp1_m
            sr_dist = (sr_tp1 - res.entry) / atr_val
            if sr_dist >= SR_CLEARANCE_ATR_MULT:
                res.tp1 = min(sr_tp1, atr_tp1)
        elif res.fire_short and res.supports:
            sr_tp1  = res.supports[0]
            atr_tp1 = res.entry - atr_val * _tp1_m
            sr_dist = (res.entry - sr_tp1) / atr_val
            if sr_dist >= SR_CLEARANCE_ATR_MULT:
                res.tp1 = max(sr_tp1, atr_tp1)

    _rsi_conf_awarded = (direction == "long" and rsi_confluence_long) or \
                        (direction == "short" and rsi_confluence_short)
    if res.signal_type == "PULL" and not _rsi_conf_awarded:
        if direction == "long" and r1h > RSI_1H_PULL_LONG_MAX:
            adjusted_score = _adj(adjusted_score, adjs, f"1H RSI extended ({r1h:.0f} > {RSI_1H_PULL_LONG_MAX:.0f})", -1)  # [FIX-AUDIT-D4]
        elif direction == "short" and r1h < RSI_1H_PULL_SHORT_MIN:
            adjusted_score = _adj(adjusted_score, adjs, f"1H RSI extended ({r1h:.0f} < {RSI_1H_PULL_SHORT_MIN:.0f})", -1)  # [FIX-AUDIT-D4]

    # ══════════════════════════════════════════════════════════════
    # LIQUIDITY & MARKET STRUCTURE CONFLUENCE LAYER
    # (non-trade-generating — only filters / boosts / reduces confidence)
    # ══════════════════════════════════════════════════════════════
    if _liq_engine is not None and ENABLE_LIQUIDITY_CONFLUENCE:
        try:
            _vol_ma_15m = safe(ind["vol_ma15"][-1]) if len(ind["vol_ma15"]) else None
            _htf_flags = {
                "1h_bull": ctx.get("h1_bull", False),
                "1h_bear": ctx.get("h1_bear", False),
                "4h_bull": ctx.get("h4_bull", False),
                "4h_bear": ctx.get("h4_bear", False),
            }
            # [Fix-46] Previously hardcoded to None — fetch_l2_imbalance()
            # was fully implemented and imported but never called, so
            # l2_imbalance_weight/l2_imbalance_threshold in LiquidityConfig
            # never contributed to any score. Gated on ENABLE_L2_IMBALANCE
            # (default off) since this adds a synchronous HL API call per
            # candidate signal; failures inside fetch_l2_imbalance already
            # degrade to None rather than raising, so this can't itself
            # break signal generation when the network call fails.
            _l2_imb = None
            if ENABLE_L2_IMBALANCE:
                _l2_imb = fetch_l2_imbalance(symbol, hl_post)

            _liq_out: ConfluenceOutput = _liq_engine.analyze(
                symbol=symbol,
                candles_15m=candles_15m,
                # [Fix-43] Previously always None — the liquidity engine's
                # candles_1h/candles_4h params were accepted but never fed,
                # so HTF liquidity structure was never actually analyzed
                # (only the EMA-cross htf_bull_flags boolean below was used
                # as a stand-in). Now threaded through from compute_signals()
                # via _apply_scoring_and_filters so the engine can run real
                # 1h/4h dealing-range / external-BSL-SSL / sweep analysis.
                candles_1h=candles_1h,
                candles_4h=candles_4h,
                direction=direction,
                base_strength=res.score,
                entry=res.entry,
                tp1=res.tp1,
                sl=res.sl,
                atr_val_15m=atr_val,
                vol_ma_15m=_vol_ma_15m,
                oi_trend=oi_data.get("oi_trend"),
                htf_bull_flags=_htf_flags,
                l2_imbalance=_l2_imb,
            )

            res.liquidity_bonus       = _liq_out.liquidity_bonus
            res.liquidity_score       = _liq_out.liquidity_score
            res.structure_score       = _liq_out.structure_score
            res.nearest_external_bsl  = _liq_out.nearest_external_bsl
            res.nearest_external_ssl  = _liq_out.nearest_external_ssl
            res.liquidity_sweep       = _liq_out.liquidity_sweep
            res.mss                   = _liq_out.mss
            res.bos                   = _liq_out.bos
            res.fvg                   = _liq_out.fvg
            res.premium_discount      = _liq_out.premium_discount
            res.htf_alignment         = _liq_out.htf_alignment
            res.liquidity_reasons     = _liq_out.reasons
            res.liquidity_hard_suppress = _liq_out.hard_suppress

            # [Fix-43] Real HTF liquidity structure — for logging/dashboard
            # visibility, same as the other liquidity_* fields above. Already
            # folded into adjusted_score via liquidity_bonus (see below); not
            # added again here.
            res.htf_liquidity_score   = _liq_out.htf_liquidity_score
            res.htf_1h_zone           = _liq_out.htf_1h_zone
            res.htf_4h_zone           = _liq_out.htf_4h_zone
            res.htf_1h_external_bsl   = _liq_out.htf_1h_external_bsl
            res.htf_1h_external_ssl   = _liq_out.htf_1h_external_ssl
            res.htf_4h_external_bsl   = _liq_out.htf_4h_external_bsl
            res.htf_4h_external_ssl   = _liq_out.htf_4h_external_ssl
            res.htf_1h_sweep          = _liq_out.htf_1h_sweep
            res.htf_4h_sweep          = _liq_out.htf_4h_sweep

            if _liq_out.hard_suppress:
                print(f"  [LIQ-SUPPRESS] {symbol} {direction.upper()} — "
                      f"counter-structure / false sweep. Reasons: "
                      f"{'; '.join(_liq_out.reasons)}")
                res.fire_long  = False
                res.fire_short = False
                return res

            # Route liquidity_bonus through adjs so the bonus-cap logic
            # accounts for it in one unified place.  Do NOT also add it
            # directly to adjusted_score here; the cap recompute path does
            # adjusted_score = res.score + sum(adjs), so adding it both
            # ways would double-count the bonus on capped signals.
            if _liq_out.liquidity_bonus != 0:
                # [PATCH-1] Cap the liquidity bonus when HTF structure is not
                # aligned with trade direction. The discount-zone/BOS/FVG stack
                # can reach +10, overwhelming all environmental penalties in a
                # counter-trend regime. When HTF aligned = False, cap at +4 so
                # structural confluence still contributes without masking weak
                # base quality. Source: cross-arbitrated from three audits.
                _LIQ_HTF_MISALIGN_CAP = 4
                _effective_liq_bonus = (
                    min(_liq_out.liquidity_bonus, _LIQ_HTF_MISALIGN_CAP)
                    if not _liq_out.htf_alignment
                    else _liq_out.liquidity_bonus
                )
                if _effective_liq_bonus != _liq_out.liquidity_bonus:
                    adjs.append((
                        f"[LIQ] liquidity_bonus capped (HTF not aligned: "
                        f"{_liq_out.liquidity_bonus} → {_effective_liq_bonus})",
                        _effective_liq_bonus
                    ))
                else:
                    adjs.append(("[LIQ] liquidity_bonus", _effective_liq_bonus))
                adjusted_score += _effective_liq_bonus

            # Per-reason strings are for display/logging only (adj=0 so
            # they do not contribute to score totals or cap accounting).
            for reason in _liq_out.reasons:
                adjs.append((f"[LIQ] {reason}", 0))

            # [Fix-24] Reconcile the two independent S/R systems — find_sr_levels()
            # (main bot, its own clustering tolerance) and the liquidity engine's
            # external BSL/SSL (different clustering tolerance) are computed
            # independently and never cross-checked. Log (and surface via
            # res.sr_liquidity_disagreement) when they disagree materially on where
            # the nearest level is. Logging/visibility only in this pass — does NOT
            # change scoring or filtering behavior.
            _sr_cmp_level = res.resistances[0] if (direction == "long" and res.resistances) else \
                            (res.supports[0] if (direction == "short" and res.supports) else None)
            _liq_cmp_level = res.nearest_external_bsl if direction == "long" else res.nearest_external_ssl
            if _sr_cmp_level is not None and _liq_cmp_level is not None and atr_val > 0:
                _sr_liq_dist_atr = abs(_sr_cmp_level - _liq_cmp_level) / atr_val
                if _sr_liq_dist_atr > SR_LIQUIDITY_DISAGREEMENT_ATR_MULT:
                    res.sr_liquidity_disagreement = (
                        f"find_sr_levels={_sr_cmp_level:.4f} vs liquidity_engine={_liq_cmp_level:.4f} "
                        f"({_sr_liq_dist_atr:.1f} ATR apart)"
                    )
                    print(f"  [SR-LIQ DISAGREEMENT] {symbol} {direction.upper()} — "
                          f"{res.sr_liquidity_disagreement}")
                    # [Fix-42] Severe disagreement (the two systems don't even
                    # agree which side of price the nearest level is on) now
                    # costs real score instead of being log-only, routed through
                    # the standard adjs/penalty-cap machinery like every other
                    # penalty.
                    if _sr_liq_dist_atr > SR_LIQUIDITY_DISAGREEMENT_SEVERE_ATR_MULT:
                        adjs.append((
                            f"S/R vs liquidity engine severe disagreement "
                            f"({_sr_liq_dist_atr:.1f} ATR apart)",
                            SR_LIQUIDITY_DISAGREEMENT_PENALTY
                        ))
                        adjusted_score += SR_LIQUIDITY_DISAGREEMENT_PENALTY

        except Exception as _liq_err:
            print(f"  [LIQ-ERROR] {symbol} {direction.upper()}: {_liq_err}")

    # ══════════════════════════════════════════════════════════════
    # [Fix-9] PENALTY STACKING CAP
    # ══════════════════════════════════════════════════════════════
    total_neg = sum(adj for _, adj in adjs if adj < 0)
    if total_neg < -MAX_NEGATIVE_ADJUSTMENTS:
        _HANDPICKED_PENALTY_PRIORITY = [
            "BTC Regime",
            "OI",
            "Exhaustion",
            "RS negative",
        ]
        # [Fix-23] Toggleable: data-driven ordering where a real win-rate bucket
        # exists, hand-picked fallback otherwise. See _get_data_driven_penalty_priority.
        PENALTY_PRIORITY = (
            _get_data_driven_penalty_priority(state, _HANDPICKED_PENALTY_PRIORITY)
            if DATA_DRIVEN_PENALTY_PRIORITY else _HANDPICKED_PENALTY_PRIORITY
        )

        cap = -MAX_NEGATIVE_ADJUSTMENTS
        current_sum = 0
        kept_indices: set[int] = set()

        for i, (lbl, adj) in enumerate(adjs):
            if adj < 0 and any(p in lbl for p in PENALTY_PRIORITY):
                if current_sum + adj >= cap:
                    kept_indices.add(i)
                    current_sum += adj

        # [FIX-AUDIT-D15] Sort non-priority penalties by magnitude so largest penalties fill cap first
        non_priority_penalties = sorted(
            [(i, lbl, adj) for i, (lbl, adj) in enumerate(adjs)
             if adj < 0 and i not in kept_indices],
            key=lambda x: x[2]   # most negative first
        )
        for i, lbl, adj in non_priority_penalties:
            if current_sum + adj >= cap:
                kept_indices.add(i)
                current_sum += adj

        trimmed_count = sum(1 for i, (_, adj) in enumerate(adjs)
                           if adj < 0 and i not in kept_indices)
        if trimmed_count > 0:
            new_adjs = [(lbl, adj) for i, (lbl, adj) in enumerate(adjs)
                        if adj >= 0 or i in kept_indices]
            adjs[:] = new_adjs
            adjusted_score = res.score + sum(adj for _, adj in adjs)
            print(f"  [PENALTY CAP] {symbol} {direction.upper()} — "
                  f"trimmed {trimmed_count} secondary penalty(ies) to stay within "
                  f"-{MAX_NEGATIVE_ADJUSTMENTS} cap. Final neg total: {current_sum}")

    # ══════════════════════════════════════════════════════════════
    # [Fix-10] BONUS STACKING CAP (mirrors [Fix-9] penalty cap)
    # ══════════════════════════════════════════════════════════════
    total_pos = sum(adj for _, adj in adjs if adj > 0)
    if total_pos > MAX_POSITIVE_ADJUSTMENTS:
        cap = MAX_POSITIVE_ADJUSTMENTS
        current_sum = 0
        kept_indices: set[int] = set()

        # Keep the highest-value bonuses first.  Sort descending by
        # magnitude so that the single most-important bonus (e.g. a
        # combined liquidity_bonus) is always included before smaller
        # incremental ones.  Ties are broken by insertion order (stable
        # sort), which respects the semantic priority already baked into
        # adjs ordering (OI, RS, structural signals come earlier).
        pos_order = sorted(
            [i for i, (_, adj) in enumerate(adjs) if adj > 0],
            key=lambda i: adjs[i][1], reverse=True,
        )
        for i in pos_order:
            adj = adjs[i][1]
            if current_sum + adj <= cap:
                kept_indices.add(i)
                current_sum += adj
            # If the largest remaining bonus alone would exceed the cap
            # (e.g. a +15 liquidity_bonus against a cap of 20 with
            # current_sum already at 8), still include it but stop
            # adding further items to avoid exceeding the cap.
            elif not kept_indices:
                kept_indices.add(i)
                current_sum += adj
                break

        trimmed_count = sum(1 for i, (_, adj) in enumerate(adjs)
                           if adj > 0 and i not in kept_indices)
        if trimmed_count > 0:
            new_adjs = [(lbl, adj) for i, (lbl, adj) in enumerate(adjs)
                        if adj <= 0 or i in kept_indices]
            adjs[:] = new_adjs
            adjusted_score = res.score + sum(adj for _, adj in adjs)
            print(f"  [BONUS CAP] {symbol} {direction.upper()} — "
                  f"trimmed {trimmed_count} secondary bonus(es) to stay within "
                  f"+{MAX_POSITIVE_ADJUSTMENTS} cap. Final pos total: {current_sum}")

    # [Fix-15] Hard backstop: the `elif not kept_indices:` branch above can let a
    # single oversized bonus push current_sum above `cap` with no subsequent clamp,
    # since it intentionally keeps that one bonus even when it alone exceeds the cap.
    # This makes the +MAX_POSITIVE_ADJUSTMENTS ceiling structurally guaranteed rather
    # than dependent on every individual contributor staying under it by convention.
    # Runs unconditionally, whether or not the capping block above was entered.
    adjusted_score = min(adjusted_score, res.score + MAX_POSITIVE_ADJUSTMENTS)

    res.final_score = adjusted_score
    res.atr_pct     = atr_pct
    res.vol_ratio   = vol_ratio

    vol_score_ok_pull = ctx.get("vol_score_ok_pull", True)
    bb_s  = "BB✓"  if (ctx["bb_long"]  if res.fire_long else ctx["bb_short"])  else "BB✗"
    d_s   = "4H✓"  if (ctx["h4_bull"] and ctx["h4_trend_held_bull"] if res.fire_long
                       else ctx["h4_bear"] and ctx["h4_trend_held_bear"]) else "4H✗"
    adx_s = "ADX✓" if ctx["adx_score_ok"]  else "ADX✗"
    obv_s = "OBV✓" if (ctx["obv_slope_long"] if res.fire_long else ctx["obv_slope_short"]) else "OBV✗"
    vol_s = "VOL✓" if vol_score_ok_pull else "VOL✗"
    if res.signal_type == "BREAK":
        accel_s = "VA✓" if vol_accel_ok else "VA✗"
        res.breakdown = f"{bb_s} {d_s} {adx_s} {obv_s} {vol_s} {oi_data['breakdown_tag']} {accel_s}"
    else:
        res.breakdown = f"{bb_s} {d_s} {adx_s} {obv_s} {vol_s} {oi_data['breakdown_tag']}"

    h1 = ctx["h1_bull"] if res.fire_long else ctx["h1_bear"]
    if res.fire_long and ctx["exhaustion_long_align"] and not ctx["full_long_align"]:
        h4_tag = "4H:⚠"
    elif not res.fire_long and ctx["exhaustion_short_align"] and not ctx["full_short_align"]:
        h4_tag = "4H:⚠"
    elif res.fire_long and ctx["pullback_long_align"] and not ctx["full_long_align"]:
        h4_tag = "4H:✓"
    elif not res.fire_long and ctx["pullback_short_align"] and not ctx["full_short_align"]:
        h4_tag = "4H:✓"
    else:
        h4_tag = f"4H:{'✓' if (ctx['full_long_align'] if res.fire_long else ctx['full_short_align']) else '✗'}"

    if res.alignment_mode == "pullback":
        h1_tag = "1H:↩ (pullback)"
    else:
        h1_tag = f"1H:{'✓' if h1 else '✗'}"
    res.v10_gates = (f"DailyADX:{'✓' if ctx['daily_adx_ok'] else '✗'} {h4_tag} {h1_tag}")

    _effective_min = get_effective_min_score(btc_regime, breadth_pct)
    if adjusted_score < _effective_min:
        print(f"  [SCORE FILTER] {symbol} {direction.upper()} suppressed: "
              f"base={res.score} → final={adjusted_score} < {_effective_min}"
              f"{' (regime-adjusted)' if _effective_min > MIN_SCORE else ''}")
        res.fire_long  = False
        res.fire_short = False
        return res

    _tp1_dist = abs(res.tp1 - res.entry)
    _sl_dist  = abs(res.sl  - res.entry)
    if _sl_dist > 0 and (_tp1_dist / _sl_dist) < MIN_RR_RATIO:
        print(f"  [RR FILTER] {symbol} {direction.upper()} suppressed — "
              f"R:R {_tp1_dist/_sl_dist:.2f} < {MIN_RR_RATIO} "
              f"(TP1={res.tp1:.4f} entry={res.entry:.4f} SL={res.sl:.4f})")
        res.fire_long  = False
        res.fire_short = False
        return res

    # Cross-cutting: SR clearance fix (Medium/M2)
    if ENABLE_SR_CLEARANCE_FIX and SR_CLEARANCE_ATR_MULT > 0 and (res.fire_long or res.fire_short):
        min_clearance = atr_val * SR_CLEARANCE_ATR_MULT
        if res.fire_long and res.resistances:
            nearest_res = res.resistances[0]
            # Only block when resistance is ABOVE entry (not yet cleared)
            if nearest_res > res.entry and nearest_res - res.entry < min_clearance:
                print(f"  [SR FILTER] {symbol} LONG suppressed — resistance {nearest_res:.4f} "
                      f"too close above entry {res.entry:.4f}")
                res.fire_long = False
        if res.fire_short and res.supports:
            nearest_sup = res.supports[0]
            # Only block when support is BELOW entry (not yet cleared)
            if nearest_sup < res.entry and res.entry - nearest_sup < min_clearance:
                print(f"  [SR FILTER] {symbol} SHORT suppressed — support {nearest_sup:.4f} "
                      f"too close below entry {res.entry:.4f}")
                res.fire_short = False

    # Cross-cutting: continuation_probability (audit §11)
    # [FIX-AUDIT-D18] Early return if SR clearance suppressed both directions — avoid running dead code
    if not res.fire_long and not res.fire_short:
        if hasattr(res, "_ind"): del res._ind
        if hasattr(res, "_ctx"): del res._ctx
        return res

    cont_prob = continuation_probability(res, ind, ctx, oi_data, btc_regime)
    res.continuation_prob_score = cont_prob
    # Optional suppression gate (default False per audit recommendation)
    if ENABLE_CONTINUATION_PROB_GATE and cont_prob < 4:
        print(f"  [CONTINUATION PROB] {symbol} suppressed — score {cont_prob} < 4")
        res.fire_long = False
        res.fire_short = False

    if hasattr(res, '_ind'):
        del res._ind
    if hasattr(res, '_ctx'):
        del res._ctx

    return res

# Cross-cutting: continuation_probability helper (audit §11)
def continuation_probability(res: SignalResult, ind: dict, ctx: dict,
                            oi_data: dict, btc_regime: dict | None) -> int:
    """
    Compute continuation probability score (0-10 scale, baseline 5).
    Factors: RSI slope, volume ratio, EMA velocity, HTF alignment, OI confirmation, BTC regime tailwind.
    """
    score = 5  # baseline

    # RSI momentum via slope
    rsi15 = ind["rsi15"]
    if len(rsi15) > 4:
        rsi_slope = rsi15[-1] - rsi15[-4]
        if rsi_slope > 5:
            score += 1
        elif rsi_slope < -5:
            score -= 1

    # Volume via vol_ratio
    vol_ratio = ctx.get("vol_ratio")
    if vol_ratio:
        if vol_ratio > 1.5:
            score += 1
        elif vol_ratio < 0.5:
            score -= 1

    # EMA velocity
    atr_val = ctx.get("atr_val", 1.0)
    ema_f15 = ind["ema_f15"]
    if len(ema_f15) > 5:
        ef_velocity = (safe(ema_f15[-1]) - safe(ema_f15[-5])) / atr_val
        if ef_velocity > 0.05:
            score += 1
        elif ef_velocity < -0.05:
            score -= 1

    # HTF alignment
    if ctx.get("full_long_align") or ctx.get("full_short_align"):
        score += 1
    elif ctx.get("exhaustion_long_align") or ctx.get("exhaustion_short_align"):
        score -= 0.5

    # OI confirmation
    if oi_data.get("oi_trend") == "rising":
        score += 1
    elif oi_data.get("oi_trend") == "falling":
        score -= 1

    # BTC regime tailwind
    if btc_regime:
        if btc_regime.get("bullish"):
            score += 1
        elif btc_regime.get("bearish"):
            score -= 1

    # Clamp to 0-10 range
    return max(0, min(10, int(score)))

def compute_signals(symbol, candles_15m, candles_1h, candles_4h, candles_d,
                    state: dict, record_market_inputs: bool = True,
                    reference_ms: int | None = None,
                    funding_rate: float | None = None) -> SignalResult:
    res = SignalResult()
    res.symbol = symbol

    if not candles_15m or len(candles_15m) < 50:
        return res

    # [Fix-ZEC-3] Guard against thin 1H/4H history that would leave RSI/EMA arrays
    # too short to produce a valid last value, causing safe() to return None and
    # crashing any subsequent >= comparison.  14 bars is the minimum RSI lookback;
    # 10 bars is the minimum for a meaningful 4H EMA.  Symbols that fail this check
    # are skipped cleanly rather than crashing the whole scan worker.
    if not candles_1h or len(candles_1h) < 14:
        return res
    if not candles_4h or len(candles_4h) < 10:
        return res

    ind = _compute_signal_indicators(symbol, candles_15m, candles_1h, candles_4h, candles_d)

    if record_market_inputs:
        record_market_inputs_from_candles(symbol, candles_15m, candles_4h)

    res = _detect_raw_signals(ind, state, reference_ms, funding_rate, symbol)
    res.symbol = symbol

    if not (res.fire_long or res.fire_short):
        return res

    res = _apply_scoring_and_filters(res, state, symbol, reference_ms,
                                      funding_rate, candles_15m,
                                      candles_1h, candles_4h)

    return res

# ═══════════════════════════════════════════════════════════════
# COOLDOWN STATE
# ═══════════════════════════════════════════════════════════════

# [AUDIT-P7-2] Schema validation beyond the existing version-number check. Real state key
# names used here (not the generic placeholder names from the audit prompt) — note the
# cooldowns key is "signal_cooldowns", not "cooldowns"; there's no separate "cooldowns" key
# in this schema. These five are the keys most load-bearing for correct operation: losing
# any of them silently (e.g. a truncated/corrupted write that still parses as valid JSON and
# still carries the right _version) would make active-signal tracking, history-based win
# rates, OI trend, or ATR-percentile logic silently operate on missing data rather than
# failing loudly. The .setdefault() calls below this in load_state() would mask that by
# quietly inventing empty containers — this check catches it before that happens and falls
# back to a fresh state instead.
REQUIRED_STATE_KEYS = {"active_signals", "signal_history", "oi_history", "atr_history", "signal_cooldowns"}

def _validate_state(s: dict) -> bool:
    return all(k in s for k in REQUIRED_STATE_KEYS)

def load_state() -> dict:
    _fresh = {
        "_version": STATE_VERSION,
        "oi_history": {}, "signal_history": [], "macro_calendar_cache": {},
        "post_loss_cooldown": {}, "atr_history": {}, "funding_history": {},
        "signal_cooldowns": {}, "last_signal_outcome": {},
        "failed_breakouts": {}, "active_signals": [], "resolved_signals": [],
    }
    for path in (STATE_FILE, STATE_FILE + ".bak"):
        if Path(path).exists():
            try:
                s = json.loads(Path(path).read_text())
                if s.get("_version", 1) != STATE_VERSION:
                    print(f"[STATE] Schema version mismatch in {path}. Starting fresh.")
                    continue
                # [AUDIT-P7-2] Validate required keys are present before trusting this file —
                # checked after the version check (so a deliberate version bump still takes
                # the "starting fresh" path above) but before .setdefault() below would mask
                # missing keys with quietly-invented empty containers.
                if not _validate_state(s):
                    print(f"[STATE] Schema invalid in {path} — missing required keys "
                          f"({REQUIRED_STATE_KEYS - set(s.keys())}). Starting fresh.")
                    continue
                s.setdefault("oi_history", {})
                s.setdefault("signal_history", [])
                s.setdefault("macro_calendar_cache", {})
                s.setdefault("post_loss_cooldown", {})
                s.setdefault("atr_history", {})
                s.setdefault("funding_history", {})
                s.setdefault("signal_cooldowns", {})
                s.setdefault("last_signal_outcome", {})
                s.setdefault("failed_breakouts", {})
                s.setdefault("active_signals", [])
                s.setdefault("resolved_signals", [])
                if path != STATE_FILE:
                    print(f"[STATE] Loaded from backup {path}")
                return s
            except Exception as e:
                print(f"[STATE] Failed to load {path}: {e}")
    print("[STATE] Starting fresh — no valid state file found")
    return _fresh

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

def check_cooldown(state, coin, direction, bar_index, signal_type: str = "",
                   candidate_score: int = 0) -> bool:
    symbol = coin if coin.endswith("USDT") else coin + "USDT"

    # [AUDIT-P7-8] active_count and the same-symbol-direction scan now run INSIDE the same
    # _state_lock acquisition that produces `active`, instead of snapshotting active under
    # the lock and then checking it after release. check_cooldown runs concurrently across
    # SCAN_WORKERS threads in main()'s Phase 2 pool — without this, two threads could both
    # read active_count just under MAX_CONCURRENT_ACTIVE_SIGNALS, both pass the check, and
    # both then have track_signal() append a new active signal, overshooting the cap (a
    # classic TOCTOU race: Time-Of-Check vs Time-Of-Use on `active`/active_count diverging
    # because of the lock release in between).
    with _state_lock:
        active = list(state.get("active_signals", []))
        _cd_key    = f"{symbol}_{direction}_{signal_type}" if signal_type in ("BREAK", "PULL") else f"{symbol}_{direction}"
        last_bar   = state.get("signal_cooldowns",  {}).get(_cd_key)
        last_sl_ts = state.get("post_loss_cooldown", {}).get(f"{symbol}_{direction}")
        prev_outcome = state.get("last_signal_outcome", {}).get(f"{symbol}_{direction}", "unknown")

        active_count = sum(1 for s in active if not s.get("resolved", False))
        if active_count >= MAX_CONCURRENT_ACTIVE_SIGNALS:
            print(f"  [MAX CONCURRENT] {hl_coin(symbol)} {direction.upper()} blocked — "
                  f"{active_count} active signals (max {MAX_CONCURRENT_ACTIVE_SIGNALS})")
            return False

        for sig in active:
            if sig.get("symbol") == symbol and not sig.get("resolved", False):
                if sig.get("direction") == direction:
                    return False

    if last_bar is not None:
        bars_elapsed = bar_index - last_bar
        if prev_outcome in ("tp1", "tp2"):
            required_bars = SIGNAL_COOLDOWN_POST_WIN_BARS
        elif candidate_score >= SIGNAL_HIGHSCORE_THRESHOLD:
            required_bars = SIGNAL_COOLDOWN_BARS_HIGHSCORE
        else:
            required_bars = SIGNAL_COOLDOWN_BARS

        if bars_elapsed < required_bars:
            remaining_bars = required_bars - bars_elapsed
            print(f"  [BAR COOLDOWN] {hl_coin(symbol)} {direction.upper()} "
                  f"{signal_type} (score={candidate_score}) blocked — "
                  f"{remaining_bars} bar(s) remaining (need {required_bars})")
            return False

    if last_sl_ts is not None:
        elapsed = int(time.time()) - last_sl_ts
        if elapsed < PULL_REENTRY_COOLDOWN_S:
            remaining = PULL_REENTRY_COOLDOWN_S - elapsed
            print(f"  [POST-LOSS COOLDOWN] {hl_coin(symbol)} {direction.upper()} "
                  f"{signal_type} blocked — {remaining}s remaining after SL")
            return False

    return True

def update_cooldown(state, coin, direction, bar_index, signal_type: str = ""):
    symbol = coin if coin.endswith("USDT") else coin + "USDT"
    cooldown_key = (f"{symbol}_{direction}_{signal_type}"
                    if signal_type in ("BREAK", "PULL")
                    else f"{symbol}_{direction}")
    with _state_lock:
        state.setdefault("signal_cooldowns", {})[cooldown_key] = bar_index

# ═══════════════════════════════════════════════════════════════
# RISK MANAGEMENT
# ═══════════════════════════════════════════════════════════════

# ═══════════════════════════════════════════════════════════════
# TELEGRAM
# ═══════════════════════════════════════════════════════════════

def stars(score: int) -> str:
    capped = max(0, min(score, 8))
    return "★" * capped + "☆" * (8 - capped)

def _sanitize_error(e: Exception) -> str:
    msg = str(e)
    if "bot" in msg and "/" in msg:
        import re
        msg = re.sub(r'https?://[^\s]+', '[URL]', msg)
    return f"{e.__class__.__name__}: {msg[:200]}"

def send_telegram(text: str) -> int | None:
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    for attempt in range(3):
        try:
            r = requests.post(url, json={
                "chat_id":    TG_CHAT_ID,
                "text":       text,
                "parse_mode": "HTML",
            }, timeout=10)
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
        r = requests.post(url, json={
            "chat_id":    TG_CHAT_ID,
            "message_id": message_id,
            "reaction":   [{"type": "emoji", "emoji": emoji}],
            "is_big":     False,
        }, timeout=10)
        r.raise_for_status()
        print(f"  [REACT] {emoji} → msg_id {message_id}")
    except Exception as e:
        print(f"  [REACT ERROR] msg_id {message_id}: {_sanitize_error(e)}")

RANK_MEDALS = {1: "🥇", 2: "🥈", 3: "🥉"}

def priority_score(sig: SignalResult) -> tuple:
    direction = "long" if sig.fire_long else "short"
    rate      = sig.funding_rate
    tailwind  = False if rate is None else (
        (rate < 0 and direction == "long") or (rate > 0 and direction == "short")
    )
    sig_type_wr = sig.win_rate_data.get("win_rate") or 0.5
    oi_confirm  = sig.oi_trend_data.get("score_adj", 0) > 0
    liq_struct = getattr(sig, 'structure_score', 0)
    liq_bonus  = getattr(sig, 'liquidity_bonus', 0)
    return (
        sig.final_score,
        round(sig_type_wr, 2),
        int(tailwind),
        int(oi_confirm),
        liq_struct,
        liq_bonus,
        sig.symbol,
    )

def format_signal(symbol: str, sig: SignalResult, engine_tag: str = "V5", rank: int = 0) -> str:
    direction = "▲ LONG" if sig.fire_long else "▼ SHORT"
    dir_str   = "long" if sig.fire_long else "short"
    emoji     = "🟢" if sig.fire_long else "🔴"
    ts        = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    def fmt(v):
        if v >= 1000: return f"{v:,.2f}"
        if v >= 1:    return f"{v:.4f}"
        return f"{v:.6f}"

    pull_entry_block = ""
    if sig.signal_type == "PULL" and sig.limit_entry is not None:
        pct_tgt = int(PULL_LIMIT_TRANCHE_PCT * 100)
        pct_now = 100 - pct_tgt
        pull_entry_block = (
            f"\n<b>📌 Split Entry Plan (PULL)</b>\n"
            f"  Tranche A ({pct_now}%):  Market @ <code>{fmt(sig.entry)}</code>\n"
            f"  Tranche B ({pct_tgt}%):  Limit  @ <code>{fmt(sig.limit_entry)}</code>  "
            f"(EMA21, {sig.limit_entry_dist:.2f}× ATR below)\n"
        )

    spread_line = ""
    if sig.spread_pct is not None:
        spread_tag = ("⚠️ elevated" if sig.spread_pct >= SPREAD_WARN_PCT else "✅ tight")
        spread_line = f"\nSpread/Liquidity: {sig.spread_pct:.3f}%  {spread_tag}"

    def _leverage_for_risk_pct(atr_pct: float, score: int, signal_type: str,
                               account_risk_pct: float) -> float:
        sl_atr_mult = SL_HIGH_ATR_MULT if atr_pct > HIGH_ATR_THRESHOLD else (
            SL_MULT_BREAK if signal_type == "BREAK" else SL_MULT_PULL
        )
        # [Fix-39] Dropped the stray /100.0 here. account_risk_pct (e.g. 2.0,
        # meaning "2%") and atr_pct (e.g. 1.5, meaning "1.5%") are both already
        # in the same "percent-number" convention used elsewhere in this file
        # (see HIGH_ATR_THRESHOLD comparisons above). Dividing sl_pct down into
        # a 0-1 fraction while leaving account_risk_pct un-scaled made the ratio
        # ~100x too large, so leverage saturated at LEVERAGE_MAX for virtually
        # any nonzero ATR% instead of producing a real 1x-10x gradient.
        sl_pct = atr_pct * sl_atr_mult
        if sl_pct <= 0:
            return 1.0
        max_lev = account_risk_pct / sl_pct
        lev = min(max(1.0, max_lev), LEVERAGE_MAX)
        if score <= 4:
            lev = max(1.0, lev * 0.8)
        return lev

    def recommended_leverage(atr_pct: float, score: int, signal_type: str) -> str:
        lev = _leverage_for_risk_pct(atr_pct, score, signal_type, LEVERAGE_BASE_RISK_PCT)
        return f"{lev:.1f}x"

    def recommended_leverage_range(atr_pct: float, score: int, signal_type: str) -> str:
        lo = _leverage_for_risk_pct(atr_pct, score, signal_type, LEVERAGE_RANGE_LOW_RISK_PCT)
        hi = _leverage_for_risk_pct(atr_pct, score, signal_type, LEVERAGE_RANGE_HIGH_RISK_PCT)
        lo_i = max(1, min(int(round(lo)), int(LEVERAGE_MAX)))
        hi_i = max(1, min(int(round(hi)), int(LEVERAGE_MAX)))
        if lo_i > hi_i:
            lo_i, hi_i = hi_i, lo_i
        if lo_i == hi_i:
            return f"{lo_i}x"
        return f"{lo_i}x\u2013{hi_i}x"

    lev_range    = recommended_leverage(sig.atr_pct, sig.final_score, sig.signal_type)
    lev_band     = recommended_leverage_range(sig.atr_pct, sig.final_score, sig.signal_type)
    funding_str  = format_funding(sig.funding_rate, dir_str)
    rate         = sig.funding_rate
    headwind     = rate is not None and (
        (rate > 0 and dir_str == "long") or (rate < 0 and dir_str == "short")
    )
    chk_funding  = "⚠️" if (headwind and rate is not None and abs(rate) >= FUNDING_SUPPRESS_EXTREME) else "✅"

    sr_lines = ""
    if sig.resistances:
        sr_lines += "🔴 Resistance: " + "  |  ".join(f"<code>{fmt(r)}</code>" for r in sig.resistances) + "\n"
    if sig.supports:
        sr_lines += "🟢 Support:    " + "  |  ".join(f"<code>{fmt(s)}</code>" for s in sig.supports) + "\n"

    oi_data      = sig.oi_trend_data
    oi_line      = oi_data.get("label", "OI Trend: Unknown")
    btc_line     = sig.btc_regime_label or "BTC Regime: Unknown"
    breadth_line = sig.breadth_label    or "Market Breadth: Unknown"
    rs_line      = sig.rs_data.get("label", "Relative Strength: N/A")
    wr_line      = sig.win_rate_data.get("label", "Win Rate: N/A")
    macro_line   = sig.macro_data.get("label", "Macro Risk: None") if sig.macro_data else "Macro Risk: None"
    d200_line    = sig.d200_label or "D200: Unknown"
    vol_ratio_line = (f"Volume Ratio: {sig.vol_ratio:.1f}x" if sig.vol_ratio is not None
                      else "Volume Ratio: N/A")

    score_trail = ""  # Adjustments block removed from Telegram output

    # ── Liquidity confluence block ──
    liq_block = ""
    if sig.liquidity_bonus != 0 or sig.liquidity_reasons or sig.sr_liquidity_disagreement:
        liq_lines = ["<b>💧 Liquidity & Structure</b>"]
        if sig.liquidity_bonus != 0:
            sign = "+" if sig.liquidity_bonus > 0 else ""
            liq_lines.append(f"  Confluence bonus: <b>{sign}{sig.liquidity_bonus}</b> "
                             f"(score={sig.liquidity_score}, struct={sig.structure_score})")
        if sig.nearest_external_bsl is not None:
            liq_lines.append(f"  Nearest ext. BSL: <code>{fmt(sig.nearest_external_bsl)}</code>")
        if sig.nearest_external_ssl is not None:
            liq_lines.append(f"  Nearest ext. SSL: <code>{fmt(sig.nearest_external_ssl)}</code>")
        if sig.sr_liquidity_disagreement:
            # [Fix-24] Visibility only — does not affect scoring/filtering.
            liq_lines.append(f"  ⚠️ S/R vs liquidity engine disagree: {sig.sr_liquidity_disagreement}")
        sw = sig.liquidity_sweep
        if sw.get("sweep_detected"):
            liq_lines.append(
                f"  Sweep: {sw.get('sweep_type','?')}  vol_conf={sw.get('volume_confirmed')}  "
                f"age={sw.get('age_bars',0)}b  false={sw.get('is_false_sweep')}"
            )
        if sig.mss.get("mss_detected"):
            liq_lines.append(f"  MSS: {sig.mss.get('direction')}")
        if sig.bos.get("bos_detected"):
            liq_lines.append(f"  BOS: {sig.bos.get('direction')}")
        if sig.fvg.get("fvg_detected"):
            liq_lines.append(f"  FVG: {sig.fvg.get('direction')}")
        liq_lines.append(f"  Zone: {sig.premium_discount}  HTF aligned: {sig.htf_alignment}")
        for r in sig.liquidity_reasons:
            safe_r = r.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            liq_lines.append(f"  • {safe_r}")
        liq_block = "\n" + "\n".join(liq_lines) + "\n"

    sr_block     = f"\n{sr_lines}" if sr_lines else ""
    medal        = RANK_MEDALS.get(rank, "")
    rank_tag     = f"{medal} <b>Priority #{rank}</b>\n" if rank else ""
    counter_tag  = "⚠️ " if "counter-trend" in (sig.btc_regime_label or "") else ""
    liq_conf_str = "ON" if ENABLE_LIQUIDITY_CONFLUENCE else "OFF"

    return (
        f"{rank_tag}{counter_tag}{emoji} <b>{direction} [{sig.signal_type}]</b>  {stars(sig.final_score)}\n"
        f"<b>Pair:</b> {symbol}\n\n"
        f"<b>Entry:</b> <code>{fmt(sig.entry)}</code>\n"
        f"<b>Entry Zone:</b> <code>{fmt(sig.entry_low)}</code> – <code>{fmt(sig.entry_high)}</code>\n"
        f"{pull_entry_block}\n"
        f"<b>TP1:</b> <code>{fmt(sig.tp1)}</code>\n"
        f"<b>TP2:</b> <code>{fmt(sig.tp2)}</code>\n"
        f"<b>SL:</b>  <code>{fmt(sig.sl)}</code>\n\n"
        f"{sr_lines}\n"
        f"<b>Leverage:</b> {lev_band}\n"
        f"<b>Liquidity Confluence:</b> {liq_conf_str}\n\n"
        f"<i>v{__version__} • {ts}</i>"
    )

# ═══════════════════════════════════════════════════════════════
# ACTIVE SIGNAL TRACKING
# ═══════════════════════════════════════════════════════════════

def track_signal(state: dict, symbol: str, direction: str,
                 msg_id: int, sig: SignalResult, bar_index: int,
                 hist_id: str | None = None):
    with _state_lock:
        active = state.setdefault("active_signals", [])
        active.append({
            "symbol":          symbol,
            "direction":       direction,
            "msg_id":          msg_id,
            "bar_index":       bar_index,
            "signal_bar_time": bar_index * 900_000,
            "entry":           sig.entry,
            "tp1":             sig.tp1,
            "tp2":             sig.tp2,
            "sl":              sig.sl,
            "tp1_hit":         False,
            "resolved":        False,
            "hist_id":         hist_id,
            "signal_type":     sig.signal_type,
        })

def check_active_signals(state: dict, bar_index_now: int,
                         scan_reference_ms: int | None = None):
    with _state_lock:
        signals = list(state.get("active_signals", []))
    if not signals:
        return

    ref_ms = scan_reference_ms if scan_reference_ms is not None else int(time.time() * 1000)
    still_active = []
    for sig in signals:
        age = bar_index_now - sig.get("bar_index", bar_index_now)
        if age > SIGNAL_MAX_AGE_BARS:
            print(f"  [TRACK] {sig['symbol']} expired after {age} bars — dropping")
            hist_id = sig.get("hist_id")
            if hist_id:
                update_signal_result(state, hist_id, "expired")
            with _state_lock:
                state.setdefault("resolved_signals", []).append({
                    "symbol":      sig["symbol"],
                    "direction":   sig.get("direction", ""),
                    "outcome":     "expired",
                    "resolved_at": int(time.time()),
                })
            continue
        if sig.get("resolved", False):
            continue

        symbol    = sig["symbol"]
        direction = sig["direction"]
        msg_id    = sig["msg_id"]
        tp1       = sig["tp1"]
        tp2       = sig["tp2"]
        sl        = sig["sl"]
        tp1_hit   = sig.get("tp1_hit", False)
        signal_bar_time_ms = sig.get("signal_bar_time")
        last_processed_ts  = sig.get("last_processed_candle_ts", signal_bar_time_ms or 0)

        try:
            candles = get_candles(symbol, "15m", N_15M,
                                  start_time_ms=signal_bar_time_ms,
                                  reference_ms=ref_ms)
        except Exception as e:
            print(f"  [TRACK] candle fetch failed for {symbol}: {e}")
            still_active.append(sig)
            continue

        if not candles:
            still_active.append(sig)
            continue

        new_candles = [c for c in candles if c["t"] > last_processed_ts]
        if not new_candles:
            still_active.append(sig)
            continue

        hist_id = sig.get("hist_id")
        entry_px = sig.get("entry", tp1)

        def resolve_signal(outcome: str):
            if hist_id:
                update_signal_result(state, hist_id, outcome)
            with _state_lock:
                state.setdefault("resolved_signals", []).append({
                    "symbol":      symbol,
                    "direction":   direction,
                    "outcome":     outcome,
                    "resolved_at": int(time.time()),
                })
            sig["resolved"] = True
            if outcome in ("tp1", "tp2", "sl"):
                cooldown_key = f"{symbol}_{direction}"
                with _state_lock:
                    state.setdefault("last_signal_outcome", {})[cooldown_key] = outcome
                    if outcome == "sl":
                        state.setdefault("post_loss_cooldown", {})[cooldown_key] = int(time.time())
                if outcome == "sl":
                    signal_type = sig.get("signal_type", "UNKNOWN")
                    record_failed_breakout(state, symbol, direction, signal_type, bar_index_now)

        for candle in new_candles:
            c_high = candle["h"]
            c_low  = candle["l"]
            c_open = candle["o"]
            c_close = candle["c"]
            last_processed_ts = candle["t"]

            if direction == "long":
                if not tp1_hit:
                    if c_high >= tp1 and c_low <= sl:
                        tp1_dist_from_open = abs(tp1 - c_open)
                        sl_dist_from_open  = abs(sl - c_open)
                        if sl_dist_from_open < tp1_dist_from_open:
                            react_to_message(msg_id, REACT_SL)
                            resolve_signal("sl")
                            break
                        else:
                            # [FIX-AUDIT-D14] SL was also touched same bar — resolve conservatively as TP1; do not wait for TP2
                            print(f"  [TRACK] {symbol} LONG same-bar TP1+SL — resolving as tp1 (SL touched, not exposing to TP2)")
                            react_to_message(msg_id, REACT_TP1)
                            resolve_signal("tp1")  # partial win; conservative resolution
                            break
                    elif c_high >= tp1:
                        react_to_message(msg_id, REACT_TP1)
                        tp1_hit = True
                        sig["tp1_hit"] = True
                    elif c_low <= sl:
                        react_to_message(msg_id, REACT_SL)
                        resolve_signal("sl")
                        break

                if tp1_hit and c_high >= tp2:
                    react_to_message(msg_id, REACT_TP2)
                    resolve_signal("tp2")
                    break

                if tp1_hit and not sig.get("resolved", False) and c_low <= sl:
                    print(f"  [TRACK] {symbol} SL hit after TP1 — resolving as tp1 (full win)")
                    resolve_signal("tp1")
                    break
            else:
                if not tp1_hit:
                    if c_low <= tp1 and c_high >= sl:
                        tp1_dist_from_open = abs(tp1 - c_open)
                        sl_dist_from_open  = abs(sl - c_open)
                        if sl_dist_from_open < tp1_dist_from_open:
                            react_to_message(msg_id, REACT_SL)
                            resolve_signal("sl")
                            break
                        else:
                            # [FIX-AUDIT-D14] SL was also touched same bar — resolve conservatively as TP1; do not wait for TP2
                            print(f"  [TRACK] {symbol} SHORT same-bar TP1+SL — resolving as tp1 (SL touched, not exposing to TP2)")
                            react_to_message(msg_id, REACT_TP1)
                            resolve_signal("tp1")  # partial win; conservative resolution
                            break
                    elif c_low <= tp1:
                        react_to_message(msg_id, REACT_TP1)
                        tp1_hit = True
                        sig["tp1_hit"] = True
                    elif c_high >= sl:
                        react_to_message(msg_id, REACT_SL)
                        resolve_signal("sl")
                        break

                if tp1_hit and c_low <= tp2:
                    react_to_message(msg_id, REACT_TP2)
                    resolve_signal("tp2")
                    break

                if tp1_hit and not sig.get("resolved", False) and c_high >= sl:
                    print(f"  [TRACK] {symbol} SL hit after TP1 — resolving as tp1 (full win)")
                    resolve_signal("tp1")
                    break

        if not sig.get("resolved", False):
            sig["last_processed_candle_ts"] = last_processed_ts
            still_active.append(sig)

    with _state_lock:
        state["active_signals"] = still_active

# ═══════════════════════════════════════════════════════════════
# DYNAMIC SPREAD EXEMPTION
# ═══════════════════════════════════════════════════════════════

_spread_history: dict[str, list[float]] = {}
_spread_history_lock = threading.Lock()

def update_spread_history(symbol: str, spread_pct: float) -> None:
    with _spread_history_lock:
        hist = _spread_history.setdefault(symbol, [])
        hist.append(spread_pct)
        _spread_history[symbol] = hist[-SPREAD_DYNAMIC_HISTORY_LEN:]

def is_spread_exempt_dynamic(symbol: str) -> bool:
    if symbol in SPREAD_EXEMPT:
        return True

    with _spread_history_lock:
        hist = _spread_history.get(symbol, [])
        if len(hist) >= 3:
            avg_spread = sum(hist) / len(hist)
            if avg_spread < SPREAD_DYNAMIC_AVG_THRESHOLD:
                return True

    return False

# ═══════════════════════════════════════════════════════════════
# DAILY SUMMARY
# ═══════════════════════════════════════════════════════════════

def should_send_summary(state: dict) -> bool:
    now = datetime.now(timezone.utc)
    with _state_lock:
        last = state.get("last_summary_ts", 0)
    if now.hour == 8 and now.minute < 15 and (now.timestamp() - last) > 3600:
        with _state_lock:
            state["last_summary_ts"] = now.timestamp()
        return True
    return False

def send_summary(state: dict):
    cutoff_24h = int(time.time()) - 86400
    cutoff_48h = int(time.time()) - 172800
    with _state_lock:
        recent = [e for e in state.get("resolved_signals", []) if e["resolved_at"] >= cutoff_24h]
        all_history = [e for e in state.get("signal_history", [])
                        if e.get("result") in ("tp1", "tp2", "sl")]
    tp1_count  = sum(1 for e in recent if e["outcome"] == "tp1")
    tp2_count  = sum(1 for e in recent if e["outcome"] == "tp2")
    sl_count   = sum(1 for e in recent if e["outcome"] == "sl")
    winners    = tp1_count + tp2_count
    losers     = sl_count
    if winners == 0 and losers == 0:
        return

    total      = len(all_history)
    overall_wr = None
    if total >= WIN_RATE_MIN_SAMPLE:
        wins       = sum(
            1.0 if r.get("result") in ("tp1", "tp2") else 0.0
            for r in all_history
        )
        overall_wr = wins / total

    lines = [
        f"📊 Signal Summary (last 24h)  •  v{__version__}",
        f"✅ Winners: {winners} (🔥×{tp1_count}  🏆×{tp2_count})",
        f"❌ Losers:  {losers} (😭×{sl_count})",
    ]
    if overall_wr is not None:
        lines.append(f"📈 Overall Win Rate: {overall_wr*100:.0f}% ({total} trades)")

    def r_pnl(entries):
        return sum(
            +2 if e["result"] == "tp2"
            else +1 if e["result"] == "tp1"
            else -1
            for e in entries if e.get("result") in ("tp1", "tp2", "sl")
        )

    breakdown_lines = []
    for sig_type in ("BREAK", "PULL"):
        for dirn in ("long", "short"):
            subset = [e for e in all_history
                      if e.get("signal_type") == sig_type and e.get("direction") == dirn]
            if len(subset) >= 5:
                wins = sum(
                    1.0 if e["result"] in ("tp1", "tp2") else 0.0
                    for e in subset
                )
                wr   = wins / len(subset)
                pnl  = r_pnl(subset)
                breakdown_lines.append(
                    f"  {sig_type} {dirn}: {wr*100:.0f}% WR  {pnl:+.2f}R  (n={len(subset)})"
                )

    if breakdown_lines:
        lines.append("\n📋 By type / direction:\n" + "\n".join(breakdown_lines))

    send_telegram("\n".join(lines))
    with _state_lock:
        state["resolved_signals"] = [
            e for e in state.get("resolved_signals", []) if e["resolved_at"] >= cutoff_48h
        ]

# ═══════════════════════════════════════════════════════════════
# SCAN
# ═══════════════════════════════════════════════════════════════

def collect_market_inputs(symbol: str, state: dict, reference_ms: int) -> tuple | None:
    data = fetch_all_candles(symbol, reference_ms=reference_ms)
    if data is None:
        return None
    candles_15m, candles_1h, candles_4h, candles_d = data
    record_market_inputs_from_candles(symbol, candles_15m, candles_4h)

    ctx = get_market_context(symbol)
    if ctx and ctx.get("funding") is not None:
        update_funding_history(state, symbol, ctx.get("funding"))

    return data

def log_liquidity_confluence(state: dict, symbol: str, direction: str,
                              sig: SignalResult) -> None:
    """Persist confluence contribution for backtesting comparison."""
    if not ENABLE_LIQUIDITY_CONFLUENCE:
        return
    with _state_lock:
        log = state.setdefault("liquidity_confluence_log", [])
        log.append({
            "timestamp": int(time.time()),
            "symbol": symbol,
            "direction": direction,
            "signal_type": sig.signal_type,
            "base_score": sig.score,
            "pre_liq_score": sig.final_score - getattr(sig, 'liquidity_bonus', 0),
            "liquidity_bonus": getattr(sig, 'liquidity_bonus', 0),
            "final_score": sig.final_score,
            "liquidity_score": getattr(sig, 'liquidity_score', 0),
            "structure_score": getattr(sig, 'structure_score', 0),
            "sweep": getattr(sig, 'liquidity_sweep', {}),
            "mss": getattr(sig, 'mss', {}),
            "bos": getattr(sig, 'bos', {}),
            "fvg": getattr(sig, 'fvg', {}),
            "zone": getattr(sig, 'premium_discount', 'neutral'),
            "htf_aligned": getattr(sig, 'htf_alignment', False),
            "reasons": getattr(sig, 'liquidity_reasons', []),
            # [Fix-43] Real HTF (1h/4h) liquidity structure — logged
            # alongside the existing fields above for the same backtesting
            # comparison purpose (e.g. "did the HTF zone/sweep correlate
            # with win rate" once enough history accumulates).
            "htf_liquidity_score": getattr(sig, 'htf_liquidity_score', 0),
            "htf_1h_zone": getattr(sig, 'htf_1h_zone', 'neutral'),
            "htf_4h_zone": getattr(sig, 'htf_4h_zone', 'neutral'),
            "htf_1h_sweep": getattr(sig, 'htf_1h_sweep', {}),
            "htf_4h_sweep": getattr(sig, 'htf_4h_sweep', {}),
        })
        if len(log) > 5000:
            state["liquidity_confluence_log"] = log[-5000:]

def scan_symbol(symbol: str, state: dict, bar_index_now: int,
                candle_bundle: tuple | None = None,
                reference_ms: int | None = None) -> list[tuple]:
    coin = hl_coin(symbol)
    if candle_bundle is None:
        data = fetch_all_candles(symbol, reference_ms=reference_ms)
        if data is None:
            print(f"    Skipping {coin}: insufficient candles")
            return []
    else:
        data = candle_bundle

    candles_15m, candles_1h, candles_4h, candles_d = data

    ctx = get_market_context(symbol)
    if ctx:
        if ctx.get("open_interest") is not None and ctx["open_interest"] < MIN_OI_USD:
            print(f"    Skipping {coin}: OI too low (${ctx['open_interest']:,.0f})")
            return []

        oi_usd_val = ctx.get("open_interest")
        update_oi_history(state, symbol, oi_usd_val)

    live_cache   = get_meta_and_asset_ctxs()
    live_funding = (live_cache.get(hl_coin(symbol), {}).get("funding")
                    if live_cache else None)
    _ctx_funding = ctx.get("funding") if ctx else None
    funding_for_suppress = live_funding if live_funding is not None else _ctx_funding
    if live_funding is not None and live_funding != _ctx_funding:
        pct = (live_funding * 100) if live_funding is not None else float("nan")
        print(f"    [FUNDING REFRESH] {coin}: updated → {pct:+.4f}%/8h")

    sig = compute_signals(symbol, candles_15m, candles_1h, candles_4h, candles_d,
                          state, record_market_inputs=False, reference_ms=reference_ms,
                          funding_rate=funding_for_suppress)
    if not (sig.fire_long or sig.fire_short):
        print(f"    {coin}: no signal")
        return []

    direction = "long" if sig.fire_long else "short"
    if not check_cooldown(state, symbol, direction, bar_index_now,
                          signal_type=sig.signal_type,
                          candidate_score=sig.final_score):
        print(f"    {coin} signal suppressed by cooldown")
        return []

    print(f"    🚀 SIGNAL: {coin} {direction.upper()} [{sig.signal_type}] "
          f"base={sig.score} final={sig.final_score}")

    if ctx:
        sig.funding_rate  = live_funding if live_funding is not None else ctx.get("funding")
        sig.open_interest = ctx.get("open_interest")
        pct = (sig.funding_rate * 100) if sig.funding_rate is not None else float("nan")
        print(f"    [MARKET CTX] {coin}: funding={pct:+.4f}%/8h  {format_oi(sig.open_interest)}")

    if FUNDING_SUPPRESS_EXTREME is not None and funding_for_suppress is not None:
        rate     = funding_for_suppress
        headwind = (rate > 0 and direction == "long") or (rate < 0 and direction == "short")
        if headwind and abs(rate) >= FUNDING_SUPPRESS_EXTREME:
            pct = rate * 100
            print(f"    [FUNDING FILTER] {coin} {direction.upper()} suppressed — "
                  f"funding {pct:+.4f}%/8h is extreme headwind")
            return []

    _breadth_pct_scan = compute_market_breadth()["breadth_50_pct"]
    _eff_min_scan = get_effective_min_score(get_btc_regime(), _breadth_pct_scan)

    if not is_spread_exempt_dynamic(symbol):
        live_mark = (live_cache.get(hl_coin(symbol), {}).get("mark_px")
                     if live_cache else None)
        if live_mark is not None and live_mark > 0:
            _close_px = candles_15m[-1]["c"]
            _spread_pct = abs(live_mark - _close_px) / _close_px * 100.0
            sig.spread_pct = _spread_pct
            update_spread_history(symbol, _spread_pct)
            if _spread_pct >= SPREAD_SUPPRESS_PCT:
                print(f"    [SPREAD FILTER] {coin} {direction.upper()} hard suppressed — "
                      f"mark/close divergence {_spread_pct:.3f}% >= {SPREAD_SUPPRESS_PCT:.2f}%")
                return []
            elif _spread_pct >= SPREAD_WARN_PCT:
                sig.final_score -= 1
                sig.score_adjustments.append(
                    (f"Spread/liquidity warn ({_spread_pct:.3f}%)", -1)
                )
                print(f"    [SPREAD WARN] {coin} {direction.upper()} −1 score — "
                      f"mark/close divergence {_spread_pct:.3f}%")
                if sig.final_score < _eff_min_scan:
                    print(f"    [SPREAD FILTER] {coin} suppressed after spread penalty "
                          f"(final={sig.final_score} < eff_min={_eff_min_scan})")
                    return []
    else:
        live_mark = (live_cache.get(hl_coin(symbol), {}).get("mark_px")
                     if live_cache else None)
        if live_mark is not None and live_mark > 0:
            _close_px = candles_15m[-1]["c"]
            _spread_pct = abs(live_mark - _close_px) / _close_px * 100.0
            sig.spread_pct = _spread_pct
            update_spread_history(symbol, _spread_pct)

    log_liquidity_confluence(state, symbol, direction, sig)
    return [(symbol, direction, sig)]

# ═══════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════

def deduplicate_correlated(signals: list[tuple]) -> list[tuple]:
    # IMPORTANT: do NOT re-sort here.  The caller has already sorted
    # `signals` by priority_score() (composite quality ranking).  Any
    # internal re-sort would discard that ranking and cause lower-quality
    # signals to survive dedup while higher-quality ones get dropped.
    # Within each correlation group the first (highest priority_score)
    # signal wins; later ones in the same group are silently dropped.
    seen_groups: set[tuple] = set()
    result: list[tuple]     = []
    for sig_tuple in signals:
        symbol    = sig_tuple[0]
        direction = sig_tuple[1]
        group  = next(
            (g for g, members in CORR_GROUPS.items() if symbol in members),
            symbol
        )
        if (group, direction) not in seen_groups:
            seen_groups.add((group, direction))
            result.append(sig_tuple)

    # [Fix-17] The pass above only dedupes by (group, direction), so a long on one
    # symbol and a short on a correlated symbol in the same CORR_GROUPS entry could
    # both survive and fire in the same scan — a silent, emergent hedge pair rather
    # than a deliberate decision. Explicitly detect any group with both a surviving
    # long and a surviving short, and drop the lower-priority_score() one of the
    # pair (more conservative than keeping both). The alternative — keep both but
    # tag them as a "correlated hedge pair" in the Telegram message / signal
    # history — was not implemented here; flag for reviewer if that's preferred.
    group_of = lambda sym: next(
        (g for g, members in CORR_GROUPS.items() if sym in members), sym
    )
    by_group: dict = {}
    for sig_tuple in result:
        g = group_of(sig_tuple[0])
        by_group.setdefault(g, []).append(sig_tuple)

    dropped_ids = set()
    for g, tuples in by_group.items():
        longs  = [t for t in tuples if t[1] == "long"]
        shorts = [t for t in tuples if t[1] == "short"]
        if longs and shorts:
            best_long  = max(longs,  key=lambda t: priority_score(t[2]))
            best_short = max(shorts, key=lambda t: priority_score(t[2]))
            loser = best_short if priority_score(best_long) >= priority_score(best_short) else best_long
            dropped_ids.add(id(loser))
            print(f"  [CORR CONFLICT] Group '{g}' has both long ({best_long[0]}) and "
                  f"short ({best_short[0]}) surviving dedup — dropping lower-priority "
                  f"{loser[1]} on {loser[0]}.")

    if dropped_ids:
        result = [t for t in result if id(t) not in dropped_ids]

    return result

_shutdown = False

def _handle_sigterm(signum, frame):
    global _shutdown
    _shutdown = True

os_signal.signal(os_signal.SIGTERM, _handle_sigterm)

def main():
    print(f"[{datetime.now(timezone.utc).isoformat()}] Scanner starting…")
    print(f"Watchlist ({len(WATCHLIST)} pairs): {[hl_coin(s) for s in WATCHLIST]}")

    # [AUDIT-P7-1] Operator alert if the liquidity engine failed to load — every liquidity
    # score (including hard suppress) silently does nothing for the rest of this process if
    # so. Uses send_telegram() (the real function in this file takes just `text`, no
    # parse_mode kwarg — HTML is hardcoded inside it). Gated on the module-level flag so a
    # long-running process only sends this once, not every scan cycle.
    global _liq_engine_missing_alerted
    if _liq_engine is None and not _liq_engine_missing_alerted:
        send_telegram(
            "⚠️ LIQUIDITY ENGINE NOT LOADED — all liquidity scoring (including hard suppress) "
            "is disabled. Check liquidity_confluence module import."
        )
        _liq_engine_missing_alerted = True

    scan_reference_ms = int(time.time() * 1000)
    bar_index_now     = scan_reference_ms // (15 * 60 * 1000)
    state             = load_state()

    prune_state(state)

    reset_breadth_cache()
    reset_rs_cache()
    reset_win_rates_cache()
    clear_indicator_cache()

    try:
        _selftest_indicator_cache_isolation()
        print("[INIT] Indicator cache isolation self-test passed.")
    except AssertionError as e:
        print(f"[INIT] FATAL: Indicator cache self-test failed: {e}")
        sys.exit(1)

    print("[INIT] Fetching market context (metaAndAssetCtxs)…")
    get_meta_and_asset_ctxs()

    print("[INIT] Computing BTC regime…")
    try:
        btc_1h = get_candles("BTCUSDT", "1h", N_1H, reference_ms=scan_reference_ms)
        btc_4h = get_candles("BTCUSDT", "4h", N_4H, reference_ms=scan_reference_ms)
        btc_1d = get_candles("BTCUSDT", "1d", N_1D, reference_ms=scan_reference_ms)
        regime = compute_btc_regime(btc_1h, btc_4h, btc_1d)
        set_btc_regime(regime)
        print(f"  {regime['label']}")
    except Exception as e:
        print(f"  [BTC REGIME] failed to compute: {e}")

    if _shutdown:
        save_state(state)
        sys.exit(0)

    print("[TRACK] Checking active signals…")
    check_active_signals(state, bar_index_now, scan_reference_ms)
    save_state(state)

    if should_send_summary(state):
        send_summary(state)
        save_state(state)

    print("[PHASE 1] Collecting market breadth / RS inputs…")
    candle_bundles: dict[str, tuple] = {}
    # [AUDIT-P7-5] Track per-scan worker error count and alert if all (or most) symbols
    # fail — a strong signal of a systematic API outage rather than isolated per-symbol
    # issues (e.g. one bad/delisted pair). Uses send_telegram() (real signature: just
    # `text`, no parse_mode kwarg).
    _phase1_errors = 0
    with ThreadPoolExecutor(max_workers=max(1, SCAN_WORKERS)) as ex:
        futures = {
            ex.submit(collect_market_inputs, sym, state, scan_reference_ms): sym
            for sym in WATCHLIST
        }
        for fut in as_completed(futures):
            sym = futures[fut]
            try:
                bundle = fut.result()
                if bundle is not None:
                    candle_bundles[sym] = bundle
            except Exception as e:
                print(f"    ERROR collecting inputs for {sym}: {e}")
                _phase1_errors += 1

    if _phase1_errors == len(WATCHLIST):
        send_telegram(
            f"🚨 ALL {_phase1_errors} SYMBOLS FAILED market-input collection this scan "
            f"cycle — possible systematic API outage."
        )
    elif _phase1_errors > len(WATCHLIST) * 0.5:
        print(f"  [SCAN] {_phase1_errors}/{len(WATCHLIST)} symbols failed input collection — "
              f"partial API degradation")

    finalize_breadth_cache()
    finalize_rs_cache()
    with _breadth_lock:
        breadth_n = len(_breadth_snapshot or {})
    with _rs_lock:
        rs_n = len(_rs_snapshot or {})
    print(f"  Breadth symbols: {breadth_n}  RS symbols: {rs_n}")

    btc_4h_bundle = candle_bundles.get("BTCUSDT")
    if btc_4h_bundle:
        # [AUDIT-P7-4] named index with assertion to catch a fetch_all_candles order
        # change early instead of silently feeding the wrong timeframe into BTC correlation.
        btc_4h_candles = btc_4h_bundle[CANDLE_BUNDLE_IDX_4H]
        assert len(btc_4h_bundle[CANDLE_BUNDLE_IDX_4H]) > 0 and btc_4h_bundle[CANDLE_BUNDLE_IDX_4H][0].get("t"), \
            "BTC 4H candle bundle index mismatch — check fetch_all_candles return order"
        for sym, bundle in candle_bundles.items():
            if sym == "BTCUSDT":
                continue
            update_dynamic_btc_correlation(sym, bundle[CANDLE_BUNDLE_IDX_4H], btc_4h_candles)

    # [Fix-33] Section 6 Item 6 diagnostic — see LOG_DYNAMIC_CORR_GROUPS and
    # log_dynamic_layer1_clustering() above. No-op unless explicitly enabled.
    log_dynamic_layer1_clustering(candle_bundles)

    if _shutdown:
        save_state(state)
        sys.exit(0)

    print("[PHASE 2] Scanning symbols for signals…")
    print("[INIT] Refreshing market context before Phase 2…")
    get_meta_and_asset_ctxs()

    pending_signals: list[tuple] = []
    # [AUDIT-P7-5] Same per-scan error aggregation as Phase 1, applied to the signal-scanning
    # loop — a separate failure mode (scan_symbol failing vs. collect_market_inputs failing).
    _phase2_errors = 0
    with ThreadPoolExecutor(max_workers=max(1, SCAN_WORKERS)) as ex:
        futures = {
            ex.submit(scan_symbol, sym, state, bar_index_now, candle_bundles.get(sym),
                      scan_reference_ms): sym
            for sym in WATCHLIST
        }
        for fut in as_completed(futures):
            sym = futures[fut]
            try:
                result = fut.result()
                if result:
                    pending_signals.extend(result)
            except Exception as e:
                print(f"    ERROR processing {sym}: {e}")
                _phase2_errors += 1

    if _phase2_errors == len(WATCHLIST):
        send_telegram(
            f"🚨 ALL {_phase2_errors} SYMBOLS FAILED signal scanning this scan cycle — "
            f"possible systematic API outage."
        )
    elif _phase2_errors > len(WATCHLIST) * 0.5:
        print(f"  [SCAN] {_phase2_errors}/{len(WATCHLIST)} symbols failed signal scanning — "
              f"partial API degradation")

    pending_signals.sort(key=lambda t: priority_score(t[2]), reverse=True)
    deduped_signals = deduplicate_correlated(pending_signals)

    _btc_regime_main = get_btc_regime()
    _breadth_pct_main = compute_market_breadth()["breadth_50_pct"]
    _max_signals = get_dynamic_max_signals(_btc_regime_main, _breadth_pct_main)
    print(f"  [DYNAMIC SIGNALS] Max signals this scan: {_max_signals} "
          f"(BTC regime: {_btc_regime_main['label'] if _btc_regime_main else 'Unknown'}, "
          f"breadth: {_breadth_pct_main*100:.0f}%)")

    top_signals     = deduped_signals[:_max_signals]
    top_set         = set(id(t) for t in top_signals)
    dropped_signals = [t for t in deduped_signals if id(t) not in top_set]

    if dropped_signals:
        dropped_names = [f"{hl_coin(s)} {d.upper()}" for s, d, _ in dropped_signals]
        print(f"  [RANK] Dropped {len(dropped_signals)} lower-priority signal(s): {', '.join(dropped_names)}")
        for symbol, direction, sig in dropped_signals:
            record_signal_history(
                state, symbol, direction, sig.signal_type, sig.final_score,
                sig.funding_rate, sig.atr_pct,
                sig.oi_trend_data.get("oi_change_pct"),
                sig.alignment_mode,
                sent=False,
                liquidity_bonus=sig.liquidity_bonus,
                liquidity_score=sig.liquidity_score,
                structure_score=sig.structure_score
            )

    signals_fired = 0
    for rank, (symbol, direction, sig) in enumerate(top_signals, start=1):
        msg    = format_signal(symbol, sig, "CORE", rank=rank)
        msg_id = send_telegram(msg)

        hist_id = record_signal_history(
            state, symbol, direction, sig.signal_type, sig.final_score,
            sig.funding_rate, sig.atr_pct,
            sig.oi_trend_data.get("oi_change_pct"),
            sig.alignment_mode,
            sent=True,
            liquidity_bonus=sig.liquidity_bonus,
            liquidity_score=sig.liquidity_score,
            structure_score=sig.structure_score
        )

        if msg_id:
            update_cooldown(state, symbol, direction, bar_index_now, signal_type=sig.signal_type)
            track_signal(state, symbol, direction, msg_id, sig, bar_index_now, hist_id=hist_id)
            print(f"  [TRACK] #{rank} {hl_coin(symbol)} {direction.upper()} "
                  f"TP1={sig.tp1:.4f} TP2={sig.tp2:.4f} SL={sig.sl:.4f}")
        else:
            print(f"  [TG FAIL] #{rank} {hl_coin(symbol)} {direction.upper()} "
                  f"— Telegram send failed, cooldown NOT set (eligible to re-fire next scan)")
        signals_fired += 1
        time.sleep(0.5)

    save_state(state)
    print(f"Scan complete. {signals_fired} signal(s) fired.")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        try:
            send_telegram(f"🚨 Engine crashed: {e}")
        except:
            pass
        raise
    finally:
        _hl_session.close()
