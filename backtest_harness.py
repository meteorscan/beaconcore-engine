"""
backtest_harness.py — Beaconcore Engine Backtest Harness
=========================================================
Replays compute_signals() bar-by-bar over historical Hyperliquid candles
to measure raw signal quality WITHOUT modifying any engine code.

Usage
-----
    # Quick check — BTC only, 30 days (run this first):
    python backtest_harness.py

    # Full watchlist, 90 days:
    python backtest_harness.py --days 90 --symbols all

    # Single symbol:
    python backtest_harness.py --days 60 --symbols BTCUSDT

    # Multiple symbols:
    python backtest_harness.py --days 60 --symbols BTCUSDT,ETHUSDT,SOLUSDT

KNOWN LIMITATIONS (read before interpreting results)
-----------------------------------------------------
1. OI / Funding unavailable historically.
   compute_signals() receives funding_rate=None and an empty state dict
   (no OI history). OI score adjustments return 0 and oi_trend="unknown".
   Signals suppressed in live trading by extreme funding will NOT be
   suppressed here. All signals are tagged oi_dependent=True.

2. No market breadth / RS in replay.
   get_btc_regime(), compute_market_breadth(), finalize_rs_cache() all read
   from global caches populated by the live scan loop. In backtest those
   caches are empty — BTC regime returns neutral/unknown and breadth
   defaults to 0.5. Regime-gated signals (Tier C breadth hard-gates,
   dynamic max signals) will score differently than live.

3. Empty state = no cooldowns, no win-rate adaptive scoring.
   Raw signal frequency will be higher than live. This is intentional —
   you are measuring signal quality, not simulated live frequency.

4. No SPREAD filter.
   scan_symbol()'s spread check requires a live mark price. Only
   compute_signals() is called here so spread penalties do not apply.

5. Entry at bar close.
   Entry price is the close of the signal bar. Live entry is slightly
   above/below close due to slippage. Results will be marginally optimistic
   on RR.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# ── Import from the engine (no modifications to engine code) ──────────────
# signal_engine.py raises RuntimeError at import if TG_BOT_TOKEN/TG_CHAT_ID
# env vars are missing. Stub them before import so the harness can run
# standalone without a live Telegram config.
import os
os.environ.setdefault("TG_BOT_TOKEN", "backtest_stub")
os.environ.setdefault("TG_CHAT_ID",   "backtest_stub")

from signal_engine import (  # noqa: E402
    compute_signals,
    get_candles,
    hl_coin,
    WATCHLIST,
    INTERVAL_MS,
    N_15M, N_1H, N_4H, N_1D,
)
import signal_engine as _signal_engine

# ── Macro calendar rate-limit fix ─────────────────────────────────────────
# In live mode, fetch_macro_calendar() caches results in the state dict for
# 1 hour (MACRO_CACHE_TTL_S=3600). In backtest, state={} is passed on every
# bar so the cache is wiped each time — causing thousands of HTTP requests
# and 429 rate-limit errors from faireconomy.media.
# Fix: monkey-patch fetch_macro_calendar() to fetch exactly once at startup
# and reuse that result for all bars. Zero changes to signal_engine.py.
_macro_once_cache: list = []
_macro_fetched: bool = False

def _macro_fetch_once(state: dict) -> list:  # noqa: ANN001
    global _macro_once_cache, _macro_fetched
    if not _macro_fetched:
        try:
            _macro_once_cache = _signal_engine.fetch_macro_calendar.__wrapped__(state) \
                if hasattr(_signal_engine.fetch_macro_calendar, "__wrapped__") \
                else _original_macro_fetch(state)
        except Exception as exc:
            print(f"[BT] Macro calendar pre-fetch failed ({exc}) — using empty cache")
            _macro_once_cache = []
        _macro_fetched = True
        print(f"[BT] Macro calendar pre-fetched once ({len(_macro_once_cache)} event(s)) "
              f"— will reuse for all replay bars")
    return _macro_once_cache

_original_macro_fetch = _signal_engine.fetch_macro_calendar
_signal_engine.fetch_macro_calendar = _macro_fetch_once
# ─────────────────────────────────────────────────────────────────────────

# ── Constants ─────────────────────────────────────────────────────────────
WARMUP_BARS  = 250   # bars skipped so EMA200 / ADX / ATR have valid history
OUTCOME_BARS = 48    # bars forward to evaluate TP / SL (48 × 15m = 12 hours)
REPLAY_STEP  = 1     # advance 1 bar per iteration (most accurate, slowest)
                     # set to 2 for ~2× speed with minimal accuracy loss


# ══════════════════════════════════════════════════════════════════════════
# CANDLE FETCHING
# ══════════════════════════════════════════════════════════════════════════

def fetch_backtest_candles(
    symbol: str,
    interval: str,
    days: int,
    end_reference_ms: int,
) -> list[dict]:
    """
    Fetch `days` of closed candles up to `end_reference_ms` using the
    engine's existing get_candles() with an explicit start_time_ms.
    Adds a 50-bar warmup buffer beyond the requested window.
    """
    iv_ms        = INTERVAL_MS.get(interval, 60 * 60 * 1000)
    bars_per_day = (24 * 60 * 60 * 1000) // iv_ms
    n_bars       = days * bars_per_day + WARMUP_BARS + 50   # warmup + safety buffer
    start_ms     = end_reference_ms - (n_bars * iv_ms)

    return get_candles(
        symbol,
        interval,
        n_bars,
        start_time_ms=start_ms,
        reference_ms=end_reference_ms,
    )


# ══════════════════════════════════════════════════════════════════════════
# LOOKAHEAD-SAFE CANDLE SLICER
# ══════════════════════════════════════════════════════════════════════════

def slice_before(candles: list[dict], before_ms: int, min_bars: int) -> list[dict] | None:
    """
    Return all candles with open timestamp strictly < before_ms.
    Returns None if fewer than min_bars survive (insufficient history at
    this bar — skip it rather than producing invalid indicator values).

    Uses timestamp filtering, NOT index arithmetic, to prevent lookahead
    across timeframe boundaries (e.g. i // 4 is approximate and wrong near
    hourly closes).
    """
    sliced = [c for c in candles if c["t"] < before_ms]
    return sliced if len(sliced) >= min_bars else None


# ══════════════════════════════════════════════════════════════════════════
# OUTCOME EVALUATOR
# ══════════════════════════════════════════════════════════════════════════

def evaluate_outcome(
    direction: str,
    entry: float,      # noqa: ARG001  (kept for caller clarity / future use)
    tp1: float,
    tp2: float,
    sl: float,
    future_bars: list[dict],
) -> str:
    """
    Walk future_bars and return the first resolved outcome.

    Return values
    -------------
    'tp2'         — TP2 hit after TP1
    'tp1'         — TP1 hit, price did not return to SL within OUTCOME_BARS
    'tp1_then_sl' — TP1 hit, then SL hit on a later bar
    'sl'          — SL hit before TP1
    'open'        — neither target hit within OUTCOME_BARS

    Same-bar TP1 + SL tie-breaking: whichever level is closer to the bar's
    open price wins. This mirrors the live bot's check_active_signals()
    resolution exactly (see the c_open distance comparison there).
    """
    tp1_hit = False

    for bar in future_bars:
        c_open = bar["o"]
        c_high = bar["h"]
        c_low  = bar["l"]

        if direction == "long":
            if not tp1_hit:
                if c_high >= tp1 and c_low <= sl:
                    # Same bar: closer to open wins
                    if abs(sl - c_open) < abs(tp1 - c_open):
                        return "sl"
                    return "tp1"   # conservative — don't expose to TP2
                if c_high >= tp1:
                    tp1_hit = True
                elif c_low <= sl:
                    return "sl"
            else:
                if c_high >= tp2:
                    return "tp2"
                if c_low <= sl:
                    return "tp1_then_sl"

        else:  # short
            if not tp1_hit:
                if c_low <= tp1 and c_high >= sl:
                    if abs(sl - c_open) < abs(tp1 - c_open):
                        return "sl"
                    return "tp1"
                if c_low <= tp1:
                    tp1_hit = True
                elif c_high >= sl:
                    return "sl"
            else:
                if c_low <= tp2:
                    return "tp2"
                if c_high >= sl:
                    return "tp1_then_sl"

    return "tp1" if tp1_hit else "open"


# ══════════════════════════════════════════════════════════════════════════
# SINGLE-SYMBOL REPLAY
# ══════════════════════════════════════════════════════════════════════════

def replay_symbol(
    symbol: str,
    all_15m: list[dict],
    all_1h:  list[dict],
    all_4h:  list[dict],
    all_1d:  list[dict],
    verbose: bool = False,
) -> list[dict]:
    """
    Slide bar-by-bar over pre-fetched candles, calling compute_signals()
    at each bar with only the history available up to that point.

    Returns a list of signal dicts (outcomes evaluated separately in
    run_backtest so future_bars can be stripped before JSON serialisation).

    Notes
    -----
    - state={} on every call: no cooldowns, no OI/funding history.
      This is intentional — we measure raw signal quality.
    - record_market_inputs=False: prevents writes to the global breadth /
      RS caches that the live scan loop maintains.
    - reference_ms is set to the bar's open timestamp so daily_vwap() and
      filter_closed_candles() behave correctly without lookahead.
    """
    signals: list[dict] = []
    end_idx = len(all_15m) - OUTCOME_BARS

    for i in range(WARMUP_BARS, end_idx, REPLAY_STEP):
        bar    = all_15m[i]
        ref_ms = bar["t"]   # this bar's open = "now"

        w15 = slice_before(all_15m, ref_ms, min_bars=50)
        w1h = slice_before(all_1h,  ref_ms, min_bars=14)
        w4h = slice_before(all_4h,  ref_ms, min_bars=10)
        w1d = slice_before(all_1d,  ref_ms, min_bars=1)

        if w15 is None or w1h is None or w4h is None:
            continue   # not enough history yet

        try:
            result = compute_signals(
                symbol               = symbol,
                candles_15m          = w15,
                candles_1h           = w1h,
                candles_4h           = w4h,
                candles_d            = w1d,
                state                = {},      # empty: no cooldowns
                record_market_inputs = False,   # don't pollute global caches
                reference_ms         = ref_ms,
                funding_rate         = None,    # not available historically
            )
        except Exception as exc:
            if verbose:
                dt = datetime.fromtimestamp(ref_ms / 1000, tz=timezone.utc)
                print(f"  [BT-ERROR] {hl_coin(symbol)} bar {i} "
                      f"({dt.strftime('%Y-%m-%d %H:%M')}): {exc}")
            continue

        if not (result.fire_long or result.fire_short):
            continue

        direction   = "long" if result.fire_long else "short"
        entry_price = w15[-1]["c"]   # close of the signal bar
        future_bars = all_15m[i + 1 : i + 1 + OUTCOME_BARS]

        sig_record = {
            "symbol":       symbol,
            "bar_index":    i,
            "timestamp_ms": ref_ms,
            "datetime":     datetime.fromtimestamp(
                                ref_ms / 1000, tz=timezone.utc
                            ).strftime("%Y-%m-%d %H:%M UTC"),
            "direction":    direction,
            "signal_type":  result.signal_type,
            "break_tier":   getattr(result, "break_tier", None),
            "pull_tier":    getattr(result, "pull_tier",  None),
            "base_score":   result.score,
            "final_score":  result.final_score,
            "entry":        entry_price,
            "tp1":          result.tp1,
            "tp2":          result.tp2,
            "sl":           result.sl,
            "atr_pct":      getattr(result, "atr_pct", None),
            "rr_tp1":       round(abs(result.tp1 - entry_price) /
                                  abs(entry_price - result.sl), 2)
                            if abs(entry_price - result.sl) > 0 else None,
            "rr_tp2":       round(abs(result.tp2 - entry_price) /
                                  abs(entry_price - result.sl), 2)
                            if abs(entry_price - result.sl) > 0 else None,
            "oi_dependent": True,   # always True: OI/funding unavailable historically
            # outcome and future_bars filled by run_backtest()
            "outcome":      None,
            "future_bars":  future_bars,
        }

        signals.append(sig_record)

        if verbose:
            dt = datetime.fromtimestamp(ref_ms / 1000, tz=timezone.utc)
            print(f"  → {hl_coin(symbol)} {direction.upper()} "
                  f"[{result.signal_type}] score={result.final_score} "
                  f"@ {dt.strftime('%Y-%m-%d %H:%M')}")

    return signals


# ══════════════════════════════════════════════════════════════════════════
# RESULTS AGGREGATOR
# ══════════════════════════════════════════════════════════════════════════

def aggregate_results(signals: list[dict]) -> dict:
    """
    Compute win rate, frequency, and per-type / per-score / per-symbol
    breakdowns. Call after outcomes have been evaluated and future_bars
    stripped.
    """
    total = len(signals)
    if total == 0:
        return {"total": 0}

    wins   = sum(1 for s in signals if s["outcome"] in ("tp2", "tp1"))
    losses = sum(1 for s in signals if s["outcome"] == "sl")

    agg: dict = {
        "total":        total,
        "tp2":          sum(1 for s in signals if s["outcome"] == "tp2"),
        "tp1":          sum(1 for s in signals if s["outcome"] == "tp1"),
        "tp1_then_sl":  sum(1 for s in signals if s["outcome"] == "tp1_then_sl"),
        "sl":           losses,
        "open":         sum(1 for s in signals if s["outcome"] == "open"),
        "win_rate":     round(wins   / total, 4),
        "loss_rate":    round(losses / total, 4),
        "by_type":      {},
        "by_direction": {},
        "by_score":     {},
        "by_symbol":    {},
    }

    # ── By signal type ────────────────────────────────────────────────────
    for sig_type in ("BREAK", "PULL"):
        subset = [s for s in signals if s.get("signal_type") == sig_type]
        if subset:
            w = sum(1 for s in subset if s["outcome"] in ("tp2", "tp1"))
            agg["by_type"][sig_type] = {
                "total":    len(subset),
                "wins":     w,
                "win_rate": round(w / len(subset), 4),
            }

    # ── By direction ──────────────────────────────────────────────────────
    for dirn in ("long", "short"):
        subset = [s for s in signals if s.get("direction") == dirn]
        if subset:
            w = sum(1 for s in subset if s["outcome"] in ("tp2", "tp1"))
            agg["by_direction"][dirn] = {
                "total":    len(subset),
                "wins":     w,
                "win_rate": round(w / len(subset), 4),
            }

    # ── By minimum final_score ────────────────────────────────────────────
    for floor in range(3, 11):
        subset = [s for s in signals if (s.get("final_score") or 0) >= floor]
        if subset:
            w = sum(1 for s in subset if s["outcome"] in ("tp2", "tp1"))
            agg["by_score"][f">={floor}"] = {
                "total":    len(subset),
                "wins":     w,
                "win_rate": round(w / len(subset), 4),
            }

    # ── By symbol ─────────────────────────────────────────────────────────
    for sym in sorted(set(s["symbol"] for s in signals)):
        subset = [s for s in signals if s["symbol"] == sym]
        w = sum(1 for s in subset if s["outcome"] in ("tp2", "tp1"))
        agg["by_symbol"][hl_coin(sym)] = {
            "total":    len(subset),
            "wins":     w,
            "win_rate": round(w / len(subset), 4),
        }

    return agg


def print_results(agg: dict, days: int, symbols: list[str]) -> None:
    total = agg.get("total", 0)
    sep   = "=" * 58

    print(f"\n{sep}")
    print(f"  BACKTEST RESULTS  |  {days}d  |  {len(symbols)} symbol(s)")
    print(sep)

    if total == 0:
        print("  No signals found in this window.")
        print(sep)
        return

    wins  = agg["tp2"] + agg["tp1"]
    print(f"  Total signals  : {total}")
    print(f"  TP1 / TP2 wins : {wins}  ({agg['win_rate']*100:.1f}%)")
    print(f"  TP1 then SL    : {agg['tp1_then_sl']}  "
          f"({agg['tp1_then_sl']/total*100:.1f}%)")
    print(f"  SL losses      : {agg['sl']}  ({agg['loss_rate']*100:.1f}%)")
    print(f"  Still open     : {agg['open']}")

    if agg.get("by_type"):
        print(f"\n  By signal type:")
        for sig_type, data in sorted(agg["by_type"].items()):
            print(f"    {sig_type:6s} : {data['total']:4d} signals  "
                  f"{data['win_rate']*100:.1f}% WR")

    if agg.get("by_direction"):
        print(f"\n  By direction:")
        for dirn, data in sorted(agg["by_direction"].items()):
            print(f"    {dirn:6s} : {data['total']:4d} signals  "
                  f"{data['win_rate']*100:.1f}% WR")

    if agg.get("by_score"):
        print(f"\n  By minimum final_score  (key question: is >=7 meaningfully better?):")
        for label, data in sorted(agg["by_score"].items(),
                                  key=lambda x: int(x[0].replace(">=", ""))):
            marker = "  ← HIGHSCORE_THRESHOLD" if label == ">=7" else ""
            print(f"    {label:5s} : {data['total']:4d} signals  "
                  f"{data['win_rate']*100:.1f}% WR{marker}")

    if agg.get("by_symbol"):
        print(f"\n  By symbol (sorted by signal count):")
        by_count = sorted(
            agg["by_symbol"].items(),
            key=lambda x: x[1]["total"],
            reverse=True,
        )
        for coin, data in by_count:
            print(f"    {coin:10s}: {data['total']:4d} signals  "
                  f"{data['win_rate']*100:.1f}% WR")

    print(sep)
    print()


# ══════════════════════════════════════════════════════════════════════════
# MAIN ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════

def run_backtest(
    symbols: list[str] | None = None,
    days: int = 30,
    output_file: str = "backtest_results.json",
    verbose: bool = False,
) -> dict:
    """
    Full backtest run.

    Parameters
    ----------
    symbols     : List of WATCHLIST symbols. None → ["BTCUSDT"] (safe default).
    days        : Calendar days of history to fetch and replay.
    output_file : Path to write JSON results (signals + aggregate stats).
    verbose     : Print each signal as it's found during replay.

    Returns
    -------
    Aggregate results dict.
    """
    if symbols is None:
        symbols = ["BTCUSDT"]

    end_ms       = int(time.time() * 1000)
    all_signals: list[dict] = []

    for symbol in symbols:
        coin = hl_coin(symbol)
        print(f"\n[BT] {coin} — fetching {days}d of candles...")

        try:
            c15 = fetch_backtest_candles(symbol, "15m", days, end_ms)
            c1h = fetch_backtest_candles(symbol, "1h",  days, end_ms)
            c4h = fetch_backtest_candles(symbol, "4h",  days, end_ms)
            c1d = fetch_backtest_candles(symbol, "1d",  days, end_ms)
        except Exception as exc:
            print(f"  [BT] Candle fetch failed for {coin}: {exc} — skipping")
            continue

        min_needed = WARMUP_BARS + OUTCOME_BARS + 1
        if len(c15) < min_needed:
            print(f"  [BT] Only {len(c15)} 15m bars for {coin} "
                  f"(need {min_needed}) — skipping")
            continue

        print(f"[BT] {coin} — replaying {len(c15)} bars "
              f"(~{len(c15)//96} days of 15m data)...")

        signals = replay_symbol(symbol, c15, c1h, c4h, c1d, verbose=verbose)
        print(f"  → {len(signals)} raw signal(s) found")

        # Evaluate outcomes and strip future_bars before serialisation
        for sig in signals:
            future = sig.pop("future_bars", [])
            sig["outcome"] = evaluate_outcome(
                direction   = sig["direction"],
                entry       = sig["entry"],
                tp1         = sig["tp1"],
                tp2         = sig["tp2"],
                sl          = sig["sl"],
                future_bars = future,
            )

        all_signals.extend(signals)

    agg = aggregate_results(all_signals)
    print_results(agg, days, symbols)

    # Save full signal log + aggregate stats to JSON
    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "engine_file":  "signal_engine.py",
        "days":         days,
        "symbols":      symbols,
        "warmup_bars":  WARMUP_BARS,
        "outcome_bars": OUTCOME_BARS,
        "aggregate":    agg,
        "signals":      all_signals,
    }
    Path(output_file).write_text(json.dumps(output, indent=2, default=str))
    print(f"[BT] Full results saved → {output_file}")

    return agg


# ══════════════════════════════════════════════════════════════════════════
# CLI
# ══════════════════════════════════════════════════════════════════════════

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Backtest harness for beaconcore signal_engine.py",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples
--------
  # Start here — BTC only, 30 days:
  python backtest_harness.py

  # Full watchlist, 90 days:
  python backtest_harness.py --days 90 --symbols all

  # Single symbol, verbose:
  python backtest_harness.py --days 60 --symbols BTCUSDT --verbose

  # Multiple symbols:
  python backtest_harness.py --days 60 --symbols BTCUSDT,ETHUSDT,SOLUSDT
        """,
    )
    p.add_argument(
        "--days", type=int, default=30,
        help="Calendar days of history to replay (default: 30)",
    )
    p.add_argument(
        "--symbols", type=str, default="BTCUSDT",
        help=(
            'Comma-separated symbols, or "all" for full WATCHLIST. '
            'Default: BTCUSDT'
        ),
    )
    p.add_argument(
        "--output", type=str, default="backtest_results.json",
        help="Output JSON file path (default: backtest_results.json)",
    )
    p.add_argument(
        "--verbose", action="store_true",
        help="Print each signal as it is found during replay",
    )
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()

    if args.symbols.lower() == "all":
        target_symbols = WATCHLIST
    else:
        target_symbols = [s.strip().upper() for s in args.symbols.split(",")]

    # Validate symbols against WATCHLIST
    invalid = [s for s in target_symbols if s not in WATCHLIST]
    if invalid:
        print(f"[BT] WARNING: these symbols are not in WATCHLIST and may "
              f"have no Hyperliquid data: {invalid}")

    run_backtest(
        symbols     = target_symbols,
        days        = args.days,
        output_file = args.output,
        verbose     = args.verbose,
    )
