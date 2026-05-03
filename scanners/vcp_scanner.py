import pandas as pd
import numpy as np
from concurrent.futures import ThreadPoolExecutor


def trend_gate(symbol_df):
    required_cols = ["close", "sma_50", "sma_200", "reg_slope_100"]

    for col in required_cols:
        if col not in symbol_df.columns:
            raise ValueError(f"Missing required column: {col}")

    latest = symbol_df.iloc[-1]

    # IMAGE SCANNER STYLE TREND
    if not (
        latest["close"] > latest["sma_50"] and
        latest["sma_50"] > latest["sma_200"]
    ):
        return False

    # Allow slightly negative but flat slope
    if latest["reg_slope_100"] < -0.001:
        return False

    # Price must be reasonably close to 52-week high
    if len(symbol_df) >= 252:
        recent_high = symbol_df["close"].rolling(252).max().iloc[-1]
        if latest["close"] < 0.75 * recent_high:
            return False

    return True


def volatility_contraction_gate(symbol_df):
    required_cols = ["atr_14", "close"]

    for col in required_cols:
        if col not in symbol_df.columns:
            raise ValueError(f"Missing required column: {col}")

    if len(symbol_df) < 10:
        return False

    latest = symbol_df.iloc[-1]

    # ATR contraction vs 10 days ago (image logic)
    atr_10_days_ago = symbol_df["atr_14"].iloc[-10]
    if latest["atr_14"] >= atr_10_days_ago:
        return False

    # ATR normalized by price
    if (latest["atr_14"] / latest["close"]) > 0.08:
        return False

    return True


def price_tightness_gate(symbol_df, lookback=15):
    required_cols = ["close", "high", "low"]

    for col in required_cols:
        if col not in symbol_df.columns:
            raise ValueError(f"Missing required column: {col}")

    if len(symbol_df) < lookback:
        return False

    recent = symbol_df.iloc[-lookback:]

    # Slightly loosened tightness
    close_range_pct = (
        recent["close"].max() - recent["close"].min()
    ) / recent["close"].iloc[-1]

    if close_range_pct > 0.08:
        return False

    # Allow occasional wide day
    daily_range_pct = (
        (recent["high"] - recent["low"]) / recent["close"]
    )

    if (daily_range_pct > 0.05).sum() > 2:
        return False

    return True


def volume_dryup_gate(
    symbol_df,
    recent_window=15,
    prior_window=30,
    max_vol_expansion=1.5,
    max_distribution_ratio=0.40
):
    required_cols = ["volume", "close"]

    for col in required_cols:
        if col not in symbol_df.columns:
            raise ValueError(f"Missing required column: {col}")

    if len(symbol_df) < recent_window + prior_window:
        return True  # IMPORTANT: do NOT kill early

    recent = symbol_df.iloc[-recent_window:]
    prior = symbol_df.iloc[-(recent_window + prior_window):-recent_window]

    avg_vol_recent = recent["volume"].mean()
    avg_vol_prior = prior["volume"].mean()

    # Allow volume expansion (no hard dry-up)
    if avg_vol_recent > max_vol_expansion * avg_vol_prior:
        return False

    # Distribution days check (soft)
    prev_close = recent["close"].shift(1)
    down_days = recent["close"] < prev_close
    high_volume = recent["volume"] > (1.5 * avg_vol_recent)

    distribution_days = (down_days & high_volume).sum()
    distribution_ratio = distribution_days / recent_window

    if distribution_ratio > max_distribution_ratio:
        return False

    return True



# =========================================================
# CONTRACTION COUNTING — vectorised trough detection
# =========================================================


def count_contractions(symbol_df, lookback=120, smoothing=5):
    """
    Count successive volatility contractions in the base.

    A contraction is defined as a swing where ATR drops to a trough
    that is lower than the previous trough — meaning each contraction
    is tighter than the last (the hallmark of a true VCP).

    Uses a vectorised rolling-min approach for trough detection instead
    of the previous O(lookback × half_w) Python loop.

    Args:
        symbol_df:  DataFrame with 'atr_14' column, sorted by date.
        lookback:   Number of bars to analyze for the base pattern.
        smoothing:  Window for smoothing ATR to reduce noise.

    Returns:
        (contraction_count, trough_values)
        contraction_count = number of successive tighter contractions
        trough_values     = list of ATR trough values found
    """
    if len(symbol_df) < lookback:
        return 0, []

    # Use smoothed ATR to reduce noise
    atr_series = symbol_df["atr_14"].iloc[-lookback:]
    atr_smooth = atr_series.rolling(smoothing, min_periods=1).mean().values
    n = len(atr_smooth)

    half_w = max(3, smoothing)

    if n <= 2 * half_w:
        return 0, []

    # Vectorised trough detection via centred rolling min
    atr_pd = pd.Series(atr_smooth)
    win_size = 2 * half_w + 1
    rolling_min = atr_pd.rolling(win_size, center=True, min_periods=1).min()

    # A point is a trough if it equals the rolling min in its window
    # and falls within the valid range [half_w, n - half_w)
    is_trough = np.zeros(n, dtype=bool)
    valid = slice(half_w, n - half_w)
    is_trough[valid] = atr_smooth[valid] == rolling_min.values[valid]

    troughs = atr_smooth[is_trough].tolist()

    if len(troughs) < 2:
        return 0, troughs

    # Count how many successive troughs are lower than the previous one
    contractions = sum(
        1 for j in range(1, len(troughs)) if troughs[j] < troughs[j - 1]
    )

    return contractions, troughs


def contraction_count_gate(symbol_df, min_contractions=2):
    """
    Gate: require at least min_contractions successive tighter
    volatility contractions (the core VCP signal).
    """
    if "atr_14" not in symbol_df.columns:
        raise ValueError("Missing required column: atr_14")

    count, _ = count_contractions(symbol_df)
    return count >= min_contractions



# =========================================================
# BREAKOUT DETECTION (P0)
# =========================================================


def compute_pivot_price(symbol_df, consolidation_window=20) -> float:
    """
    Compute the pivot price at the moment a VCP is first confirmed.

    The pivot is the highest high inside the consolidation window
    (excluding the current bar).  This value is persisted in
    ``vcp_pivot_anchors`` and must NOT change on future pipeline runs.

    Args:
        symbol_df:            DataFrame sorted by date.
        consolidation_window: Number of bars to look back for the base.

    Returns:
        float — the pivot (resistance) price.
    """
    if len(symbol_df) < consolidation_window + 1:
        # Not enough data — use all available bars
        return float(symbol_df["high"].max())

    consolidation = symbol_df.iloc[-(consolidation_window + 1):-1]
    return float(consolidation["high"].max())


def detect_breakout(symbol_df, anchored_pivot: float, vol_surge_factor: float = 1.4) -> dict:
    """
    Detect if a VCP candidate is breaking out of its consolidation.

    Evaluates today's close against a FIXED, caller-supplied pivot price.
    The pivot must be sourced from ``vcp_pivot_anchors`` (set on the day
    the stock first qualified as a VCP candidate) so it never drifts with
    the sliding consolidation window.

    Breakout conditions:
    1. Today's close > anchored_pivot  (price breakout)
    2. Today's volume > vol_surge_factor × consolidation avg  (volume confirmation)

    Args:
        symbol_df:      DataFrame with close, volume columns, sorted by date.
        anchored_pivot: The fixed pivot price established on confirmation day.
        vol_surge_factor: Volume ratio threshold for institutional confirmation.

    Returns:
        dict with:
        - is_breakout:     bool
        - pivot_price:     float (the anchored pivot, echoed back)
        - breakout_volume: float (ratio of today's vol vs avg)
    """
    required_cols = ["close", "volume"]
    for col in required_cols:
        if col not in symbol_df.columns:
            raise ValueError(f"Missing required column: {col}")

    if len(symbol_df) < 21:
        return {
            "is_breakout": False,
            "pivot_price": round(float(anchored_pivot), 2),
            "breakout_volume": None,
        }

    latest = symbol_df.iloc[-1]
    # Use the same 20-bar window for volume baseline
    consolidation = symbol_df.iloc[-21:-1]
    avg_vol = consolidation["volume"].mean()

    price_break = latest["close"] > anchored_pivot
    vol_ratio = latest["volume"] / avg_vol if avg_vol > 0 else 0
    vol_surge = vol_ratio >= vol_surge_factor

    return {
        "is_breakout": bool(price_break and vol_surge),
        "pivot_price": round(float(anchored_pivot), 2),
        "breakout_volume": round(float(vol_ratio), 2),
    }


# =========================================================
# BASE STRUCTURE DETECTION (P1)
# =========================================================

def detect_base(symbol_df, min_weeks=3, max_depth_pct=0.35):
    """
    Identify if the stock is forming a proper consolidation base.
    
    A proper base allows the stock to digest previous gains. 
    It shouldn't be too deep (typically < 35% drawdown) or too short.
    
    Returns:
        dict with:
        - is_valid_base: bool — passes depth and length criteria
        - base_depth_pct: float — peak to trough drawdown in the base
        - base_length_days: int — length of the base
    """
    if len(symbol_df) < 20: 
        return {
            "is_valid_base": False,
            "base_depth_pct": 0.0,
            "base_length_days": 0
        }
        
    close = symbol_df["close"].values
    
    # Find the peak that starts the base (look back up to 1 year)
    lookback = min(252, len(close))
    recent_closes = close[-lookback:]
    
    high_val = recent_closes.max()
    peak_idx = len(close) - lookback + recent_closes.argmax()
    
    # Calculate depth from that peak
    if peak_idx >= len(close) - 1:
        # Peak is today, no base formed yet
        return {
            "is_valid_base": False,
            "base_depth_pct": 0.0,
            "base_length_days": 0
        }
        
    trough = close[peak_idx:].min()
    depth_pct = (high_val - trough) / high_val if high_val > 0 else 0
    
    # Calculate length from peak
    base_length_days = len(close) - peak_idx
    base_length_weeks = base_length_days / 5
    
    is_valid = bool(depth_pct <= max_depth_pct and base_length_weeks >= min_weeks)
    
    return {
        "is_valid_base": is_valid,
        "base_depth_pct": round(float(depth_pct), 3),
        "base_length_days": int(base_length_days)
    }


# =========================================================
# VCP CANDIDATE CHECK (UPDATED)
# =========================================================


def is_vcp_candidate(df: pd.DataFrame) -> bool:
    if not trend_gate(df):
        return False

    if not volatility_contraction_gate(df):
        return False

    if not price_tightness_gate(df):
        return False

    # Volume is now a SOFT filter
    if not volume_dryup_gate(df):
        return False

    # Require at least 1 volatility contraction to be a true VCP
    if "atr_14" in df.columns:
        count, _ = count_contractions(df)
        if count < 1:
            return False

    return True


def get_vcp_details(df: pd.DataFrame, anchored_pivot: float | None = None) -> dict:
    """
    Get detailed VCP analysis for a candidate stock.

    Args:
        df:             OHLCV + indicator DataFrame sorted by date.
        anchored_pivot: If provided (from ``vcp_pivot_anchors``), breakout is
                        evaluated against this fixed price.  For brand-new
                        candidates the caller passes None and the pivot is
                        computed fresh from the current consolidation window.

    Returns gate results, contraction info, breakout status, and base structure.
    """
    details = {
        "trend_pass": False,
        "volatility_pass": False,
        "tightness_pass": False,
        "volume_pass": False,
        "vcp_candidate": False,
        "contraction_count": 0,
        "is_breakout": False,
        "pivot_price": None,
        "breakout_volume": None,
        "is_valid_base": False,
        "base_depth_pct": 0.0,
        "base_length_days": 0,
        "vcp_quality": "none"
    }

    try:
        details["trend_pass"] = trend_gate(df)
    except Exception:
        return details

    if not details["trend_pass"]:
        return details

    try:
        details["volatility_pass"] = volatility_contraction_gate(df)
        details["tightness_pass"] = price_tightness_gate(df)
        details["volume_pass"] = volume_dryup_gate(df)
    except Exception:
        return details

    # Contraction counting (needed for candidate check)
    if "atr_14" in df.columns:
        count, troughs = count_contractions(df)
        details["contraction_count"] = count

    # Full VCP candidate = passes all 4 original gates + at least 1 contraction
    details["vcp_candidate"] = (
        details["trend_pass"] and
        details["volatility_pass"] and
        details["tightness_pass"] and
        details["volume_pass"] and
        details["contraction_count"] >= 1
    )

    # Only compute expensive analysis for actual candidates
    if details["vcp_candidate"]:
        # Base structure detection
        base_info = detect_base(df)
        details["is_valid_base"] = base_info["is_valid_base"]
        details["base_depth_pct"] = base_info["base_depth_pct"]
        details["base_length_days"] = base_info["base_length_days"]

        # Breakout detection against the anchored (fixed) pivot.
        # For a brand-new candidate anchored_pivot is None — compute it fresh
        # from the current consolidation window and it will be persisted.
        pivot = anchored_pivot if anchored_pivot is not None else compute_pivot_price(df)
        breakout = detect_breakout(df, anchored_pivot=pivot)
        details["is_breakout"]     = breakout["is_breakout"]
        details["pivot_price"]     = breakout["pivot_price"]
        details["breakout_volume"] = breakout["breakout_volume"]

        # Quality classification
        cc = details["contraction_count"]
        if not details["is_valid_base"]:
            details["vcp_quality"] = "emerging"
        elif details["is_breakout"]:
            details["vcp_quality"] = "trigger"      # ACTIONABLE: breaking out now
        elif cc >= 3:
            details["vcp_quality"] = "textbook"     # Classic 3+ contraction VCP
        elif cc >= 2:
            details["vcp_quality"] = "strong"       # Solid 2-contraction VCP
        else:
            details["vcp_quality"] = "emerging"     # Passes gates but fewer contractions

    return details



# =========================================================
# UNIVERSE SCANNER (UPDATED)
# =========================================================


def scan_universe(
    indicator_df,
    existing_anchors: dict | None = None,
    max_workers: int = 8,
) -> pd.DataFrame:
    """
    Scan every symbol and return a one-row-per-symbol VCP results DataFrame.

    Args:
        indicator_df:     Grouped DataFrame / dict / iterable of (symbol, df) pairs.
        existing_anchors: Mapping of {symbol: pivot_price} loaded from
                          ``vcp_pivot_anchors``.  When a symbol is present
                          its stored pivot is passed to ``get_vcp_details``
                          so ``is_breakout`` is evaluated against the fixed
                          entry price, not the current sliding window.
        max_workers:      Thread-pool size.
    """
    if isinstance(indicator_df, pd.DataFrame):
        groups = list(indicator_df.groupby("symbol"))
    elif isinstance(indicator_df, dict):
        groups = list(indicator_df.items())
    else:
        groups = list(indicator_df)

    anchors = existing_anchors or {}

    def _process_symbol(args):
        symbol, symbol_df = args
        symbol_df = symbol_df.sort_values("date")
        anchored_pivot = anchors.get(symbol)  # None for new candidates
        try:
            details = get_vcp_details(symbol_df, anchored_pivot=anchored_pivot)
        except Exception:
            details = {
                "vcp_candidate": False,
                "contraction_count": 0,
                "is_breakout": False,
                "pivot_price": None,
                "breakout_volume": None,
                "is_valid_base": False,
                "base_depth_pct": 0.0,
                "base_length_days": 0,
                "vcp_quality": "none"
            }

        return {
            "symbol": symbol,
            "vcp_candidate": details["vcp_candidate"],
            "contraction_count": details.get("contraction_count", 0),
            "is_breakout": details.get("is_breakout", False),
            "pivot_price": details.get("pivot_price", None),
            "breakout_volume": details.get("breakout_volume", None),
            "is_valid_base": details.get("is_valid_base", False),
            "base_depth_pct": details.get("base_depth_pct", 0.0),
            "base_length_days": details.get("base_length_days", 0),
            "vcp_quality": details.get("vcp_quality", "none")
        }

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        results = list(executor.map(_process_symbol, groups))

    return pd.DataFrame(results)
