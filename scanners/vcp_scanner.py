import pandas as pd
import numpy as np


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
# NEW: CONTRACTION COUNTING (P0)
# =========================================================


def count_contractions(symbol_df, lookback=120, smoothing=5):
    """
    Count successive volatility contractions in the base.

    A contraction is defined as a swing where ATR drops to a trough
    that is lower than the previous trough — meaning each contraction
    is tighter than the last (the hallmark of a true VCP).

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

    # Find local troughs (ATR valleys) using a simple min-of-window approach
    half_w = max(3, smoothing)
    troughs = []

    for i in range(half_w, n - half_w):
        window = atr_smooth[max(0, i - half_w):min(n, i + half_w + 1)]
        if atr_smooth[i] == window.min():
            troughs.append(atr_smooth[i])

    if len(troughs) < 2:
        return 0, troughs

    # Count how many successive troughs are lower than the previous one
    contractions = 0
    for j in range(1, len(troughs)):
        if troughs[j] < troughs[j - 1]:
            contractions += 1

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
# NEW: BREAKOUT DETECTION (P0)
# =========================================================


def detect_breakout(symbol_df, consolidation_window=20, vol_surge_factor=1.4):
    """
    Detect if a VCP candidate is breaking out of its consolidation.

    Breakout conditions:
    1. Today's close is above the highest close in the consolidation
       period (excluding today) — the "pivot point"
    2. Today's volume is above average (institutional accumulation)

    Args:
        symbol_df:             DataFrame with close, volume, high columns.
        consolidation_window:  Number of bars to define the consolidation.
        vol_surge_factor:      Volume must be this × average to confirm.

    Returns:
        dict with:
        - is_breakout:     bool — whether a breakout is detected
        - pivot_price:     float — the resistance level that was broken
        - breakout_volume: float — ratio of today's volume vs average
    """
    required_cols = ["close", "volume", "high"]

    for col in required_cols:
        if col not in symbol_df.columns:
            raise ValueError(f"Missing required column: {col}")

    if len(symbol_df) < consolidation_window + 1:
        return {
            "is_breakout": False,
            "pivot_price": None,
            "breakout_volume": None
        }

    latest = symbol_df.iloc[-1]
    consolidation = symbol_df.iloc[-(consolidation_window + 1):-1]

    # Pivot = highest close in the consolidation range
    pivot_price = consolidation["high"].max()

    # Average volume during consolidation (the quiet period)
    avg_vol = consolidation["volume"].mean()

    # Breakout conditions
    price_break = latest["close"] > pivot_price
    vol_ratio = latest["volume"] / avg_vol if avg_vol > 0 else 0
    vol_surge = vol_ratio >= vol_surge_factor

    return {
        "is_breakout": bool(price_break and vol_surge),
        "pivot_price": round(float(pivot_price), 2),
        "breakout_volume": round(float(vol_ratio), 2)
    }



# =========================================================
# NEW: BASE STRUCTURE DETECTION (P1)
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

    return True


def get_vcp_details(df: pd.DataFrame) -> dict:
    """
    Get detailed VCP analysis for a candidate stock.
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

    # Full VCP candidate = passes all 4 original gates
    details["vcp_candidate"] = (
        details["trend_pass"] and
        details["volatility_pass"] and
        details["tightness_pass"] and
        details["volume_pass"]
    )

    # Base structure detection
    if details["trend_pass"]:
        base_info = detect_base(df)
        details["is_valid_base"] = base_info["is_valid_base"]
        details["base_depth_pct"] = base_info["base_depth_pct"]
        details["base_length_days"] = base_info["base_length_days"]

    # Contraction counting (works for any stock in uptrend)
    if details["trend_pass"] and "atr_14" in df.columns:
        count, troughs = count_contractions(df)
        details["contraction_count"] = count

    # Breakout detection (only for VCP candidates)
    if details["vcp_candidate"]:
        breakout = detect_breakout(df)
        details["is_breakout"] = breakout["is_breakout"]
        details["pivot_price"] = breakout["pivot_price"]
        details["breakout_volume"] = breakout["breakout_volume"]

    # Quality classification
    if details["vcp_candidate"]:
        cc = details["contraction_count"]
        # If the base is too deep (>35%), it limits quality
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


def scan_universe(indicator_df: pd.DataFrame) -> pd.DataFrame:
    results = []

    for symbol, symbol_df in indicator_df.groupby("symbol"):
        symbol_df = symbol_df.sort_values("date")

        try:
            details = get_vcp_details(symbol_df)
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

        results.append({
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
        })

    return pd.DataFrame(results)