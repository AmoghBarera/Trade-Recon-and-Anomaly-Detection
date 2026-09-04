from datetime import datetime
import math

def parse_timestamp(ts_str: str) -> datetime:
    if ts_str.endswith('Z'):
        ts_str = ts_str[:-1] + '+00:00'
    return datetime.fromisoformat(ts_str)

def is_within_tolerance(val1: float, val2: float, tolerance: float = 0.01, is_percentage: bool = False) -> tuple[bool, float]:
    actual_diff = abs(val1 - val2)
    if is_percentage:
        allowed_diff = abs(val1) * (tolerance / 100.0)
    else:
        allowed_diff = tolerance
    
    if allowed_diff == 0:
        breach_factor = float('inf') if actual_diff > 0 else 0.0
    else:
        breach_factor = actual_diff / allowed_diff
        
    return actual_diff <= allowed_diff, breach_factor

def is_timestamp_within_drift(ts1: datetime, ts2: datetime, drift_ms: int = 100) -> tuple[bool, float]:
    diff_ms = abs((ts1 - ts2).total_seconds() * 1000)
    if drift_ms == 0:
        breach_factor = float('inf') if diff_ms > 0 else 0.0
    else:
        breach_factor = diff_ms / drift_ms
    return diff_ms <= drift_ms, breach_factor

def calculate_pnl_consistency(price: float, quantity: int, commission: float, pnl_impact: float, threshold: float = 1.0) -> bool:
    calculated_pnl = (price * quantity) - commission
    return abs(calculated_pnl - pnl_impact) < threshold
