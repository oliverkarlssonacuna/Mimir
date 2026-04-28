"""Robust statistics for anomaly detection on volatile metrics.

Why robust stats (median + IQR + MAD) instead of mean + stddev:
- Volatile ratio metrics (e.g. matchmaking cancel ratio with low volume) naturally
  swing between extremes. The mean is dragged toward the spikes, making "normal
  range" useless.
- Tukey's fences (P25 - 1.5*IQR, P75 + 1.5*IQR) are the standard NIST-recommended
  way to identify true outliers vs normal noise.
- MAD (median absolute deviation) gives a robust spread measure that ignores spikes.
"""

from dataclasses import dataclass
from typing import Iterable


@dataclass
class MetricStats:
    n: int
    median: float
    p10: float
    p25: float
    p75: float
    p90: float
    iqr: float
    mad: float
    min_val: float
    max_val: float
    tukey_low: float       # mild outlier threshold (Q1 - 1.5*IQR)
    tukey_high: float      # mild outlier threshold (Q3 + 1.5*IQR)
    cv_robust: float       # MAD / |median| — relative spread
    volatility: str        # "low" | "medium" | "high"

    def classify(self, value: float) -> str:
        """Classify a value relative to the historical distribution.

        Returns one of:
          "typical"   — within IQR (P25..P75): the central 50% of historical values
          "elevated"  — between P75 and tukey_high (or P25 and tukey_low)
          "outlier"   — beyond Tukey's fence (NIST: a true mild outlier)
        """
        if value is None:
            return "unknown"
        if value > self.tukey_high or value < self.tukey_low:
            return "outlier"
        if value > self.p75 or value < self.p25:
            return "elevated"
        return "typical"


def _percentile(sorted_values: list[float], p: float) -> float:
    """Linear-interpolation percentile (NIST-equivalent, matches numpy's default)."""
    if not sorted_values:
        return 0.0
    if len(sorted_values) == 1:
        return sorted_values[0]
    k = (len(sorted_values) - 1) * p
    f = int(k)
    c = min(f + 1, len(sorted_values) - 1)
    if f == c:
        return sorted_values[f]
    return sorted_values[f] + (sorted_values[c] - sorted_values[f]) * (k - f)


def compute_stats(values: Iterable[float]) -> MetricStats | None:
    """Compute robust statistics over a numeric series.

    Returns None if there are fewer than 3 finite values — too little to be useful.
    """
    finite = [float(v) for v in values if v is not None and not _is_nan(v)]
    if len(finite) < 3:
        return None

    sorted_vals = sorted(finite)
    median = _percentile(sorted_vals, 0.5)
    p10 = _percentile(sorted_vals, 0.10)
    p25 = _percentile(sorted_vals, 0.25)
    p75 = _percentile(sorted_vals, 0.75)
    p90 = _percentile(sorted_vals, 0.90)
    iqr = p75 - p25

    abs_devs = sorted(abs(v - median) for v in finite)
    mad = _percentile(abs_devs, 0.5)

    tukey_low = p25 - 1.5 * iqr
    tukey_high = p75 + 1.5 * iqr

    if abs(median) > 1e-9:
        cv_robust = mad / abs(median)
    elif iqr > 1e-9:
        # If median is ~0, fall back to IQR / range
        rng = sorted_vals[-1] - sorted_vals[0]
        cv_robust = iqr / rng if rng > 1e-9 else 0.0
    else:
        cv_robust = 0.0

    if cv_robust >= 0.5:
        volatility = "high"
    elif cv_robust >= 0.2:
        volatility = "medium"
    else:
        volatility = "low"

    return MetricStats(
        n=len(finite),
        median=median,
        p10=p10,
        p25=p25,
        p75=p75,
        p90=p90,
        iqr=iqr,
        mad=mad,
        min_val=sorted_vals[0],
        max_val=sorted_vals[-1],
        tukey_low=tukey_low,
        tukey_high=tukey_high,
        cv_robust=cv_robust,
        volatility=volatility,
    )


def _is_nan(v) -> bool:
    try:
        return v != v  # NaN != NaN
    except Exception:
        return False
