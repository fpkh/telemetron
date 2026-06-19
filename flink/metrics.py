"""
Pure per-minute aggregation logic — no PyFlink dependency, so it can be unit-tested
(see tests/test_metrics.py).
"""

import statistics
from datetime import datetime, timezone


def iso_to_epoch_ms(s: str) -> int:
    """'2026-06-19T03:25:12.413Z' -> epoch ms (UTC)."""
    dt = datetime.strptime(s, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def window_time_label(window_start_ms: int) -> str:
    """Window start (epoch ms) -> 'hh:mm' in the local system timezone.

    The epoch ms is an absolute instant; we render it in local time so the
    dashboard labels match the wall clock of whoever is running this.
    """
    return datetime.fromtimestamp(window_start_ms / 1000).strftime("%H:%M")


def compute_window_metrics(type_name, latencies, tool_calls, window_start_ms) -> dict:
    """
    Average latency_ms and exact median tool_calls_count over a window.
    Returns a ready-to-serialize dict for the output aggregate.
    """
    avg_latency = round(sum(latencies) / len(latencies), 1)
    median_tc = float(statistics.median(tool_calls))
    return {
        "time": window_time_label(window_start_ms),
        "type_name": type_name,
        "avg_latency_ms": avg_latency,
        "median_tool_calls_count": median_tc,
    }
