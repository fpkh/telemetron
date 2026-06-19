"""
Unit tests for the pure aggregation logic (flink/metrics.py).
Runs without PyFlink: `python tests/test_metrics.py`
"""

import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "flink"))

from metrics import compute_window_metrics, iso_to_epoch_ms, window_time_label


def local_label(iso: str) -> str:
    """Expected 'hh:mm' label for an instant, in the local timezone — so these
    tests pass regardless of the machine's timezone."""
    return datetime.fromtimestamp(iso_to_epoch_ms(iso) / 1000).strftime("%H:%M")


def test_iso_roundtrip():
    ms = iso_to_epoch_ms("2026-06-19T03:25:12.413Z")
    assert window_time_label(ms) == local_label("2026-06-19T03:25:12.413Z")


def test_window_time_truncates_to_minute():
    # any instant within the same minute maps to the same label
    a = iso_to_epoch_ms("2026-06-19T03:25:00.000Z")
    b = iso_to_epoch_ms("2026-06-19T03:25:59.999Z")
    assert window_time_label(a) == window_time_label(b)


def test_avg_and_median_odd():
    start = iso_to_epoch_ms("2026-06-19T03:25:00.000Z")
    out = compute_window_metrics("RAG answer", [1000, 2000, 3000], [1, 5, 2], start)
    assert out["time"] == local_label("2026-06-19T03:25:00.000Z")
    assert out["type_name"] == "RAG answer"
    assert out["avg_latency_ms"] == 2000.0
    assert out["median_tool_calls_count"] == 2.0  # median of [1,2,5]


def test_median_even_is_mean_of_middle():
    start = iso_to_epoch_ms("2026-06-19T10:00:00.000Z")
    out = compute_window_metrics("Code assistant", [1500, 2500], [2, 4], start)
    assert out["avg_latency_ms"] == 2000.0
    assert out["median_tool_calls_count"] == 3.0  # (2+4)/2


def test_avg_rounded_one_decimal():
    start = iso_to_epoch_ms("2026-06-19T10:00:00.000Z")
    out = compute_window_metrics("Email assistant", [1000, 1001], [0, 1], start)
    assert out["avg_latency_ms"] == 1000.5


def run():
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for t in tests:
        t()
        print(f"  ok: {t.__name__}")
    print(f"\nPASSED {len(tests)} tests")


if __name__ == "__main__":
    run()
