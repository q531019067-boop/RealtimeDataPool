"""Unit test for _LatencyTracker threshold detection."""
import sys
sys.path.insert(0, r"C:\Users\Administrator\Downloads\GitHub\RealtimeDataPool\src")
import logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

from rdp.fetcher import _LatencyTracker, P95_LATENCY_THRESHOLD_S

# 1) Mixed latencies: 18 fast + 2 slow → p95 should hit the slow ones
t = _LatencyTracker(window=20)
for i in range(20):
    t.add(0.1 if i < 18 else 3.5)
print(f"[1] p95 (18x0.1 + 2x3.5): {t.p95():.2f}s  expect ~3.5")

# 2) All fast
t2 = _LatencyTracker(window=20)
for _ in range(20):
    t2.add(0.05)
print(f"[2] p95 (20x0.05):       {t2.p95():.2f}s  expect 0.05")

# 3) Too few samples → should return 0.0
t3 = _LatencyTracker(window=100)
for _ in range(5):
    t3.add(5.0)
print(f"[3] p95 (5 samples):     {t3.p95():.2f}s  expect 0.0 (insufficient data)")

# 4) At threshold boundary
t4 = _LatencyTracker(window=20)
for _ in range(18):
    t4.add(1.0)
t4.add(2.0)
t4.add(2.0)
print(f"[4] p95 (18x1.0+2x2.0):  {t4.p95():.2f}s  expect ~2.0  threshold={P95_LATENCY_THRESHOLD_S}s")

# 5) Sliding window: oldest samples should drop out
t5 = _LatencyTracker(window=10)
for _ in range(10):
    t5.add(5.0)  # all slow
print(f"[5a] p95 (10x5.0):       {t5.p95():.2f}s  expect 5.0")
for _ in range(10):
    t5.add(0.05)  # fill with fast — slow ones should drop
print(f"[5b] p95 (after evict):  {t5.p95():.2f}s  expect 0.05 (slow evicted)")

print("--- _LatencyTracker OK")
