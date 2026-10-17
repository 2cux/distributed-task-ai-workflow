# AI workflow load regression

Result: **PASS**

UTC: 2026-10-17T04:35:00.619780+00:00; environment: `{"python": "3.12.10", "sqlite": "3.49.1", "platform": "Windows-11-10.0.26200-SP0", "machine": "LAPTOP-598DOPCF"}`

Closed-loop clients: one workflow in flight per client; duration controls admission, then bounded drain.
Latency quantiles are upper bounds from fixed-memory 1% logarithmic histograms; failures are included.

| Phase | Round | Clients | Workflows | WF/s | HTTP failure % | WF failure % | HTTP P95/P99 ms | WF P95/P99 ms | Writer wait P95 ms | SQLite BEGIN P95 ms | Busy | Result |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- | ---: | ---: | ---: | --- |
| Warmup | 1 | 1 | 32 | 3.149749 | 0.000 | 0.000 | 37.326/47.394 | 386.844/391.202 | 16.021 | 0.089 | 0 | PASS |
| Warmup | 1 | 4 | 70 | 6.801863 | 0.000 | 0.000 | 79.513/137.44 | 840.627/950.291 | 110.419 | 14.218 | 0 | PASS |
| Warmup | 1 | 8 | 146 | 14.191296 | 0.000 | 0.000 | 62.622/130.77 | 874.76/1046.342 | 77.946 | 7.672 | 0 | PASS |
| Warmup | 2 | 4 | 111 | 10.723746 | 0.000 | 0.000 | 48.347/77.946 | 516.245/956.413 | 35.515 | 0.433 | 0 | PASS |
| Warmup | 2 | 8 | 132 | 12.691488 | 0.000 | 0.000 | 74.163/148.828 | 937.861/1155.813 | 94.168 | 9.839 | 0 | PASS |
| Warmup | 2 | 1 | 31 | 2.991889 | 0.000 | 0.000 | 41.644/51.321 | 472.024/585.954 | 19.164 | 0.077 | 0 | PASS |
| Measured | 1 | 1 | 563 | 3.125523 | 0.000 | 0.000 | 38.076/47.868 | 410.643/444.668 | 14.36 | 0.059 | 0 | PASS |
| Measured | 1 | 4 | 1892 | 10.496196 | 0.000 | 0.000 | 47.868/74.905 | 526.622/593.411 | 41.231 | 1.345 | 0 | PASS |
| Measured | 1 | 8 | 2625 | 14.559255 | 0.000 | 0.000 | 60.78/105.06 | 892.343/1453.045 | 79.513 | 7.984 | 0 | PASS |
| Measured | 2 | 4 | 1966 | 10.901937 | 0.000 | 0.000 | 46.925/72.702 | 496.102/593.411 | 38.842 | 0.697 | 0 | PASS |
| Measured | 2 | 8 | 2666 | 14.780646 | 0.000 | 0.000 | 58.992/108.243 | 746.014/1167.371 | 76.41 | 7.905 | 0 | PASS |
| Measured | 2 | 1 | 568 | 3.149628 | 0.000 | 0.000 | 36.228/45.094 | 402.552/418.897 | 12.369 | 0.047 | 0 | PASS |

Configuration:

```json
{
  "concurrencies": [
    1,
    4,
    8
  ],
  "workers": 4,
  "duration": 180,
  "repeat": 2,
  "warmup": 10,
  "window": 30,
  "drain_timeout": 60,
  "request_timeout": 10,
  "provider_delay_ms": 25
}
```

Gate limits:

```json
{
  "http_failure_rate": 0,
  "workflow_failure_rate": 0,
  "http_p95_ms": 2000,
  "workflow_p95_ms": 10000,
  "writer_wait_p95_ms": 1000,
  "sqlite_begin_p95_ms": 500,
  "busy_errors": 0,
  "min_workflows_per_second": 0.1,
  "min_throughput_ratio": 0.75,
  "max_latency_ratio": 1.3
}
```

Failure reasons: none

Per-window evidence, endpoint distributions, exact persisted node/attempt/event checks and provider call counts are in JSON.
SQLite BEGIN time includes lock acquisition and statement overhead; writer wait includes the local FIFO queue.
A zero busy-error count does not imply zero contention. Setup PRAGMAs, cursor fetching and other processes are not instrumented.
This measures the local AI orchestration path with a deterministic HTTP provider; it does not measure production model inference.
