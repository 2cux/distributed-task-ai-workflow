# AI workflow load regression

Result: **PASS**

UTC: 2026-10-17T05:19:19.733722+00:00; environment: `{"python": "3.12.10", "sqlite": "3.49.1", "platform": "Windows-11-10.0.26200-SP0", "machine": "LAPTOP-598DOPCF"}`

Closed-loop clients: one workflow in flight per client; duration controls admission, then bounded drain.
Latency quantiles are upper bounds from fixed-memory 1% logarithmic histograms; failures are included.

| Phase | Round | Clients | Workflows | WF/s | HTTP failure % | WF failure % | HTTP P95/P99 ms | WF P95/P99 ms | Writer wait P95 ms | SQLite BEGIN P95 ms | Busy | Result |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- | ---: | ---: | ---: | --- |
| Warmup | 1 | 1 | 36 | 3.54476 | 0.000 | 0.000 | 25.321/28.818 | 346.738/350.988 | 3.393 | 0.074 | 0 | PASS |
| Warmup | 1 | 4 | 142 | 13.808757 | 0.000 | 0.000 | 29.691/44.648 | 368.069/453.605 | 11.652 | 0.17 | 0 | PASS |
| Warmup | 1 | 8 | 172 | 16.664653 | 0.000 | 0.000 | 62.002/86.962 | 716.905/799.828 | 63.88 | 0.437 | 0 | PASS |
| Warmup | 2 | 4 | 132 | 13.011301 | 0.000 | 0.000 | 34.47/47.868 | 398.566/453.605 | 10.341 | 0.154 | 0 | PASS |
| Warmup | 2 | 8 | 150 | 14.590955 | 0.000 | 0.000 | 76.41/129.475 | 784.068/919.382 | 85.249 | 0.67 | 0 | PASS |
| Warmup | 2 | 1 | 37 | 3.591271 | 0.000 | 0.000 | 32.472/42.905 | 353.707/363.315 | 3.531 | 0.093 | 0 | PASS |
| Measured | 1 | 1 | 661 | 3.669532 | 0.000 | 0.000 | 26.349/32.151 | 353.707/371.75 | 3.427 | 0.092 | 0 | PASS |
| Measured | 1 | 4 | 2429 | 13.474361 | 0.000 | 0.000 | 34.129/47.394 | 383.014/402.552 | 9.742 | 0.184 | 0 | PASS |
| Measured | 1 | 8 | 3338 | 18.519013 | 0.000 | 0.000 | 53.939/80.308 | 599.345/682.11 | 50.813 | 0.441 | 0 | PASS |
| Measured | 2 | 4 | 2340 | 12.984258 | 0.000 | 0.000 | 38.076/53.405 | 402.552/435.906 | 15.093 | 0.211 | 0 | PASS |
| Measured | 2 | 8 | 3722 | 20.649685 | 0.000 | 0.000 | 46.925/76.41 | 531.888/892.343 | 37.326 | 0.344 | 0 | PASS |
| Measured | 2 | 1 | 652 | 3.6221 | 0.000 | 0.000 | 27.97/34.47 | 353.707/379.222 | 3.26 | 0.088 | 0 | PASS |

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
