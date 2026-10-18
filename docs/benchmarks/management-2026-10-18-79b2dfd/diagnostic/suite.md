# Management Benchmark report

Result: **PASS**; UTC timestamp: 2026-10-18T04:06:22.830306+00:00.

Environment: Python 3.12.10; SQLite 3.49.1; Windows-11-10.0.26200-SP0.

## Configuration

```json
{
  "workflows": 100,
  "reads": 300,
  "concurrency": 8,
  "workers": 4,
  "deadline": 60,
  "coordinator_poll_seconds": 0.1,
  "worker_poll_seconds": 0.05,
  "worker_heartbeat_seconds": 1,
  "repeat": 3,
  "warmup": 0
}
```

Each round uses a new temporary SQLite database, a loopback HTTP service and dedicated workers.
The workload submits root(abs) → join(identity) DAGs and interleaves overview, list and metrics reads.
Measured time includes worker startup, HTTP load and completion polling; service/DB setup and shutdown are excluded.
The deadline is the completion polling cutoff, not a hard timeout for the HTTP batch.

## Individual rounds

| Round | Result | Requests | HTTP errors | HTTP req/s | P95 ms | P99 ms | Completed workflows | Attempts | Results | DB |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |
| Measured 1 | PASS | 400 | 0 | 45.78 | 336.889 | 657.592 | 100 | 200 | PASS | ok |
| Measured 2 | PASS | 400 | 0 | 42.47 | 378.958 | 423.826 | 100 | 200 | PASS | ok |
| Measured 3 | PASS | 400 | 0 | 46.82 | 330.623 | 459.209 | 100 | 200 | PASS | ok |

## Statistics across measured rounds

Warmups are excluded. Failed measured rounds remain in the statistics and make the report FAIL.
Latency statistics below describe per-round percentiles; they are not pooled request percentiles.

| Metric | Min | Median | Max | Mean | Sample standard deviation |
| --- | ---: | ---: | ---: | ---: | ---: |
| http_requests_per_second | 42.47 | 45.78 | 46.82 | 45.023 | 2.272 |
| workflows_per_second | 9.94 | 10.7 | 11.08 | 10.573 | 0.58 |
| execution_seconds | 9.026 | 9.348 | 10.064 | 9.479 | 0.531 |
| http_p50_latency_ms | 143.662 | 154.33 | 169.117 | 155.703 | 12.783 |
| http_p95_latency_ms | 330.623 | 336.889 | 378.958 | 348.823 | 26.285 |
| http_p99_latency_ms | 423.826 | 459.209 | 657.592 | 513.542 | 125.999 |
| http_maximum_latency_ms | 1625.507 | 1924.789 | 2091.49 | 1880.595 | 236.114 |

Passed measured rounds: 3/3. Aggregate HTTP throughput: 44.95 req/s; workflow throughput: 10.55 workflows/s.

## Correctness checks

Every round checks expected HTTP statuses, completion before the cutoff, exact workflow/task counts,
both node results, exactly one attempt per task, zero retries, SQLite integrity and clean shutdown.

This is a local capacity sample, not a production SLO. It does not benchmark model inference,
cross-host networking, long-running soak, or fault recovery. Timings vary with machine load and disk.
