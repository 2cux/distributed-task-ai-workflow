# Management Benchmark report

Result: **PASS**; UTC timestamp: 2026-10-13T07:11:36.007136+00:00.

Environment: Python 3.12.10; SQLite 3.49.1; Windows-11-10.0.26200-SP0.

## Configuration

```json
{
  "workflows": 100,
  "reads": 300,
  "concurrency": 4,
  "workers": 2,
  "deadline": 60.0,
  "coordinator_poll_seconds": 0.1,
  "worker_poll_seconds": 0.05,
  "worker_heartbeat_seconds": 1,
  "repeat": 3,
  "warmup": 1
}
```

Each round uses a new temporary SQLite database, a loopback HTTP service and dedicated workers.
The workload submits root(abs) → join(identity) DAGs and interleaves overview, list and metrics reads.
Measured time includes worker startup, HTTP load and completion polling; service/DB setup and shutdown are excluded.
The deadline is the completion polling cutoff, not a hard timeout for the HTTP batch.

## Individual rounds

| Round | Result | Requests | HTTP errors | HTTP req/s | P95 ms | P99 ms | Completed workflows | Attempts | Results | DB |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |
| Warmup 1 | PASS | 400 | 0 | 38.52 | 153.749 | 637.074 | 100 | 200 | PASS | ok |
| Measured 1 | PASS | 400 | 0 | 37.66 | 189.362 | 492.568 | 100 | 200 | PASS | ok |
| Measured 2 | PASS | 400 | 0 | 35.73 | 176.865 | 542.117 | 100 | 200 | PASS | ok |
| Measured 3 | PASS | 400 | 0 | 24.01 | 257.52 | 1695.526 | 100 | 200 | PASS | ok |

## Statistics across measured rounds

Warmups are excluded. Failed measured rounds remain in the statistics and make the report FAIL.
Latency statistics below describe per-round percentiles; they are not pooled request percentiles.

| Metric | Min | Median | Max | Mean | Sample standard deviation |
| --- | ---: | ---: | ---: | ---: | ---: |
| http_requests_per_second | 24.01 | 35.73 | 37.66 | 32.467 | 7.387 |
| workflows_per_second | 5.73 | 8.4 | 9.05 | 7.727 | 1.759 |
| execution_seconds | 11.052 | 11.903 | 17.452 | 13.469 | 3.476 |
| http_p50_latency_ms | 85.341 | 92.014 | 108.193 | 95.183 | 11.751 |
| http_p95_latency_ms | 176.865 | 189.362 | 257.52 | 207.916 | 43.411 |
| http_p99_latency_ms | 492.568 | 542.117 | 1695.526 | 910.07 | 680.676 |
| http_maximum_latency_ms | 2530.877 | 4666.791 | 7008.318 | 4735.329 | 2239.507 |

Passed measured rounds: 3/3. Aggregate HTTP throughput: 31.19 req/s; workflow throughput: 7.42 workflows/s.

## Correctness checks

Every round checks expected HTTP statuses, completion before the cutoff, exact workflow/task counts,
both node results, exactly one attempt per task, zero retries, SQLite integrity and clean shutdown.

This is a local capacity sample, not a production SLO. It does not benchmark model inference,
cross-host networking, long-running soak, or fault recovery. Timings vary with machine load and disk.
