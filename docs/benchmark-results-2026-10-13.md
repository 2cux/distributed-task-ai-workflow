# Management Benchmark report

Result: **FAIL**; UTC timestamp: 2026-10-13T07:09:55.373746+00:00.

Environment: Python 3.12.10; SQLite 3.49.1; Windows-11-10.0.26200-SP0.

## Configuration

```json
{
  "workflows": 100,
  "reads": 300,
  "concurrency": 8,
  "workers": 4,
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
| Warmup 1 | PASS | 400 | 0 | 35.28 | 250.208 | 4189.503 | 100 | 200 | PASS | ok |
| Measured 1 | FAIL | 400 | 1 | 26.07 | 403.378 | 4831.6 | 100 | 200 | PASS | ok |
| Measured 2 | FAIL | 400 | 2 | 34.91 | 305.682 | 2105.241 | 100 | 200 | PASS | ok |
| Measured 3 | FAIL | 400 | 1 | 31.0 | 657.491 | 3416.727 | 100 | 200 | PASS | ok |

## Statistics across measured rounds

Warmups are excluded. Failed measured rounds remain in the statistics and make the report FAIL.
Latency statistics below describe per-round percentiles; they are not pooled request percentiles.

| Metric | Min | Median | Max | Mean | Sample standard deviation |
| --- | ---: | ---: | ---: | ---: | ---: |
| http_requests_per_second | 26.07 | 31.0 | 34.91 | 30.66 | 4.43 |
| workflows_per_second | 6.39 | 7.49 | 8.06 | 7.313 | 0.849 |
| execution_seconds | 12.411 | 13.352 | 15.662 | 13.808 | 1.673 |
| http_p50_latency_ms | 91.975 | 93.253 | 157.381 | 114.203 | 37.399 |
| http_p95_latency_ms | 305.682 | 403.378 | 657.491 | 455.517 | 181.607 |
| http_p99_latency_ms | 2105.241 | 3416.727 | 4831.6 | 3451.189 | 1363.506 |
| http_maximum_latency_ms | 10032.574 | 10050.241 | 10158.899 | 10080.571 | 68.406 |

Passed measured rounds: 0/3. Aggregate HTTP throughput: 30.22 req/s; workflow throughput: 7.24 workflows/s.

## Correctness checks

Every round checks expected HTTP statuses, completion before the cutoff, exact workflow/task counts,
both node results, exactly one attempt per task, zero retries, SQLite integrity and clean shutdown.

- Measured 1: http_requests, clean_shutdown; HTTP errors: ['TimeoutError']; lifecycle errors: ['worker_WorkerSessionMismatch', 'worker_WorkerSessionMismatch'].

- Measured 2: http_requests; HTTP errors: ['TimeoutError']; lifecycle errors: [].

- Measured 3: http_requests; HTTP errors: ['TimeoutError']; lifecycle errors: [].

This is a local capacity sample, not a production SLO. It does not benchmark model inference,
cross-host networking, long-running soak, or fault recovery. Timings vary with machine load and disk.
