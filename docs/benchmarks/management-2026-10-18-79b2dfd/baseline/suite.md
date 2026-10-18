# Management Benchmark report

Result: **FAIL**; UTC timestamp: 2026-10-18T04:05:29.995374+00:00.

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
  "repeat": 5,
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
| Warmup 1 | PASS | 400 | 0 | 44.62 | 344.294 | 521.905 | 100 | 200 | PASS | ok |
| Measured 1 | FAIL | 400 | 2 | 37.13 | 518.999 | 564.773 | 98 | 196 | FAIL | ok |
| Measured 2 | PASS | 400 | 0 | 48.25 | 329.753 | 413.159 | 100 | 200 | PASS | ok |
| Measured 3 | PASS | 400 | 0 | 43.52 | 350.297 | 626.81 | 100 | 200 | PASS | ok |
| Measured 4 | FAIL | 400 | 1 | 42.81 | 408.597 | 495.607 | 99 | 198 | FAIL | ok |
| Measured 5 | PASS | 400 | 0 | 47.88 | 326.715 | 392.462 | 100 | 200 | PASS | ok |

## Statistics across measured rounds

Warmups are excluded. Failed measured rounds remain in the statistics and make the report FAIL.
Latency statistics below describe per-round percentiles; they are not pooled request percentiles.

| Metric | Min | Median | Max | Mean | Sample standard deviation |
| --- | ---: | ---: | ---: | ---: | ---: |
| http_requests_per_second | 37.13 | 43.52 | 48.25 | 43.918 | 4.526 |
| workflows_per_second | 1.63 | 10.2 | 11.22 | 7.172 | 5.066 |
| execution_seconds | 8.909 | 9.808 | 60.021 | 29.543 | 27.824 |
| http_p50_latency_ms | 141.19 | 148.674 | 165.386 | 153.433 | 10.507 |
| http_p95_latency_ms | 326.715 | 350.297 | 518.999 | 386.872 | 80.853 |
| http_p99_latency_ms | 392.462 | 495.607 | 626.81 | 498.562 | 99.236 |
| http_maximum_latency_ms | 1293.447 | 2044.36 | 2068.641 | 1867.001 | 330.904 |

Passed measured rounds: 3/5. Aggregate HTTP throughput: 43.52 req/s; workflow throughput: 3.36 workflows/s.

## Correctness checks

Every round checks expected HTTP statuses, completion before the cutoff, exact workflow/task counts,
both node results, exactly one attempt per task, zero retries, SQLite integrity and clean shutdown.

- Measured 1: http_requests, completion_before_deadline, workflow_count, task_count, single_attempts, task_results; HTTP errors: ['http_503']; lifecycle errors: [].

- Measured 4: http_requests, completion_before_deadline, workflow_count, task_count, single_attempts, task_results; HTTP errors: ['http_503']; lifecycle errors: [].

This is a local capacity sample, not a production SLO. It does not benchmark model inference,
cross-host networking, long-running soak, or fault recovery. Timings vary with machine load and disk.
