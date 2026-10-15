"""Low-cardinality process metrics; persisted engine totals are read at scrape time."""
from __future__ import annotations

from collections import defaultdict
from threading import Lock
import time


class ServiceMetrics:
    buckets = (.005, .01, .025, .05, .1, .25, .5, 1, 2, 5)

    def __init__(self):
        self._lock = Lock()
        self.started_at = time.time()
        self.started_clock = time.monotonic()
        self.requests = defaultdict(int)
        self.latency = defaultdict(lambda: [0] * len(self.buckets))
        self.sums = defaultdict(float)
        self.coordinator_ticks = 0
        self.coordinator_errors = 0
        self.coordinator_last_success = 0.0
        self.overloads = 0
        self.database_busy_errors = 0

    def observe(self, method, route, status, elapsed):
        with self._lock:
            key = (method, route, str(status))
            self.requests[key] += 1
            self.sums[key] += elapsed
            for index, bound in enumerate(self.buckets):
                if elapsed <= bound:
                    self.latency[key][index] += 1

    def tick(self, success):
        with self._lock:
            self.coordinator_ticks += 1
            if success:
                self.coordinator_last_success = time.time()
            else:
                self.coordinator_errors += 1

    def overloaded(self):
        with self._lock:
            self.overloads += 1

    def database_busy(self):
        with self._lock:
            self.database_busy_errors += 1

    def render(self, overview):
        lines = []

        def metric(name, kind, help_text):
            lines.extend((f'# HELP {name} {help_text}', f'# TYPE {name} {kind}'))

        metric('workflow_api_requests_total', 'counter', 'Completed HTTP requests by route template.')
        with self._lock:
            for (method, route, status), count in sorted(self.requests.items()):
                labels = f'method="{method}",route="{route}",status="{status}"'
                lines.append(f'workflow_api_requests_total{{{labels}}} {count}')
            metric('workflow_api_request_duration_seconds', 'histogram', 'HTTP handling latency including serialization.')
            for key, count in sorted(self.requests.items()):
                method, route, status = key
                labels = f'method="{method}",route="{route}",status="{status}"'
                for bound, hits in zip(self.buckets, self.latency[key]):
                    lines.append(f'workflow_api_request_duration_seconds_bucket{{{labels},le="{bound}"}} {hits}')
                lines.extend((f'workflow_api_request_duration_seconds_bucket{{{labels},le="+Inf"}} {count}',
                              f'workflow_api_request_duration_seconds_count{{{labels}}} {count}',
                              f'workflow_api_request_duration_seconds_sum{{{labels}}} {self.sums[key]:.9f}'))
            for name, kind, value in (
                ('workflow_api_uptime_seconds', 'gauge', time.monotonic() - self.started_clock),
                ('workflow_api_overloads_total', 'counter', self.overloads),
                ('workflow_api_database_busy_total', 'counter', self.database_busy_errors),
                ('workflow_coordinator_ticks_total', 'counter', self.coordinator_ticks),
                ('workflow_coordinator_errors_total', 'counter', self.coordinator_errors),
                ('workflow_coordinator_last_success_timestamp_seconds', 'gauge', self.coordinator_last_success)):
                metric(name, kind, name.replace('_', ' '))
                lines.append(f'{name} {value}')
        for resource in ('tasks', 'workflows', 'sessions'):
            name = f'workflow_engine_{resource}'
            metric(name, 'gauge', f'Persisted {resource} by current state.')
            for state, count in sorted(overview[resource].items()):
                lines.append(f'{name}{{status="{state}"}} {count}')
        for field in ('attempts', 'retries', 'dispatchable_tasks', 'overdue_sessions', 'open_incidents'):
            name = f'workflow_engine_{field}'
            metric(name, 'gauge', f'Current persisted {field}; survives service restart.')
            lines.append(f'{name} {overview[field]}')
        return '\n'.join(lines) + '\n'
