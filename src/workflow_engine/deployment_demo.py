"""Deterministic deployment probes; no provider calls or external side effects."""
import os
from pathlib import Path
import socket
import time


def echo(value, delay=0):
    if not isinstance(delay, (int, float)) or not 0 <= delay <= 120:
        raise ValueError('delay must be between 0 and 120 seconds')
    if value == 'answer' and os.environ.get('WORKFLOW_PROBE_GATE') == '1':
        # Only the lab victim enables this gate. Its private tmpfs is controlled
        # through docker exec; neither center nor replacement shares these files.
        Path('/tmp/workflow-probe-entered').touch()
        deadline = time.monotonic() + 120
        while not Path('/tmp/workflow-probe-release').exists():
            if time.monotonic() >= deadline:
                raise TimeoutError('container supervisor did not release probe')
            time.sleep(.02)
    time.sleep(delay)
    return dict(value=value, host=socket.gethostname(), pid=os.getpid())


def summarize(item):
    return dict(answer='verified:' + str(item['value']), source_host=item['host'],
                host=socket.gethostname())


def create_registry():
    return {'probe.echo.v1': echo, 'probe.summarize.v1': summarize}
