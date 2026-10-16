"""CLI-process synchronization probes; never loaded by the container deployment."""
import os
from pathlib import Path
import time

from workflow_engine.deployment_demo import echo as ordinary_echo, summarize


def gated_echo(value, delay=0):
    if value == 'answer':
        release = Path(os.environ['WORKFLOW_TEST_RELEASE'])
        release.with_suffix('.entered').write_text('entered', encoding='utf-8')
        deadline = time.monotonic() + 30
        while not release.exists():
            if time.monotonic() >= deadline:
                raise TimeoutError('test supervisor did not release invocation')
            time.sleep(.01)
    return ordinary_echo(value, delay)


def create_registry():
    return {'probe.echo.v1': gated_echo, 'probe.summarize.v1': summarize}
