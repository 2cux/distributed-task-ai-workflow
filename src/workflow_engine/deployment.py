"""Standalone central/worker processes. Only the central process opens SQLite.

Registry factories are trusted deployment configuration, never workflow input.
Stop attestations are deliberately a local central-host command, not worker RPC.
"""
from __future__ import annotations

import argparse
import importlib
import json
import os
from pathlib import Path
import signal
import ssl
from threading import Event

from .management import ManagementService
from .persistence import SQLiteTaskStore
from .recovery import RecoverySystem
from .remote import RemoteWorker
from .retry import RetryPolicy
from .taskengine import TaskEngine


def load_registry(spec):
    module, separator, factory = spec.partition(':')
    if not separator or not module or not factory:
        raise ValueError('registry must be a trusted module:factory')
    registry = getattr(importlib.import_module(module), factory)()
    if not isinstance(registry, dict) or not registry or any(
            not isinstance(key, str) or not key or not callable(value)
            for key, value in registry.items()):
        raise ValueError('registry factory must return a nonempty handler mapping')
    return registry


def contract_registry(names):
    # Each name needs a distinct callable for the store's reverse mapping.
    def contract():
        def forbidden(*args, **kwargs):
            raise RuntimeError('central service must not execute business handlers')
        return forbidden
    if not names or len(names) != len(set(names)):
        raise ValueError('handler names must be nonempty and unique')
    return {name: contract() for name in names}


def _secret(name):
    value = os.environ.get(name)
    if not value:
        raise ValueError(name + ' environment variable is required')
    return value


def _server_tls(options):
    if bool(options.cert) != bool(options.key):
        raise ValueError('--cert and --key must be provided together')
    if not options.cert:
        return None
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    context.load_cert_chain(options.cert, options.key)
    return context


def _emit(event, **fields):
    print(json.dumps(dict(event=event, **fields)), flush=True)


def run_central(options, stop):
    registry = load_registry(options.registry) if options.registry else contract_registry(options.handlers)
    engine = TaskEngine(database_path=options.database, task_registry=registry,
                        retry_policy=RetryPolicy(options.max_retries))
    tls = _server_tls(options)
    recovery = engine.create_recovery_system(fence_after=options.fence_after)
    rpc = engine.create_recovery_service(_secret('WORKFLOW_RECOVERY_TOKEN'),
        host=options.host, port=options.rpc_port, ssl_context=tls,
        allow_insecure=options.allow_insecure)
    try:
        management = ManagementService(engine, _secret('WORKFLOW_API_TOKEN'),
            host=options.host, port=options.api_port, ssl_context=tls,
            allow_insecure=options.allow_insecure, recovery=recovery,
            poll_interval=options.poll_interval)
        try:
            rpc.start()
            management.start()
            _emit('central_ready', rpc_url=rpc.url, api_url=management.url)
            stop.wait()
        finally:
            management_drained = management.shutdown(5)
    finally:
        rpc_drained = rpc.shutdown(5)
    return 0 if management_drained and rpc_drained else 1


def run_worker(options, stop):
    context = ssl.create_default_context(cafile=options.ca) if options.ca else None
    worker = RemoteWorker(options.server, load_registry(options.registry),
        _secret('WORKFLOW_RECOVERY_TOKEN'), options.worker_id, host_id=options.host_id,
        ssl_context=context, heartbeat_interval=options.heartbeat_interval,
        heartbeat_timeout=options.heartbeat_timeout, request_timeout=options.request_timeout,
        poll_interval=options.poll_interval)
    worker.start()
    _emit('worker_ready', worker_id=worker.worker_id, session_id=worker.session_id)
    try:
        while not stop.wait(.05):
            if worker.join(0):
                break
    finally:
        # A transport failure in stop() must not skip waiting for business drain.
        try:
            worker.stop()
        except Exception as error:
            worker.last_error = error
        worker.join()  # SIGKILL is the supervisor's hard-stop boundary.
    error = worker.last_error
    _emit('worker_stopped', session_id=worker.session_id,
          error=type(error).__name__ if error else None)
    return 1 if error else 0


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    central = commands.add_parser('central')
    central.add_argument('--database', required=True)
    source = central.add_mutually_exclusive_group(required=True)
    source.add_argument('--registry')
    source.add_argument('--handlers', nargs='+')
    central.add_argument('--host', default='127.0.0.1')
    central.add_argument('--api-port', type=int, default=8080)
    central.add_argument('--rpc-port', type=int, default=8090)
    central.add_argument('--cert')
    central.add_argument('--key')
    central.add_argument('--allow-insecure', action='store_true')
    central.add_argument('--max-retries', type=int, default=1)
    central.add_argument('--fence-after', type=float, default=0)
    central.add_argument('--poll-interval', type=float, default=.1)
    worker = commands.add_parser('worker')
    worker.add_argument('--server', required=True)
    worker.add_argument('--registry', required=True)
    worker.add_argument('--worker-id', required=True)
    worker.add_argument('--host-id')
    worker.add_argument('--ca')
    worker.add_argument('--heartbeat-interval', type=float, default=1)
    worker.add_argument('--heartbeat-timeout', type=float, default=10)
    worker.add_argument('--request-timeout', type=float, default=2)
    worker.add_argument('--poll-interval', type=float, default=.05)
    confirm = commands.add_parser('confirm-stopped', help='trusted supervisor attestation on central host')
    confirm.add_argument('--database', required=True)
    confirm.add_argument('--session-id', required=True)
    confirm.add_argument('--host-id', required=True)
    confirm.add_argument('--operator', required=True)
    confirm.add_argument('--evidence', required=True)
    options = parser.parse_args(argv)
    if options.command == 'confirm-stopped':
        # No handler/provider credentials are needed to attest an exact session.
        if not Path(options.database).is_file():
            parser.error('stop confirmation requires an existing central database')
        recovery = RecoverySystem(SQLiteTaskStore(options.database, {}))
        recovery.confirm_host_stopped(options.host_id, session_ids=[options.session_id],
            operator=options.operator, evidence=options.evidence)
        _emit('stop_confirmed', session_id=options.session_id)
        return 0
    stop = Event()
    for name in ('SIGINT', 'SIGTERM'):
        if hasattr(signal, name):
            signal.signal(getattr(signal, name), lambda *_: stop.set())
    return run_central(options, stop) if options.command == 'central' else run_worker(options, stop)


if __name__ == '__main__':
    raise SystemExit(main())
