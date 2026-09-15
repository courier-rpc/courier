#!/usr/bin/env python3
"""Start an isolated MQTT fixture, run real client tests, and always stop it."""
import argparse
import json
import os
from pathlib import Path
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
from urllib.parse import urlparse


def binary(name, variable):
    value = os.environ.get(variable) or shutil.which(name)
    if not value:
        raise RuntimeError(f"Missing {name}; install it or set {variable}")
    return value


def stop(process):
    if process is None or process.poll() is not None:
        return
    os.killpg(process.pid, signal.SIGTERM)
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        os.killpg(process.pid, signal.SIGKILL)
        process.wait(timeout=5)


def run(command, cwd, env, timeout=180):
    print(f"\n[{Path(cwd).name}] {' '.join(map(str, command))}", flush=True)
    process = subprocess.Popen(command, cwd=cwd, env=env, start_new_session=True)
    try:
        code = process.wait(timeout=timeout)
        if code:
            raise RuntimeError(f"Command exited {code}: {command[0]}")
    finally:
        stop(process)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--suite', choices=['all', 'go', 'js', 'dart', 'swift'], default='all')
    parser.add_argument('--skip-install', action='store_true', help='Use already installed dependencies')
    args = parser.parse_args()
    go_repo = Path(__file__).resolve().parents[1]
    js_repo = Path(os.environ.get('COURIER_JS_DIR', go_repo.parent / 'courier-js')).resolve()
    dart_repo = Path(os.environ.get('COURIER_FLUTTER_DIR', go_repo.parent / 'courier-flutter')).resolve()
    swift_repo = Path(os.environ.get('COURIER_SWIFT_DIR', go_repo.parent / 'courier-swift')).resolve()
    suites = ['go', 'js', 'dart', 'swift'] if args.suite == 'all' else [args.suite]
    env = os.environ.copy()
    go = binary('go', 'GO_BIN')
    npm = binary('npm', 'NPM_BIN') if 'js' in suites else None
    dart = binary('dart', 'DART_BIN') if 'dart' in suites else None
    swift = binary('swift', 'SWIFT_BIN') if 'swift' in suites else None
    # Resolve tools and dependencies before opening any listener.
    if not args.skip_install:
        if npm:
            run([npm, 'ci'], js_repo, env, timeout=300)
        if dart:
            run([dart, 'pub', 'get'], dart_repo, env, timeout=300)
    if swift:
        run([swift, 'build', '--build-tests'], swift_repo, env, timeout=900)
    fixture = None
    endpoint = None
    with tempfile.TemporaryDirectory(prefix='courier-mqtt-') as tmp:
        tmp = Path(tmp)
        executable = tmp / 'mqtt-fixture'
        ready = tmp / 'ready.json'
        run([go, 'build', '-o', str(executable), '.'], go_repo / 'integration', env, timeout=300)
        with (tmp / 'fixture.log').open('w+') as log:
            try:
                fixture = subprocess.Popen([str(executable), '--ready-file', str(ready)],
                                           stdout=log, stderr=log, start_new_session=True)
                deadline = time.monotonic() + 30
                while not ready.exists():
                    if fixture.poll() is not None:
                        raise RuntimeError('MQTT fixture exited before readiness')
                    if time.monotonic() >= deadline:
                        raise RuntimeError('MQTT fixture readiness timed out')
                    time.sleep(0.05)
                info = json.loads(ready.read_text())
                endpoint = urlparse(info['url'])
                env['COURIER_MQTT_URL'] = info['url']
                env['COURIER_INTEGRATION_SERVICE'] = info['service']
                print(f"\nMQTT ready: {info['url']} (PID {fixture.pid}); two devices subscribed", flush=True)
                if 'go' in suites:
                    run([go, 'test', '-race', '-count=1', '-timeout=60s', './rpc', '-run', '^TestMQTTIntegration', '-v'], go_repo, env, 90)
                if 'js' in suites:
                    run([npm, 'run', 'test:integration:cases'], js_repo, env, 90)
                if 'dart' in suites:
                    run([dart, 'test', 'test/mqtt_integration_test.dart', '--reporter', 'expanded', '--timeout', '30s'], dart_repo, env, 90)
                if 'swift' in suites:
                    run([swift, 'test', '--skip-build', '--filter', 'MQTTIntegrationTests'], swift_repo, env, 90)
                print('\nAll selected MQTT integration suites passed.', flush=True)
            except BaseException:
                log.flush()
                log.seek(0)
                print('\n--- MQTT fixture log ---\n' + log.read(), file=sys.stderr, flush=True)
                raise
            finally:
                stop(fixture)
                if endpoint:
                    # Verify shutdown rather than only reporting that a signal was sent.
                    with socket.socket() as connection:
                        connection.settimeout(1)
                        if connection.connect_ex((endpoint.hostname, endpoint.port)) == 0:
                            raise RuntimeError('MQTT listener is still open after cleanup')
                    print(f'MQTT fixture stopped; port {endpoint.port} is closed. Temporary files removed on exit.', flush=True)


def interrupted(_signum, _frame):
    raise KeyboardInterrupt


if __name__ == '__main__':
    signal.signal(signal.SIGTERM, interrupted)
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted; cleanup completed.', file=sys.stderr)
        sys.exit(130)
    except Exception as error:
        print(f'Integration failed: {error}', file=sys.stderr)
        sys.exit(1)
