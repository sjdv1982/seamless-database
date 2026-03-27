import json
import os
import signal
import subprocess
import time
from pathlib import Path
from urllib.error import URLError
from urllib.request import urlopen


DEFAULT_RANDOM_PORT_START = 49152
DEFAULT_RANDOM_PORT_END = 65535


def wait_for_status_file(status_file: Path, timeout: float = 10.0):
    deadline = time.monotonic() + timeout
    last_error = None
    while time.monotonic() < deadline:
        try:
            payload = json.loads(status_file.read_text())
        except (FileNotFoundError, json.JSONDecodeError) as exc:
            last_error = exc
            time.sleep(0.1)
            continue
        if payload.get("status") == "running" and isinstance(payload.get("port"), int):
            return payload
        last_error = payload
        time.sleep(0.1)
    raise RuntimeError(f"Database status file did not report running state: {last_error}")


def wait_for_server(port: int, timeout: float = 10.0):
    deadline = time.monotonic() + timeout
    url = f"http://127.0.0.1:{port}/healthcheck"
    last_error = None
    while time.monotonic() < deadline:
        try:
            with urlopen(url, timeout=1.0) as response:  # noqa: S310 - local test server
                if response.status == 200 and response.read().decode() == "OK":
                    return
                last_error = response.status
        except URLError as exc:
            last_error = exc
        time.sleep(0.1)
    raise RuntimeError(f"Database server at {url} did not become ready: {last_error}")


def test_random_port_default(tmp_path):
    database_file = tmp_path / "seamless.db"
    status_file = tmp_path / "status.json"
    status_file.write_text("{}\n")
    command = [
        "seamless-database",
        str(database_file),
        "--writable",
        "--status-file",
        str(status_file),
    ]
    process = subprocess.Popen(  # noqa: S603,S607 - controlled test command
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        env=os.environ.copy(),
    )
    try:
        payload = wait_for_status_file(status_file)
        port = payload["port"]
        assert DEFAULT_RANDOM_PORT_START <= port <= DEFAULT_RANDOM_PORT_END, port
        assert port != 5522
        wait_for_server(port)
    finally:
        if process.poll() is None:
            process.send_signal(signal.SIGINT)
        try:
            stdout, _ = process.communicate(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            stdout, _ = process.communicate()
        if stdout:
            print("Server logs:")
            print(stdout, end="" if stdout.endswith("\n") else "\n")
