"""Tiny stdlib-only client shared by smoke_test.py and chaos_test.py."""

from __future__ import annotations

import http.client
import json
import os
import subprocess
import threading
import time
import urllib.error
import urllib.request
from urllib.parse import urlsplit
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parent.parent
TERMINAL = {"SUCCEEDED", "FAILED", "DEAD_LETTER", "CANCELLED"}
RUN_TERMINAL = {"SUCCEEDED", "FAILED", "CANCELLED"}


def compose(*args: str, check: bool = True) -> str:
    """Run `docker compose ...` in the project directory and return stdout."""
    env = {**os.environ, "COMPOSE_BAKE": "false"}
    proc = subprocess.run(
        ["docker", "compose", *args], cwd=PROJECT_DIR, env=env, capture_output=True, text=True, check=False
    )
    if check and proc.returncode != 0:
        raise RuntimeError(f"docker compose {' '.join(args)} failed:\n{proc.stderr}")
    return proc.stdout


def create_api_key(name: str) -> str:
    out = compose("exec", "-T", "api", "python", "-m", "app.cli", "create-api-key", "--name", name)
    return out.strip().splitlines()[-1]


def psql(sql: str) -> str:
    return compose("exec", "-T", "postgres", "psql", "-U", "flowgrid", "-tAc", sql).strip()


def _lower(headers) -> dict[str, str]:
    # urllib preserves header case on success but not on HTTPError; normalise.
    return {k.lower(): v for k, v in headers.items()}


class Client:
    def __init__(self, base_url: str, api_key: str | None) -> None:
        self.base = base_url.rstrip("/")
        self.api_key = api_key

    def call(
        self, method: str, path: str, body: dict | None = None, headers: dict | None = None, auth: bool = True
    ) -> tuple[int, dict, dict]:
        h = {"Content-Type": "application/json", **(headers or {})}
        if auth and self.api_key:
            h["Authorization"] = f"Bearer {self.api_key}"
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(self.base + path, data=data, method=method, headers=h)
        try:
            with urllib.request.urlopen(req, timeout=15) as res:
                return res.status, json.loads(res.read() or b"{}"), _lower(res.headers)
        except urllib.error.HTTPError as err:
            return err.code, json.loads(err.read() or b"{}"), _lower(err.headers)

    def submit(self, job_type: str, payload: dict, expect: int = 202, headers: dict | None = None, **opts) -> dict:
        status, body, _ = self.call("POST", "/api/v1/jobs", {"type": job_type, "payload": payload, **opts}, headers)
        assert status == expect, f"expected {expect}, got {status}: {body}"
        return body

    def job(self, job_id: str) -> dict:
        return self.call("GET", f"/api/v1/jobs/{job_id}")[1]

    def attempts(self, job_id: str) -> list[dict]:
        return self.call("GET", f"/api/v1/jobs/{job_id}/attempts")[1]

    def wait_run(self, workflow_id: str, run_id: str, timeout: float = 60) -> dict:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            run = self.call("GET", f"/api/v1/workflows/{workflow_id}/runs/{run_id}")[1]
            if run["status"] in RUN_TERMINAL:
                return run
            time.sleep(0.25)
        raise AssertionError(f"run {run_id} not finished within {timeout}s (last: {run['status']})")

    def wait(self, job_id: str, timeout: float = 30, until: set[str] = TERMINAL) -> dict:
        deadline = time.monotonic() + timeout
        seen: list[str] = []
        while time.monotonic() < deadline:
            job = self.job(job_id)
            if not seen or seen[-1] != job["status"]:
                seen.append(job["status"])
            if job["status"] in until:
                job["_observed"] = seen
                return job
            time.sleep(0.2)
        raise AssertionError(f"job {job_id} did not reach {until} within {timeout}s (seen: {seen})")


class SSEReader(threading.Thread):
    """Reads /api/v1/events in the background and collects parsed events."""

    def __init__(self, base_url: str, api_key: str) -> None:
        super().__init__(daemon=True)
        url = urlsplit(base_url)
        self.conn = http.client.HTTPConnection(url.hostname, url.port or 80, timeout=60)
        self.api_key = api_key
        self.events: list[tuple[float, dict]] = []
        self.ready = threading.Event()

    def run(self) -> None:
        self.conn.request("GET", "/api/v1/events", headers={"Authorization": f"Bearer {self.api_key}"})
        res = self.conn.getresponse()
        kind = None
        for raw in res:
            line = raw.decode().rstrip("\n")
            if line.startswith("event: "):
                kind = line[7:]
                if kind == "ready":
                    self.ready.set()
            elif line.startswith("data: ") and kind not in (None, "ready"):
                self.events.append((time.monotonic(), json.loads(line[6:])))

    def close(self) -> None:
        try:
            self.conn.sock.close()
        except Exception:
            pass


def run_checks(checks) -> int:
    passed = 0
    for name, fn in checks:
        start = time.monotonic()
        try:
            detail = fn()
        except AssertionError as exc:
            print(f"  FAIL  {name}: {exc}")
            continue
        passed += 1
        took = time.monotonic() - start
        print(f"  ok    {name}  [{took:.1f}s]" + (f"  ({detail})" if detail else ""))
    print(f"\n{passed}/{len(checks)} checks passed")
    return 0 if passed == len(checks) else 1
