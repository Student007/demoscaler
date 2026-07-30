"""Run one cooperative task in a Kubernetes multi-container Job Pod.

The three roles use the same image but different commands/configuration. They
run in parallel in one Pod and share /workspace through an emptyDir volume.
This is intentionally separate from the scalable worker deployment: a bundle
is one logical unit, while the normal workers are independent consumers.
"""

import json
import os
import socket
import time
from pathlib import Path

import redis


REDIS_HOST = os.environ.get("REDIS_HOST", "queue")
BUNDLE_ID = os.environ.get("BUNDLE_ID", "unknown-bundle")
TASK_ROLE = os.environ.get("TASK_ROLE", "unknown")
POD_NAME = os.environ.get("POD_NAME", socket.gethostname())
NODE_NAME = os.environ.get("NODE_NAME", "unknown-node")
CONTAINER_NAME = os.environ.get("CONTAINER_NAME", TASK_ROLE)
WORKSPACE = Path(os.environ.get("WORKSPACE", "/workspace"))
BUNDLE_KEY = f"scale:bundle:{BUNDLE_ID}"
BUNDLE_ACTIVE_TTL_SECONDS = 1800
BUNDLE_FINISHED_TTL_SECONDS = 8
TASK_ROLES = ("collect", "analyze", "assemble")
redis_client = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)


def refresh_bundle_ttl():
    """Keep active bundles observable and remove terminal bundles shortly after completion."""
    statuses = []
    for role in TASK_ROLES:
        raw_task = redis_client.hget(BUNDLE_KEY, f"task:{role}")
        if not raw_task:
            redis_client.expire(BUNDLE_KEY, BUNDLE_ACTIVE_TTL_SECONDS)
            return
        try:
            statuses.append(json.loads(raw_task).get("status"))
        except json.JSONDecodeError:
            redis_client.expire(BUNDLE_KEY, BUNDLE_ACTIVE_TTL_SECONDS)
            return
    ttl = BUNDLE_FINISHED_TTL_SECONDS if all(status in {"completed", "failed"} for status in statuses) else BUNDLE_ACTIVE_TTL_SECONDS
    redis_client.expire(BUNDLE_KEY, ttl)


def publish(status, message=""):
    payload = {
        "role": TASK_ROLE,
        "status": status,
        "message": message,
        "bundle_id": BUNDLE_ID,
        "pod": POD_NAME,
        "node": NODE_NAME,
        "container": CONTAINER_NAME,
        "process": "bundle/app.py",
        "pid": os.getpid(),
        "updated": int(time.time()),
    }
    redis_client.hset(
        BUNDLE_KEY,
        mapping={
            "bundle_id": BUNDLE_ID,
            "orchestrator": "kubernetes",
            "pod": POD_NAME,
            "node": NODE_NAME,
            f"task:{TASK_ROLE}": json.dumps(payload),
        },
    )
    refresh_bundle_ttl()


def write_result(filename, content):
    WORKSPACE.mkdir(parents=True, exist_ok=True)
    (WORKSPACE / filename).write_text(content, encoding="utf-8")


def wait_for_results(*filenames, timeout=30):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if all((WORKSPACE / filename).exists() for filename in filenames):
            return
        time.sleep(0.5)
    missing = [filename for filename in filenames if not (WORKSPACE / filename).exists()]
    raise TimeoutError(f"missing shared results: {', '.join(missing)}")


def run_task():
    if TASK_ROLE == "collect":
        publish("processing", "sammelt Bundle-Eingaben")
        time.sleep(2)
        write_result("collect.txt", f"collect completed for {BUNDLE_ID}\n")
        return "Eingaben liegen im gemeinsamen /workspace."

    if TASK_ROLE == "analyze":
        publish("processing", "analysiert Bundle-Daten")
        time.sleep(3)
        write_result("analyze.txt", f"analyze completed for {BUNDLE_ID}\n")
        return "Analyse liegt im gemeinsamen /workspace."

    if TASK_ROLE == "assemble":
        publish("waiting", "wartet auf collect.txt und analyze.txt")
        wait_for_results("collect.txt", "analyze.txt")
        publish("processing", "baut das Bundle-Ergebnis")
        content = (WORKSPACE / "collect.txt").read_text(encoding="utf-8")
        content += (WORKSPACE / "analyze.txt").read_text(encoding="utf-8")
        time.sleep(1)
        write_result("bundle-result.txt", content + f"assemble completed for {BUNDLE_ID}\n")
        return "Bundle-Ergebnis wurde aus den gemeinsamen Dateien gebaut."

    raise ValueError(f"unknown TASK_ROLE: {TASK_ROLE}")


def main():
    redis_client.ping()
    publish("waiting", "Container gestartet")
    message = run_task()
    publish("completed", message)
    print(f"bundle {BUNDLE_ID} task {TASK_ROLE} completed", flush=True)


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        try:
            publish("failed", str(error))
        finally:
            print(f"bundle {BUNDLE_ID} task {TASK_ROLE} failed: {error}", flush=True)
        raise
