"""Process jobs from the shared queue and publish a small heartbeat.

Each Compose, Swarm, or Kubernetes replica runs this same file. The worker
publishes its runtime placement alongside its queue heartbeat so the dashboard
can show which node, pod/task, and process is handling a job.
"""

import json
import os
import socket
import time

import redis


REDIS_HOST = os.environ.get("REDIS_HOST", "queue")
QUEUE_KEY = os.environ.get("QUEUE_KEY", "jobs")
PROCESS_SECONDS = float(os.environ.get("PROCESS_SECONDS", "2"))
redis_client = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)


def runtime_env(name, default=""):
    """Read orchestrator metadata and ignore unresolved Swarm templates."""
    value = os.environ.get(name, default).strip()
    if value.startswith("{{") and value.endswith("}}"):
        return ""
    return value


ORCHESTRATOR = runtime_env("ORCHESTRATOR", "compose").lower()
NODE_NAME = runtime_env("NODE_NAME")
NODE_ID = runtime_env("NODE_ID")
POD_NAME = runtime_env("POD_NAME")
POD_NAMESPACE = runtime_env("POD_NAMESPACE")
TASK_NAME = runtime_env("TASK_NAME")
TASK_SLOT = runtime_env("TASK_SLOT")
CONTAINER_NAME = socket.gethostname()
worker_name = (
    runtime_env("WORKER_NAME")
    or POD_NAME
    or TASK_NAME
    or CONTAINER_NAME
)
worker_key = f"scale:worker:{worker_name}"


def publish(status, current_job=""):
    # Jede Replik veröffentlicht denselben Beobachtungssatz, ergänzt um ihre
    # Orchestrator- und Platzierungsdaten.
    processed = redis_client.hget(worker_key, "processed") or "0"
    redis_client.hset(
        worker_key,
        mapping={
            "name": worker_name,
            "orchestrator": ORCHESTRATOR,
            "node": NODE_NAME,
            "node_id": NODE_ID,
            "pod": POD_NAME,
            "namespace": POD_NAMESPACE,
            "task": TASK_NAME,
            "slot": TASK_SLOT,
            "container": CONTAINER_NAME,
            "process": "worker.py",
            "pid": str(os.getpid()),
            "status": status,
            "current_job": current_job,
            "processed": processed,
            "last_seen": str(int(time.time())),
        },
    )
    # Expiration removes stale replicas from the dashboard after a stop.
    redis_client.expire(worker_key, 20)


def main():
    redis_client.ping()
    print(f"worker {worker_name} waiting for jobs", flush=True)
    while True:
        publish("waiting")
        # BRPOP blockiert, bis ein Auftrag vorhanden ist. Genau deshalb kann
        # dasselbe Worker-Image mit --scale mehrfach parallel laufen.
        item = redis_client.brpop(QUEUE_KEY, timeout=5)
        if item is None:
            continue
        _, raw_job = item
        job = json.loads(raw_job)
        publish("processing", job["id"])
        print(f"worker {worker_name} processing {job['id']}", flush=True)
        # Die Wartezeit simuliert Arbeit und macht die Verteilung in der GUI
        # sichtbar; sie ist keine fachliche Berechnung.
        time.sleep(PROCESS_SECONDS)
        redis_client.incr("scale:processed")
        redis_client.hincrby(worker_key, "processed", 1)
        print(f"worker {worker_name} finished {job['id']}", flush=True)


if __name__ == "__main__":
    try:
        main()
    finally:
        publish("stopped")
