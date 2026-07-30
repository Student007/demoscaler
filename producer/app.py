"""Create jobs and put them into the shared Redis list.

The producer is deliberately small: the important Docker observations live in
compose.yaml, while this file makes the queue interaction explicit and
repeatable for the students.
"""

import json
import os
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

import redis


REDIS_HOST = os.environ.get("REDIS_HOST", "queue")
QUEUE_KEY = os.environ.get("QUEUE_KEY", "jobs")
DEFAULT_COUNT = int(os.environ.get("JOB_COUNT_DEFAULT", "12"))
redis_client = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)


def enqueue(count: int) -> list[str]:
    # Didaktischer Kern: Ein Auftrag ist nur JSON in einer Redis-Liste. Die
    # Studierenden müssen dafür weder Flask noch ein Python-Framework kennen.
    count = max(1, min(count, 200))
    jobs = []
    for number in range(count):
        job_id = f"job-{time.time_ns()}-{number:03d}"
        jobs.append(job_id)
        redis_client.rpush(
            QUEUE_KEY,
            json.dumps({"id": job_id, "created_at": time.time()}),
        )
    return jobs


class Handler(BaseHTTPRequestHandler):
    def send_json(self, payload, status=200):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):  # noqa: N802 - required by BaseHTTPRequestHandler
        parsed = urlparse(self.path)
        if parsed.path == "/health":
            redis_client.ping()
            self.send_json({"status": "ok", "service": "producer"})
            return
        if parsed.path == "/enqueue":
            values = parse_qs(parsed.query).get("n", [str(DEFAULT_COUNT)])
            try:
                count = int(values[0])
            except ValueError:
                self.send_json({"error": "n must be an integer"}, 400)
                return
            jobs = enqueue(count)
            self.send_json({"enqueued": len(jobs), "jobs": jobs})
            return
        self.send_json({"service": "producer", "endpoint": "/enqueue?n=12"})

    def log_message(self, format, *args):
        print(f"producer: {format % args}", flush=True)


if __name__ == "__main__":
    server = ThreadingHTTPServer(("0.0.0.0", 8000), Handler)
    print(f"producer listening on :8000, queue={QUEUE_KEY}", flush=True)
    server.serve_forever()
