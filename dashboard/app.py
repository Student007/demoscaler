"""Serve the local learning dashboard and expose observations as JSON."""

import json
import os
import socket
import ssl
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.request import Request, urlopen
from urllib.parse import parse_qs, urlparse

import redis


REDIS_HOST = os.environ.get("REDIS_HOST", "queue")
QUEUE_KEY = os.environ.get("QUEUE_KEY", "jobs")
PRODUCER_URL = os.environ.get("PRODUCER_URL", "http://producer:8000")


def runtime_env(name, default=""):
    value = os.environ.get(name, default).strip()
    if value.startswith("{{") and value.endswith("}}"):
        return default
    return value


ORCHESTRATOR = runtime_env("ORCHESTRATOR", "compose").lower()
NODE_NAME = runtime_env("NODE_NAME", "local-engine")
POD_NAME = runtime_env("POD_NAME")
POD_NAMESPACE = runtime_env("POD_NAMESPACE")
TASK_NAME = runtime_env("TASK_NAME")
TASK_SLOT = runtime_env("TASK_SLOT")
CONTAINER_NAME = socket.gethostname()
BUNDLE_NAMESPACE = POD_NAMESPACE or os.environ.get("BUNDLE_NAMESPACE", "demoscale")
BUNDLE_IMAGE = os.environ.get("BUNDLE_IMAGE", "danbu/demoscale:demoscale-bundle-task-1.2.8")
BUNDLE_ROLES = ("collect", "analyze", "assemble")
BUNDLE_ACTIVE_TTL_SECONDS = 1800
BUNDLE_FINISHED_TTL_SECONDS = 8
redis_client = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)


def bundle_statuses():
    """Read bundle/task heartbeats written by the multi-container Jobs."""
    bundles = []
    for key in sorted(redis_client.scan_iter("scale:bundle:*")):
        data = redis_client.hgetall(key)
        if not data:
            continue
        tasks = []
        for role in BUNDLE_ROLES:
            raw_task = data.get(f"task:{role}")
            if raw_task:
                try:
                    tasks.append(json.loads(raw_task))
                except json.JSONDecodeError:
                    tasks.append({"role": role, "status": "unknown"})
            else:
                tasks.append({"role": role, "status": "pending"})
        statuses = {task.get("status") for task in tasks}
        if "failed" in statuses:
            status = "failed"
        elif all(task.get("status") == "completed" for task in tasks):
            status = "completed"
        elif any(task.get("status") in {"processing", "waiting"} for task in tasks):
            status = "running"
        else:
            status = "pending"
        if status in {"completed", "failed"}:
            last_updated = max(int(task.get("updated") or 0) for task in tasks)
            if last_updated and int(time.time()) - last_updated >= BUNDLE_FINISHED_TTL_SECONDS:
                continue
        bundles.append(
            {
                "id": data.get("bundle_id", key.rsplit(":", 1)[-1]),
                "status": status,
                "orchestrator": data.get("orchestrator", "kubernetes"),
                "pod": data.get("pod") or next((task.get("pod") for task in tasks if task.get("pod")), ""),
                "node": data.get("node") or next((task.get("node") for task in tasks if task.get("node")), ""),
                "tasks": tasks,
            }
        )
    return bundles[-12:]


def kubernetes_api_request(path, method="GET", payload=None):
    """Call the in-cluster Kubernetes API using the dashboard ServiceAccount."""
    host = os.environ.get("KUBERNETES_SERVICE_HOST")
    port = os.environ.get("KUBERNETES_SERVICE_PORT", "443")
    token_path = "/var/run/secrets/kubernetes.io/serviceaccount/token"
    ca_path = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
    if not host or not os.path.exists(token_path):
        raise RuntimeError("Kubernetes API ist nur innerhalb eines Kubernetes-Pods verfügbar")
    with open(token_path, encoding="utf-8") as token_file:
        token = token_file.read().strip()
    body = json.dumps(payload).encode("utf-8") if payload is not None else None
    request = Request(
        f"https://{host}:{port}{path}",
        data=body,
        method=method,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        },
    )
    context = ssl.create_default_context(cafile=ca_path if os.path.exists(ca_path) else None)
    with urlopen(request, timeout=10, context=context) as response:
        raw = response.read()
    return json.loads(raw) if raw else {}


def bundle_job_manifest(bundle_id):
    """Build one Job whose Pod contains three cooperating task containers."""
    common_env = [
        {"name": "ORCHESTRATOR", "value": "kubernetes"},
        {"name": "BUNDLE_ID", "value": bundle_id},
        {"name": "REDIS_HOST", "value": REDIS_HOST},
        {"name": "WORKSPACE", "value": "/workspace"},
        {"name": "POD_NAME", "valueFrom": {"fieldRef": {"fieldPath": "metadata.name"}}},
        {"name": "NODE_NAME", "valueFrom": {"fieldRef": {"fieldPath": "spec.nodeName"}}},
    ]
    containers = []
    for role in BUNDLE_ROLES:
        containers.append(
            {
                "name": role,
                "image": BUNDLE_IMAGE,
                "imagePullPolicy": "IfNotPresent",
                "env": common_env + [
                    {"name": "TASK_ROLE", "value": role},
                    {"name": "CONTAINER_NAME", "value": role},
                ],
                "volumeMounts": [{"name": "workspace", "mountPath": "/workspace"}],
                "securityContext": {
                    "allowPrivilegeEscalation": False,
                    "readOnlyRootFilesystem": True,
                },
                "resources": {
                    "requests": {"cpu": "10m", "memory": "32Mi"},
                    "limits": {"memory": "128Mi"},
                },
            }
        )
    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": f"bundle-{bundle_id}",
            "labels": {
                "app.kubernetes.io/name": "demoscale",
                "app.kubernetes.io/component": "bundle-task",
                "demoscale.io/bundle": bundle_id,
            },
        },
        "spec": {
            "backoffLimit": 0,
            "activeDeadlineSeconds": 90,
            "ttlSecondsAfterFinished": BUNDLE_FINISHED_TTL_SECONDS,
            "template": {
                "metadata": {
                    "labels": {
                        "app.kubernetes.io/name": "demoscale",
                        "app.kubernetes.io/component": "bundle-task",
                        "demoscale.io/bundle": bundle_id,
                    }
                },
                "spec": {
                    "restartPolicy": "Never",
                    "securityContext": {
                        "runAsNonRoot": True,
                        "runAsUser": 10001,
                        "runAsGroup": 10001,
                        "fsGroup": 10001,
                        "seccompProfile": {"type": "RuntimeDefault"},
                    },
                    "containers": containers,
                    "volumes": [{"name": "workspace", "emptyDir": {}}],
                },
            },
        },
    }


def create_bundle():
    if ORCHESTRATOR != "kubernetes":
        raise RuntimeError("Auftrags-Bundles benötigen Kubernetes/KIND")
    bundle_id = uuid.uuid4().hex[:8]
    redis_client.hset(
        f"scale:bundle:{bundle_id}",
        mapping={
            "bundle_id": bundle_id,
            "orchestrator": "kubernetes",
            "status": "pending",
            "created": str(int(time.time())),
        },
    )
    redis_client.expire(f"scale:bundle:{bundle_id}", BUNDLE_ACTIVE_TTL_SECONDS)
    try:
        kubernetes_api_request(
            f"/apis/batch/v1/namespaces/{BUNDLE_NAMESPACE}/jobs",
            method="POST",
            payload=bundle_job_manifest(bundle_id),
        )
    except Exception:
        redis_client.hset(f"scale:bundle:{bundle_id}", "status", "failed")
        raise
    return bundle_id


HTML = """<!doctype html>
<html lang="de">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Docker-Skalierungsdemo</title>
<style>
:root{color-scheme:light;--blue:#174ea6;--blue-dark:#123b7a;--blue-light:#eaf2ff;--green:#18794e;--amber:#a15c00;--ink:#17233b;--muted:#5d6b82;--line:#d8e1ef;--line-strong:#9bbbe0}
*{box-sizing:border-box}body{margin:0;background:#f6f8fc;color:var(--ink);font:16px/1.5 system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}
main{max-width:1050px;margin:0 auto;padding:2.5rem 1.25rem 3rem}h1,h2,h3{line-height:1.2;margin:0}h1{font-size:clamp(2rem,4vw,3.25rem);color:var(--blue-dark);letter-spacing:-.03em}h2{font-size:1.35rem;color:var(--blue-dark)}p{margin:.5rem 0}.eyebrow{color:var(--blue);font-size:.78rem;font-weight:750;letter-spacing:.12em;text-transform:uppercase}.lead{max-width:720px;color:var(--muted);font-size:1.08rem;margin-top:.75rem}
.quickstart,.panel,.explanation{background:#fff;border:1px solid var(--line);border-radius:18px;box-shadow:0 8px 24px #21447812}.quickstart{display:grid;grid-template-columns:1.5fr 1fr;gap:1.25rem;margin:2rem 0;padding:1.25rem 1.5rem}.quickstart strong{color:var(--blue-dark)}ol{margin:.55rem 0 0;padding-left:1.35rem}.command{align-self:center;background:var(--blue-light);border-left:4px solid var(--blue);border-radius:10px;color:var(--blue-dark);font:700 .92rem/1.4 ui-monospace,SFMono-Regular,Menlo,monospace;padding:1rem;overflow-wrap:anywhere}
.runtime{align-items:center;background:#fff;border:1px solid var(--line);border-radius:18px;box-shadow:0 8px 24px #21447812;display:grid;gap:1.2rem;grid-template-columns:minmax(240px,.8fr) 1.2fr;margin:1.5rem 0;padding:1.2rem 1.5rem}.runtime h2{margin-top:.2rem}.runtime-subtitle{color:var(--muted);font-size:.92rem;margin:.35rem 0 0}.runtime-badge{background:var(--blue);border-radius:999px;color:#fff;display:inline-block;font-size:.8rem;font-weight:750;padding:.28rem .65rem}.runtime-facts{display:grid;gap:.6rem;grid-template-columns:repeat(2,minmax(0,1fr))}.runtime-fact{background:var(--blue-light);border-radius:10px;padding:.65rem .8rem}.runtime-fact-label{color:var(--muted);display:block;font-size:.75rem}.runtime-fact-value{display:block;font-size:.9rem;font-weight:700;overflow-wrap:anywhere}
.actions{display:flex;align-items:center;gap:.7rem;flex-wrap:wrap;margin:1.25rem 0 1.5rem}.actions h2{margin-right:.5rem}.button{background:var(--blue);border:0;border-radius:10px;color:#fff;cursor:pointer;font:700 .95rem system-ui;padding:.7rem 1rem;transition:background .15s,transform .15s}.button:hover{background:var(--blue-dark);transform:translateY(-1px)}.button:disabled{cursor:wait;opacity:.6;transform:none}.message{color:var(--muted);font-size:.9rem}
.stats{display:grid;grid-template-columns:repeat(3,1fr);gap:1rem;margin-bottom:1.5rem}.stat{background:var(--blue-light);border-radius:14px;padding:1rem 1.1rem}.stat-label{color:var(--muted);font-size:.88rem}.stat-value{color:var(--blue-dark);font-size:2rem;font-weight:800;line-height:1.1;margin-top:.2rem}
.panel{padding:1.35rem 1.5rem}.section-head{align-items:baseline;display:flex;justify-content:space-between;gap:1rem;margin-bottom:1rem}.updated{color:var(--muted);font-size:.84rem}.topology-legend{display:flex;flex-wrap:wrap;gap:.5rem .9rem;margin:-.25rem 0 1rem;color:var(--muted);font-size:.82rem}.topology-legend span{align-items:center;display:inline-flex;gap:.4rem}.topology-legend span::before{border-radius:4px;content:"";display:inline-block;height:.72rem;width:.72rem}.topology-legend span:nth-child(1)::before{background:#f9fcff;border:2px solid #7da9d6}.topology-legend span:nth-child(2)::before{background:#fff;border:1px solid #9bbbe0}.topology-legend span:nth-child(3)::before{background:var(--blue-light);border:1px solid var(--blue)}.topology-shell{background:#f7fbff;border:2px solid #a8c5e4;border-radius:17px;padding:1.1rem}.topology-shell.compose{border-color:#9dbfe3}.topology-shell.swarm{border-color:#75a8d8}.topology-shell.kubernetes{border-color:#5b8fc8}.topology-header{align-items:flex-start;display:flex;gap:1rem;justify-content:space-between;margin-bottom:1rem}.topology-header h3{color:var(--blue);font-size:1.2rem}.topology-header p{color:var(--muted);font-size:.88rem;margin:.25rem 0 0}.topology-header .topology-mode{color:var(--blue);font-size:.76rem;font-weight:800;letter-spacing:.08em;text-transform:uppercase;white-space:nowrap}.node-grid{display:grid;gap:1rem;grid-template-columns:repeat(auto-fit,minmax(260px,1fr))}.node-group{background:#f9fcff;border:2px solid #7da9d6;border-radius:14px;padding:.9rem}.node-group.compose{border-color:#a7c4e4}.node-group.swarm{border-color:#6f9dca}.node-group.kubernetes{border-color:#6d94c4}.node-title{align-items:baseline;display:flex;flex-wrap:wrap;gap:.5rem;justify-content:space-between;margin-bottom:.7rem}.node-title strong{color:var(--blue-dark);font-size:1rem}.node-title span{color:var(--muted);font-size:.78rem}.node-subtitle{color:var(--muted);font-size:.82rem;margin:-.35rem 0 .75rem}.pod-grid{display:grid;gap:.75rem;grid-template-columns:repeat(auto-fit,minmax(235px,1fr))}.pod-group{background:#fff;border:1px solid #9bbbe0;border-radius:11px;padding:.75rem}.pod-title{align-items:baseline;display:flex;gap:.4rem;justify-content:space-between;margin-bottom:.55rem}.pod-title strong{font:700 .8rem ui-monospace,SFMono-Regular,Menlo,monospace;overflow-wrap:anywhere}.pod-title span{color:var(--blue);font-size:.76rem;font-weight:750}.task-grid{display:grid;gap:.75rem;grid-template-columns:repeat(auto-fit,minmax(235px,1fr))}.worker{background:#fff;border:1px solid var(--line);border-radius:11px;padding:.85rem}.worker[open]{box-shadow:0 3px 12px #21447810}.worker-summary{align-items:center;cursor:pointer;display:flex;flex-wrap:wrap;gap:.55rem;list-style:none;min-width:0}.worker-summary::-webkit-details-marker{display:none}.worker-summary::before{color:var(--blue);content:"▸";font-size:1rem;font-weight:800;line-height:1}.worker[open]>.worker-summary::before{content:"▾"}.worker-summary .worker-name{flex:1 1 95px;min-width:0}.worker-job{color:var(--muted);font-size:.78rem;max-width:43%;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}.worker-summary:focus-visible{border-radius:6px;outline:3px solid #8bb4df;outline-offset:3px}.worker-details{border-top:1px solid var(--line);margin-top:.75rem;padding-top:.55rem}.worker-name{font:700 .88rem ui-monospace,SFMono-Regular,Menlo,monospace;overflow-wrap:anywhere}.badge{border-radius:999px;font-size:.78rem;font-weight:750;padding:.2rem .55rem;white-space:nowrap}.waiting{background:#eaf6ef;color:var(--green)}.processing{background:#fff3df;color:var(--amber)}.stopped{background:#fbeaea;color:#a33}.worker-meta{color:var(--muted);font-size:.86rem}.meta-row{display:grid;gap:.5rem;grid-template-columns:82px 1fr;margin-top:.3rem}.meta-label{color:var(--muted)}.meta-value{color:var(--ink);font-weight:650;overflow-wrap:anywhere}.empty{border:1px dashed var(--line);border-radius:12px;color:var(--muted);padding:1.1rem;text-align:center}
.explanation{display:grid;grid-template-columns:repeat(3,1fr);gap:1rem;margin-top:1.5rem;padding:1.25rem 1.5rem}.explanation h3{color:var(--blue);font-size:1rem}.explanation p{color:var(--muted);font-size:.92rem}.footer{color:var(--muted);font-size:.85rem;margin-top:1.4rem}.footer a{color:var(--blue)}
@media(max-width:720px){main{padding-top:1.5rem}.quickstart,.runtime,.explanation{grid-template-columns:1fr}.runtime-facts{grid-template-columns:1fr}.stats{grid-template-columns:1fr}.section-head{align-items:flex-start;flex-direction:column}}
.topology-icon{display:inline-block;font-size:1.05em;margin-right:.35rem;vertical-align:-.08em}.worker-role-icon{align-items:center;background:var(--blue-light);border:1px solid #9bbbe0;border-radius:7px;display:inline-flex;font-size:1rem;height:1.7rem;justify-content:center;width:1.7rem}.worker-summary .worker-job{flex:0 1 43%}.meta-icon{display:inline-block;font-size:.95em;margin-right:.28rem;vertical-align:-.08em}.learning-note{align-items:flex-start;background:#eaf2ff;border:1px solid #a8c5e4;border-left:5px solid var(--blue);border-radius:14px;display:flex;gap:.85rem;margin-top:1.5rem;padding:1rem 1.2rem}.learning-symbol{font-size:1.5rem;line-height:1}.learning-note h2{font-size:1.05rem}.learning-note p{color:var(--muted);font-size:.92rem;margin:.35rem 0 0}.learning-more{margin-top:.75rem}.learning-more summary{color:var(--blue-dark);cursor:pointer;font-size:.88rem;font-weight:750}.learning-more summary:focus-visible{outline:3px solid #8bb4df;outline-offset:3px}.learning-more p{margin-top:.35rem}.view-switcher{display:flex;gap:.5rem;margin:1.5rem 0 .75rem}.view-button{background:#fff;border:1px solid var(--line);border-radius:999px;color:var(--muted);cursor:pointer;font:700 .9rem system-ui;padding:.55rem .85rem}.view-button.active,.view-button[aria-selected="true"]{background:var(--blue);border-color:var(--blue);color:#fff}.view-button:focus-visible{outline:3px solid #8bb4df;outline-offset:2px}.bundle-actions{align-items:center;display:flex;flex-wrap:wrap;gap:.6rem;margin:1rem 0}.bundle-explainer{background:var(--blue-light);border-left:4px solid var(--blue);border-radius:10px;color:var(--muted);display:grid;gap:.15rem;margin-bottom:1rem;padding:.8rem 1rem}.bundle-explainer strong{color:var(--blue-dark)}.bundle-explainer code{color:var(--blue-dark);font:700 .85em ui-monospace,SFMono-Regular,Menlo,monospace}.bundle-grid{display:grid;gap:1rem;grid-template-columns:repeat(auto-fit,minmax(300px,1fr))}.bundle-card{background:#f9fcff;border:2px solid #75a8d8;border-radius:14px;padding:1rem}.bundle-card-head{align-items:flex-start;display:flex;gap:.75rem;justify-content:space-between}.bundle-card h3{color:var(--blue-dark);font-size:1rem}.bundle-card p{color:var(--muted);font-size:.82rem;margin:.2rem 0 0}.bundle-status{border-radius:999px;font-size:.76rem;font-weight:800;padding:.2rem .5rem;white-space:nowrap}.bundle-pending{background:#f1f3f6;color:var(--muted)}.bundle-running{background:#fff3df;color:var(--amber)}.bundle-completed{background:#eaf6ef;color:var(--green)}.bundle-failed{background:#fbeaea;color:#a33}.bundle-placement{color:var(--muted);display:flex;flex-wrap:wrap;font-size:.8rem;gap:.7rem;margin:.8rem 0}.bundle-pod{background:#fff;border:1px solid #9bbbe0;border-radius:11px;padding:.75rem}.bundle-pod-head{align-items:baseline;display:flex;gap:.4rem;justify-content:space-between}.bundle-pod-head strong{font:700 .8rem ui-monospace,SFMono-Regular,Menlo,monospace;overflow-wrap:anywhere}.bundle-pod-head span{color:var(--blue);font-size:.78rem;font-weight:750}.bundle-tasks{display:grid;gap:.45rem;margin-top:.6rem}.bundle-task{align-items:center;border-top:1px solid var(--line);display:grid;gap:.35rem;grid-template-columns:1fr auto;padding-top:.45rem}.bundle-task:first-child{border-top:0;padding-top:0}.bundle-task-name{font:700 .8rem ui-monospace,SFMono-Regular,Menlo,monospace}.bundle-task-detail{color:var(--muted);font-size:.77rem}.bundle-task-status{font-size:.76rem;font-weight:750}.bundle-task-status.completed{color:var(--green)}.bundle-task-status.processing,.bundle-task-status.waiting{color:var(--amber)}.bundle-task-status.failed{color:#a33}
.topology-icon{display:inline-block;font-size:1.05em;margin-right:.35rem;vertical-align:-.08em}.worker-role-icon{align-items:center;background:var(--blue-light);border:1px solid #9bbbe0;border-radius:7px;display:inline-flex;font-size:1rem;height:1.7rem;justify-content:center;width:1.7rem}.worker-summary .worker-job{flex:0 1 43%}.meta-icon{display:inline-block;font-size:.95em;margin-right:.28rem;vertical-align:-.08em}.learning-note{align-items:flex-start;background:#eaf2ff;border:1px solid #a8c5e4;border-left:5px solid var(--blue);border-radius:14px;display:flex;gap:.85rem;margin-top:1.5rem;padding:1rem 1.2rem}.learning-symbol{font-size:1.5rem;line-height:1}.learning-note h2{font-size:1.05rem}.learning-note p{color:var(--muted);font-size:.92rem;margin:.35rem 0 0}.learning-more{margin-top:.75rem}.learning-more summary{color:var(--blue-dark);cursor:pointer;font-size:.88rem;font-weight:750}.learning-more summary:focus-visible{outline:3px solid #8bb4df;outline-offset:3px}.learning-more p{margin-top:.35rem}.view-switcher{display:flex;gap:.5rem;margin:1.5rem 0 .75rem}.view-button{background:#fff;border:1px solid var(--line);border-radius:999px;color:var(--muted);cursor:pointer;font:700 .9rem system-ui;padding:.55rem .85rem}.view-button.active,.view-button[aria-selected="true"]{background:var(--blue);border-color:var(--blue);color:#fff}.view-button:focus-visible{outline:3px solid #8bb4df;outline-offset:2px}.kind-banner{background:#fff3df;border:1px solid #e4bf79;border-left:5px solid var(--amber);border-radius:10px;color:#7c4b00;font-size:.9rem;margin:1rem 0;padding:.75rem 1rem}.kind-banner.ready{background:#eaf6ef;border-color:#9bc9ad;border-left-color:var(--green);color:#12603c}.bundle-actions{align-items:center;display:flex;flex-wrap:wrap;gap:.6rem;margin:1rem 0}.bundle-explainer{background:var(--blue-light);border-left:4px solid var(--blue);border-radius:10px;color:var(--muted);display:grid;gap:.15rem;margin-bottom:1rem;padding:.8rem 1rem}.bundle-explainer strong{color:var(--blue-dark)}.bundle-explainer code{color:var(--blue-dark);font:700 .85em ui-monospace,SFMono-Regular,Menlo,monospace}.bundle-grid{display:grid;gap:1rem;grid-template-columns:repeat(auto-fit,minmax(300px,1fr))}.bundle-card{background:#f9fcff;border:2px solid #75a8d8;border-radius:14px;padding:1rem}.bundle-card-head{align-items:flex-start;display:flex;gap:.75rem;justify-content:space-between}.bundle-card h3{color:var(--blue-dark);font-size:1rem}.bundle-card p{color:var(--muted);font-size:.82rem;margin:.2rem 0 0}.bundle-status{border-radius:999px;font-size:.76rem;font-weight:800;padding:.2rem .5rem;white-space:nowrap}.bundle-pending{background:#f1f3f6;color:var(--muted)}.bundle-running{background:#fff3df;color:var(--amber)}.bundle-completed{background:#eaf6ef;color:var(--green)}.bundle-failed{background:#fbeaea;color:#a33}.bundle-placement{color:var(--muted);display:flex;flex-wrap:wrap;font-size:.8rem;gap:.7rem;margin:.8rem 0}.bundle-pod{background:#fff;border:1px solid #9bbbe0;border-radius:11px;padding:.75rem}.bundle-pod-head{align-items:baseline;display:flex;gap:.4rem;justify-content:space-between}.bundle-pod-head strong{font:700 .8rem ui-monospace,SFMono-Regular,Menlo,monospace;overflow-wrap:anywhere}.bundle-pod-head span{color:var(--blue);font-size:.78rem;font-weight:750}.bundle-tasks{display:grid;gap:.45rem;margin-top:.6rem}.bundle-task{align-items:center;border-top:1px solid var(--line);display:grid;gap:.35rem;grid-template-columns:1fr auto;padding-top:.45rem}.bundle-task:first-child{border-top:0;padding-top:0}.bundle-task-name{font:700 .8rem ui-monospace,SFMono-Regular,Menlo,monospace}.bundle-task-detail{color:var(--muted);font-size:.77rem}.bundle-task-status{font-size:.76rem;font-weight:750}.bundle-task-status.completed{color:var(--green)}.bundle-task-status.processing,.bundle-task-status.waiting{color:var(--amber)}.bundle-task-status.failed{color:#a33}
.badge.completed{background:#eaf6ef;color:var(--green)}.badge.failed{background:#fbeaea;color:#a33}
</style>
</head>
<body>
<main>
<span class="eyebrow">Docker-Praxisprojekt</span>
<h1>Docker-Skalierungsdemo</h1>
<p class="lead">Erzeuge Aufträge und beobachte, wie mehrere gleichartige Worker sie aus derselben Queue verarbeiten. Die Seite aktualisiert sich automatisch.</p>

<section class="quickstart" aria-labelledby="quickstart-title">
  <div><strong id="quickstart-title">So untersuchen Sie die Skalierung</strong>
    <ol><li>Erzeugen Sie unten einige Jobs.</li><li>Starten Sie danach weitere Worker mit dem angegebenen Befehl.</li><li>Beobachten Sie, wie neue Container als eigene Worker erscheinen.</li></ol>
  </div>
  <div id="scale-command" class="command">docker compose up -d --scale worker=4</div>
</section>

<section class="runtime" aria-labelledby="runtime-title">
  <div><span class="eyebrow">Aktive Laufzeit</span><h2 id="runtime-title">Wird ermittelt …</h2><p id="runtime-subtitle" class="runtime-subtitle">Die Platzierungsdaten werden aus dem Status gelesen.</p></div>
  <div id="runtime-facts" class="runtime-facts"><div class="runtime-fact"><span class="runtime-fact-label">Status</span><span class="runtime-fact-value">Lade Status …</span></div></div>
</section>

<nav class="view-switcher" aria-label="Lernansichten">
  <button class="view-button active" data-view="scale" aria-selected="true">Skalierung &amp; Platzierung</button>
  <button class="view-button" data-view="bundles" aria-selected="false">Auftrags-Bundles</button>
</nav>

<div id="scale-view" class="view-panel">

<section class="actions" aria-label="Jobs erzeugen">
  <h2>Aufträge erzeugen</h2>
  <button class="button" data-jobs="5">5 Jobs</button>
  <button class="button" data-jobs="20">20 Jobs</button>
  <button class="button" data-jobs="40">40 Jobs</button>
  <span id="message" class="message" role="status"></span>
</section>

<section class="stats" aria-label="Aktuelle Kennzahlen">
  <div class="stat"><div class="stat-label">Jobs in der Queue</div><div id="queue" class="stat-value">–</div></div>
  <div class="stat"><div class="stat-label">Insgesamt verarbeitet</div><div id="processed" class="stat-value">–</div></div>
  <div class="stat"><div class="stat-label">Aktive Worker</div><div id="worker-count" class="stat-value">–</div></div>
</section>

<section class="panel" aria-labelledby="workers-title">
  <div class="section-head"><h2 id="workers-title">Worker-Prozesse und Platzierung</h2><span id="updated" class="updated">Lade Status …</span></div>
  <div id="topology" class="topology"><div class="empty">Die Worker werden geladen …</div></div>
</section>

<section class="learning-note" aria-labelledby="learning-title">
  <div class="learning-symbol" aria-hidden="true">💡</div>
  <div><h2 id="learning-title">Warum gibt es Pods?</h2><p id="learning-note">Kubernetes platziert nicht einzelne Container, sondern Pods als kleinste Einheit des Schedulers. In dieser Demo enthält jeder Worker-Pod genau einen Worker-Container.</p><details id="learning-more" class="learning-more" hidden><summary id="learning-more-title">Warum nicht mehrere Worker-Container in einem Pod?</summary><p id="learning-more-note">Mehrere Container in einem Pod können parallel laufen. Sie teilen sich aber Node, Netzwerk, Volumes und Lebenszyklus. Für unabhängige Queue-Worker sind getrennte Pods sinnvoller: Kubernetes kann sie auf verschiedene KIND-Nodes verteilen, einzeln ersetzen und unabhängig skalieren.</p></details></div>
</section>

<section class="explanation" aria-label="Was ist zu beobachten">
  <div><h3>Queue</h3><p>Neue Jobs warten zunächst in Redis. Eine größere Zahl zeigt, dass mehr Arbeit ansteht.</p></div>
  <div><h3>Worker</h3><p>Jede Karte zeigt einen laufenden Container bzw. Pod/Task und den Prozess, der Jobs verarbeitet.</p></div>
  <div><h3>Skalierung</h3><p>Der Befehl oben passt sich an Compose, Swarm oder Kubernetes an.</p></div>
</section>

</div>

<div id="bundles-view" class="view-panel" hidden>
  <section class="panel bundle-panel" aria-labelledby="bundles-title">
    <div class="section-head"><div><h2 id="bundles-title">Auftrags-Bundles in Kubernetes/KIND</h2><p class="runtime-subtitle">Ein Bundle wird als Kubernetes-Job gestartet. Sein Pod enthält drei kooperierende Task-Container.</p></div><span id="bundle-runtime" class="updated">Wird geprüft …</span></div>
    <div id="kind-banner" class="kind-banner" role="status">Für diese Lernansicht: Docker Desktop → Einstellungen/Settings → Kubernetes → Kind aktivieren.</div>
    <div class="bundle-actions" aria-label="Bundles erzeugen">
      <button class="button" data-bundles="1">1 Bundle</button>
      <button class="button" data-bundles="3">3 Bundles</button>
      <button class="button" data-bundles="5">5 Bundles</button>
      <span id="bundle-message" class="message" role="status"></span>
    </div>
    <div class="bundle-explainer"><strong>Warum bleiben die Container zusammen?</strong><span>Sie teilen sich ein <code>emptyDir</code>-Arbeitsverzeichnis. <code>collect</code> und <code>analyze</code> arbeiten parallel; <code>assemble</code> liest ihre Ergebnisse aus demselben Pod-Volume.</span></div>
    <div id="bundles"><div class="empty">Noch kein Bundle erzeugt.</div></div>
  </section>
</div>

<p class="footer"><a href="/status">Technischer JSON-Status (/status)</a> · Diese Ansicht ist die grafische Alternative für die Untersuchung im Browser. · <a href="https://www.linkedin.com/in/daniel-bunzendahl/" rel="author">© 2026 Daniel Bunzendahl</a> · Apache-2.0</p>
</main>
<script>
const labels={waiting:'wartet auf Job',processing:'verarbeitet gerade',stopped:'gestoppt',completed:'fertig',failed:'Fehler'};
const orchestratorLabels={compose:'Docker Compose (--scale)',swarm:'Docker Swarm',kubernetes:'Kubernetes'};
const escapeHtml=value=>String(value??'').replace(/[&<>\"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','\"':'&quot;',"'":'&#39;'}[c]));
const shown=value=>value===undefined||value===null||value===''?'–':value;
const normalizeMode=value=>['compose','swarm','kubernetes'].includes(String(value||'').toLowerCase())?String(value).toLowerCase():'compose';
const modeLabel=value=>orchestratorLabels[normalizeMode(value)]||value||'Docker Compose (--scale)';
function renderRuntime(state){
  const runtime=state.runtime||{};
  const workerModes=[...new Set((state.workers||[]).map(worker=>worker.orchestrator).filter(Boolean))];
  const mode=normalizeMode(runtime.orchestrator||workerModes[0]);
  const scaleCommands={compose:'docker compose up -d --scale worker=4',swarm:'docker service scale demoscale_worker=4',kubernetes:'kubectl -n demoscale scale deployment worker --replicas=4'};
  document.querySelector('#runtime-title').textContent=modeLabel(mode);
  const subtitles={
    compose:'Eine Docker Engine mit mehreren gleichartigen Containern – genau der lokale --scale-Fall.',
    swarm:'Ein Rahmen steht für einen Swarm-Host; mehrere Rahmen zeigen die Verteilung über mehrere Hosts.',
    kubernetes:'KIND-Hosts enthalten Pods; die Karten darin zeigen die Container und ihre laufenden Prozesse.',
  };
  document.querySelector('#runtime-subtitle').textContent=workerModes.length>1?'Mehrere Laufzeitangaben in der Queue sichtbar.':subtitles[mode];
  document.querySelector('#scale-command').textContent=scaleCommands[mode]||scaleCommands.compose;
  const learningTitles={
    compose:'Was ist die Vergleichsebene?',
    swarm:'Was ist die Vergleichsebene?',
    kubernetes:'Warum gibt es Pods?',
  };
  const learningNotes={
    compose:'Compose skaliert Container direkt auf einer Docker Engine. Deshalb gibt es hier keine zusätzliche Pod-Hülle.',
    swarm:'Swarm plant Service-Tasks und startet daraus Container auf Nodes. Eine Pod-Ebene wie bei Kubernetes gibt es in Swarm nicht.',
    kubernetes:'Der Kubernetes-Scheduler platziert nicht einzelne Container, sondern Pods als kleinste Einheit. In dieser Demo ergeben vier Deployment-Replikas vier Pods mit jeweils einem Worker-Container. Der Pod bündelt Netzwerk, Volumes und Lebenszyklus.',
  };
  const learningMore=document.querySelector('#learning-more');
  learningMore.hidden=mode!=='kubernetes';
  document.querySelector('#learning-more-title').textContent='Warum nicht mehrere Worker-Container in einem Pod?';
  document.querySelector('#learning-more-note').textContent='Ja, mehrere Container in einem Pod können parallel laufen. Sie teilen sich aber Node, Netzwerk, Volumes und Lebenszyklus. Für unabhängige Queue-Worker sind getrennte Pods sinnvoller: Kubernetes kann sie auf verschiedene KIND-Nodes verteilen, einzeln ersetzen und unabhängig skalieren.';
  document.querySelector('#learning-title').textContent=learningTitles[mode];
  document.querySelector('#learning-note').textContent=learningNotes[mode];
  const facts=[
    ['Orchestrator',modeLabel(mode)],
    ['Host/Node',runtime.node],
    ['Pod',runtime.pod],
    ['Task',runtime.task],
    ['Container',runtime.container],
    ['Prozess',(runtime.process||'dashboard/app.py')+(runtime.pid?` (PID ${runtime.pid})`:'')],
  ];
  document.querySelector('#runtime-facts').innerHTML=facts.map(([label,value])=>`<div class="runtime-fact"><span class="runtime-fact-label">${escapeHtml(label)}</span><span class="runtime-fact-value">${escapeHtml(shown(value))}</span></div>`).join('');
}
function metaRow(label,value,icon=''){return `<div class="meta-row"><span class="meta-label">${icon?`<span class="meta-icon" aria-hidden="true">${icon}</span>`:''}${escapeHtml(label)}</span><span class="meta-value">${escapeHtml(shown(value))}</span></div>`;}
function groupBy(items,selector){
  return items.reduce((groups,item)=>{
    const key=selector(item)||'unbekannt';
    if(!groups.has(key))groups.set(key,[]);
    groups.get(key).push(item);
    return groups;
  },new Map());
}
function workerCard(worker,mode){
  const effectiveMode=normalizeMode(worker.orchestrator||mode);
  const status=worker.status||'waiting';
  const process=(worker.process||'worker.py')+(worker.pid?` (PID ${worker.pid})`:'');
  const currentJob=worker.current_job?`Job: ${worker.current_job}`:'Kein aktueller Job';
  const placement=effectiveMode==='kubernetes'
    ? metaRow('Container',worker.container||worker.name,'🐳')
    : effectiveMode==='swarm'
      ? metaRow('Task',worker.task||worker.name)+metaRow('Container',worker.container||worker.name,'🐳')
      : metaRow('Container',worker.container||worker.name,'🐳');
  const pod=effectiveMode==='kubernetes'?metaRow('Pod',worker.pod||worker.name):'';
  return `<details class="worker"><summary class="worker-summary"><span class="worker-role-icon" aria-label="Docker-Container">🐳</span><span class="worker-role-icon" aria-label="Prozess">⚙️</span><span class="worker-name">${escapeHtml(worker.name)}</span><span class="worker-job">${escapeHtml(currentJob)}</span><span class="badge ${escapeHtml(status)}">${escapeHtml(labels[status]||status)}</span></summary><div class="worker-details"><div class="worker-meta">${metaRow('Laufzeit',modeLabel(effectiveMode))}${metaRow('Node',worker.node,'🖥️')}${pod}${placement}${metaRow('Prozess',process,'⚙️')}${metaRow('Job',worker.current_job)}${metaRow('Verarbeitet',worker.processed||0)}</div></div></details>`;
}
function nodeGroup(node,workers,mode,subtitleOverride=''){
  const title=mode==='compose'?'Docker Engine':mode==='swarm'?'Swarm-Node':'KIND-Host / Kubernetes-Node';
  const subtitle=subtitleOverride|| (mode==='compose'?'Mehrere Container auf demselben Host':mode==='swarm'?'Swarm-Tasks werden auf diesem Host ausgeführt':'Pods sind die kleinste von Kubernetes platzierte Einheit; hier jeweils mit einem Worker-Container');
  if(mode==='kubernetes'){
    const pods=[...groupBy(workers,worker=>worker.pod||worker.name)];
    return `<section class="node-group ${mode}"><div class="node-title"><strong><span class="topology-icon" aria-hidden="true">🖥️</span>${escapeHtml(title)}</strong><span>${escapeHtml(shown(node))}</span></div><p class="node-subtitle">${subtitle}</p><div class="pod-grid">${pods.map(([podName,podWorkers])=>`<section class="pod-group"><div class="pod-title"><span>Pod</span><strong>${escapeHtml(podName)}</strong></div><div class="task-grid">${podWorkers.map(worker=>workerCard(worker,mode)).join('')}</div></section>`).join('')}</div></section>`;
  }
  return `<section class="node-group ${mode}"><div class="node-title"><strong><span class="topology-icon" aria-hidden="true">🖥️</span>${escapeHtml(title)}</strong><span>${escapeHtml(shown(node))}</span></div><p class="node-subtitle">${subtitle}</p><div class="task-grid">${workers.map(worker=>workerCard(worker,mode)).join('')}</div></section>`;
}
function renderTopology(state){
  const workers=state.workers||[];
  const workerModes=[...new Set(workers.map(worker=>worker.orchestrator).filter(Boolean))];
  const mode=normalizeMode(state.runtime?.orchestrator||workerModes[0]);
  const title=modeLabel(mode);
  const note=mode==='compose'?'Eine Engine · mehrere Container':mode==='swarm'?'Hosts/Nodes · Swarm-Tasks · Container':'KIND-Hosts · Pods · Container/Prozesse';
  let body='';
  if(mode==='compose'){
    const node=state.runtime?.node||workers[0]?.node||'local-engine';
    body=nodeGroup(node,workers,mode);
  }else{
    body=[...groupBy(workers,worker=>worker.node)].map(([node,nodeWorkers])=>nodeGroup(node,nodeWorkers,mode)).join('');
  }
  document.querySelector('#topology').innerHTML=topologyShell(mode,title,note,body);
}
function topologyShell(mode,title,note,body){
  return `<div class="topology-shell ${mode}"><div class="topology-header"><div><h3>${escapeHtml(title)}</h3><p>${escapeHtml(note)}</p></div><span class="topology-mode">${mode==='kubernetes'?'Kind-Modus':mode==='swarm'?'Service-Modus':'Lokale Replik'}</span></div><div class="node-grid">${body}</div></div>`;
}
function bundleTaskStatus(status){
  if(status==='completed')return 'completed';
  if(status==='failed')return 'failed';
  if(status==='processing')return 'processing';
  return 'waiting';
}
function bundleWorkers(bundle){
  const pod=bundle.pod||`bundle-${bundle.id}`;
  return (bundle.tasks||[]).map(task=>{
    const role=task.role||task.container||'task';
    return {
      name:task.container||role,
      orchestrator:'kubernetes',
      node:bundle.node||'noch nicht geplant',
      pod,
      container:task.container||role,
      process:task.process||'bundle/app.py',
      current_job:task.message||role,
      status:bundleTaskStatus(task.status),
      processed:task.status==='completed'?1:0,
    };
  });
}
function renderBundles(state){
  const mode=normalizeMode(state.runtime?.orchestrator);
  const container=document.querySelector('#bundles');
  const buttons=document.querySelectorAll('[data-bundles]');
  const enabled=mode==='kubernetes';
  buttons.forEach(button=>button.disabled=!enabled);
  document.querySelector('#bundle-runtime').textContent=enabled?'Kubernetes/KIND aktiv':'Nur Kubernetes/KIND verfügbar';
  const banner=document.querySelector('#kind-banner');
  banner.classList.toggle('ready',enabled);
  banner.textContent=enabled?'Kind-Modus erkannt. Die Bundle-Jobs können als Multi-Container-Pods gestartet werden.':'Diese Dashboard-Instanz läuft nicht in Kubernetes. KIND starten, die Kubernetes-Manifeste anwenden und das Dashboard per Port-Forward öffnen.';
  if(!enabled){container.innerHTML='<div class="empty">Ein laufender KIND-Cluster allein genügt nicht: Diese Ansicht wird aktiv, sobald das Dashboard selbst als Kubernetes-Pod läuft.</div>';return;}
  const bundles=state.bundles||[];
  if(!bundles.length){container.innerHTML='<div class="empty">Noch kein Bundle erzeugt. Starte ein Bundle, um einen Pod mit drei Task-Containern zu beobachten.</div>';return;}
  const workers=bundles.flatMap(bundleWorkers);
  const nodes=[...groupBy(workers,worker=>worker.node)];
  const nodeMarkup=nodes.map(([node,nodeWorkers])=>nodeGroup(node,nodeWorkers,'kubernetes','Pods sind die kleinste von Kubernetes platzierte Einheit; jedes Bundle bildet hier einen Pod mit drei kooperierenden Containern.')).join('');
  container.innerHTML=topologyShell('kubernetes','Kubernetes','KIND-Hosts · Pods · Container/Prozesse',nodeMarkup);
}
function render(state){
  renderRuntime(state);
  document.querySelector('#queue').textContent=state.queue_length;
  document.querySelector('#processed').textContent=state.processed;
  document.querySelector('#worker-count').textContent=state.workers.length;
  document.querySelector('#updated').textContent='Zuletzt aktualisiert: '+new Date().toLocaleTimeString('de-DE');
  const container=document.querySelector('#topology');
  if(!state.workers.length){container.innerHTML='<div class="empty">Noch kein Worker sichtbar. Starten Sie den Stack oder Compose mit --scale.</div>';}
  else{renderTopology(state);}
  renderBundles(state);
}
async function refresh(){
  try{const response=await fetch('/status');if(!response.ok)throw new Error('Status '+response.status);render(await response.json());}
  catch(error){document.querySelector('#updated').textContent='Status momentan nicht erreichbar';console.error(error);}
}
async function enqueue(amount,button){
  button.disabled=true;document.querySelector('#message').textContent=amount+' Jobs werden erzeugt …';
  try{const response=await fetch('/enqueue?n='+amount);if(!response.ok)throw new Error('Producer '+response.status);document.querySelector('#message').textContent=amount+' Jobs wurden in die Queue gelegt.';await refresh();}
  catch(error){document.querySelector('#message').textContent='Jobs konnten nicht erzeugt werden.';console.error(error);}
  finally{button.disabled=false;}
}
async function enqueueBundles(amount,button){
  button.disabled=true;document.querySelector('#bundle-message').textContent=amount+' Bundle(s) werden als Kubernetes-Jobs erzeugt …';
  try{const response=await fetch('/bundle?n='+amount);const result=await response.json();if(!response.ok)throw new Error(result.error||'Bundle '+response.status);document.querySelector('#bundle-message').textContent=result.created+' Bundle(s) wurden gestartet.';await refresh();}
  catch(error){document.querySelector('#bundle-message').textContent=error.message||'Bundles konnten nicht erzeugt werden.';console.error(error);}
  finally{button.disabled=false;}
}
function setView(view){
  document.querySelector('#scale-view').hidden=view!=='scale';
  document.querySelector('#bundles-view').hidden=view!=='bundles';
  document.querySelectorAll('[data-view]').forEach(button=>{const active=button.dataset.view===view;button.classList.toggle('active',active);button.setAttribute('aria-selected',String(active));});
}
document.querySelectorAll('[data-jobs]').forEach(button=>button.addEventListener('click',()=>enqueue(button.dataset.jobs,button)));
document.querySelectorAll('[data-bundles]').forEach(button=>button.addEventListener('click',()=>enqueueBundles(button.dataset.bundles,button)));
document.querySelectorAll('[data-view]').forEach(button=>button.addEventListener('click',()=>setView(button.dataset.view)));
refresh();setInterval(refresh,2000);
</script>
</body>
</html>
"""


class Handler(BaseHTTPRequestHandler):
    def send_bytes(self, body, content_type="application/json; charset=utf-8", status=200):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def status_payload(self):
        # Die GUI fragt diesen JSON-Endpunkt regelmäßig ab. Damit bleibt die
        # Beobachtungslogik von der Darstellung im Browser getrennt.
        workers = []
        for key in sorted(redis_client.scan_iter("scale:worker:*")):
            worker = redis_client.hgetall(key)
            if worker:
                workers.append(worker)
        return {
            "runtime": {
                "orchestrator": ORCHESTRATOR,
                "node": NODE_NAME,
                "pod": POD_NAME,
                "namespace": POD_NAMESPACE,
                "task": TASK_NAME,
                "slot": TASK_SLOT,
                "container": CONTAINER_NAME,
                "process": "dashboard/app.py",
                "pid": os.getpid(),
            },
            "queue_length": redis_client.llen(QUEUE_KEY),
            "processed": int(redis_client.get("scale:processed") or 0),
            "workers": workers,
            "bundles": bundle_statuses(),
        }

    def do_GET(self):  # noqa: N802 - required by BaseHTTPRequestHandler
        parsed = urlparse(self.path)
        if parsed.path == "/health":
            redis_client.ping()
            self.send_bytes(json.dumps({"status": "ok"}).encode("utf-8"))
            return
        if parsed.path == "/status":
            self.send_bytes(json.dumps(self.status_payload()).encode("utf-8"))
            return
        if parsed.path == "/bundle":
            try:
                amount = int(parse_qs(parsed.query).get("n", ["1"])[0])
                amount = max(1, min(amount, 5))
                bundle_ids = [create_bundle() for _ in range(amount)]
                self.send_bytes(
                    json.dumps({"created": len(bundle_ids), "bundles": bundle_ids}).encode("utf-8"),
                    status=201,
                )
            except RuntimeError as error:
                self.send_bytes(json.dumps({"error": str(error)}).encode("utf-8"), status=409)
            except Exception as error:  # noqa: BLE001 - endpoint returns a readable test error
                print(f"dashboard: bundle creation failed: {error}", flush=True)
                self.send_bytes(json.dumps({"error": "Bundle konnte nicht erzeugt werden."}).encode("utf-8"), status=500)
            return
        if parsed.path == "/enqueue":
            # The dashboard calls producer by its Compose DNS name. No
            # producer port is published to the host.
            query = parsed.query or "n=12"
            with urlopen(f"{PRODUCER_URL}/enqueue?{query}", timeout=5) as response:
                self.send_bytes(response.read())
            return
        self.send_bytes(HTML.encode("utf-8"), "text/html; charset=utf-8")

    def log_message(self, format, *args):
        print(f"dashboard: {format % args}", flush=True)


if __name__ == "__main__":
    server = ThreadingHTTPServer(("0.0.0.0", 8080), Handler)
    print("dashboard listening on :8080", flush=True)
    server.serve_forever()
