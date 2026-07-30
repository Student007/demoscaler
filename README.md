# Demoscale – Docker-Skalierungsdemo

Demoscale ist ein lokales Lehrbeispiel für horizontale Skalierung:

- Das Dashboard nimmt Aufträge an.
- Der Producer legt sie in einer Redis-Queue ab.
- Mehrere identische Worker verarbeiten die Aufträge parallel.

Im Browser werden Queue-Länge, verarbeitete Jobs und die Platzierung der
Worker sichtbar. Die Demo ist nicht für den Produktivbetrieb vorgesehen.

## Voraussetzungen

- Docker Desktop
- Docker Compose v2
- für die Kubernetes-Variante: `kubectl` und Docker Desktop Kubernetes

Alle Befehle werden im geklonten Projektverzeichnis ausgeführt.

## Schnellstart mit vier Workern

Die veröffentlichten Images werden automatisch geladen. Starten Sie die Demo
mit vier Worker-Containern. Dieser Einzeiler kann vollständig kopiert und
eingefügt werden:

```bash
docker compose up -d --no-build --scale worker=4 && docker compose ps
```

Öffnen Sie anschließend <http://localhost:8080>.

Erzeugen Sie Aufträge im Dashboard oder im Terminal:

```bash
curl "http://localhost:8080/enqueue?n=40"
docker compose logs --tail=50 worker
```

Die vier Worker verwenden dasselbe Image, laufen aber in getrennten
Containern und verarbeiten gemeinsam die Redis-Queue.

> **Wichtig:** Bei Docker Compose sind dies vier Worker-Container auf einer
> Docker Engine – keine vier physischen Rechner oder Kubernetes-Nodes.

## Kubernetes und KIND aktivieren

KIND steht für „Kubernetes in Docker“. Docker Desktop betreibt dabei die
Kubernetes-Nodes als Container. So lässt sich lokal untersuchen, wie
Kubernetes Deployments, Pods und Container auf mehrere Nodes verteilt.

### 1. KIND in Docker Desktop einschalten

1. Docker Desktop öffnen.
2. **Settings/Einstellungen → Kubernetes** auswählen.
3. **Enable Kubernetes** aktivieren.
4. Als Bereitstellungsmethode **KIND** auswählen.
5. Unter **Nodes** vier Nodes einstellen.
6. Die Einstellungen anwenden und warten, bis der Cluster **Running** meldet.

Prüfen Sie danach den Kontext und die Nodes:

```bash
kubectl config use-context docker-desktop
kubectl get nodes
```

### 2. Demoscale in Kubernetes starten

Wenn KIND bereits läuft, startet der folgende Einzeiler die vollständige
Kubernetes-Variante. Er beendet zuerst Compose, wählt den Docker-Desktop-
Kontext, wendet die Manifeste an, wartet auf alle Deployments und richtet
anschließend den Port-Forward ein:

```bash
docker compose down && kubectl config use-context docker-desktop && kubectl apply -k 'https://github.com/Student007/demoscale//kubernetes?ref=1.2.9' && kubectl -n demoscale wait --for=condition=Available deployment --all --timeout=180s && kubectl -n demoscale get pods -o wide && kubectl -n demoscale port-forward service/dashboard 8080:8080
```

Der letzte Teil des Einzeilers hält das Terminal absichtlich geöffnet. Solange
der Port-Forward läuft, ist das Kubernetes-Dashboard unter
<http://localhost:8080> erreichbar. Beenden Sie den Port-Forward mit
`Ctrl+C`.

### Was wird simuliert?

Die KIND-Variante veranschaulicht:

- die Verteilung mehrerer Worker-Pods auf vier Kubernetes-Nodes;
- die selbstständige Verwaltung und Ersetzung von Pods durch Deployments;
- die Kommunikation über Kubernetes-Services;
- eine persistente Redis-Queue über ein Volume;
- Kubernetes-Jobs mit Multi-Container-Pods.

In der Ansicht **Auftrags-Bundles** erzeugt ein Auftrag einen Kubernetes-Job.
Sein Pod enthält drei kooperierende Container:

- `collect` sammelt Daten;
- `analyze` verarbeitet Daten;
- `assemble` führt die Ergebnisse zusammen.

Die Container teilen sich ein `emptyDir`-Volume. Dadurch zeigt die Demo den
Unterschied zwischen unabhängigen Worker-Pods und mehreren eng
zusammenarbeitenden Containern in einem Pod.

KIND simuliert die Kubernetes-Platzierung lokal. Die vier Nodes sind keine
vier eigenständigen physischen Rechner.

## Nützliche Prüfungen

Compose:

```bash
docker compose ps
docker compose logs --tail=50 worker
docker compose config --images
```

Kubernetes:

```bash
kubectl -n demoscale get pods -o wide
kubectl -n demoscale get deployments,services,pvc
kubectl -n demoscale logs deployment/worker --tail=50
```

## Sicheres Cleanup

Nur die Compose-Ressourcen dieser Demo entfernen:

```bash
docker compose down --remove-orphans
```

Das Redis-Volume bleibt dabei erhalten.

Nur die Kubernetes-Ressourcen dieser Demo entfernen:

```bash
kubectl delete namespace demoscale --ignore-not-found
```

Der KIND-Cluster und andere Docker-Projekte bleiben bestehen. Vermeiden Sie
für diese Demo globale Befehle wie `docker system prune -a --volumes` sowie
**Reset cluster**, da diese auch andere Ressourcen oder Projekte betreffen
können.

## Lizenz

Copyright © 2026 [Daniel Bunzendahl](https://www.linkedin.com/in/daniel-bunzendahl/).

Das Projekt steht unter der [Apache License 2.0](LICENSE). Ergänzende Hinweise
enthält [NOTICE](NOTICE).
