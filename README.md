# Demoscale – Docker-Skalierungsdemo

Diese Demo zeigt horizontale Skalierung mit Docker Compose: Ein Dashboard nimmt
Aufträge an, ein Producer legt sie in Redis ab und mehrere identische Worker
holen die Aufträge aus derselben Queue. Im Browser werden Queue-Länge,
verarbeitete Jobs und die einzelnen Worker-Container sichtbar.

Die Demo ist ein lokales Lehrbeispiel, kein produktionsfertiger Queue- oder
Monitoringdienst.


## Lokal starten

### Schnellstart mit einem Befehl

Wenn die veröffentlichten Images verwendet werden sollen, genügt aus diesem Ordner ein einziger Befehl:

```bash
docker compose pull && docker compose up -d --no-build --scale worker=1 && docker compose ps
```

Eine `.env`-Datei ist für diesen Schnellstart nicht erforderlich; die Datei `.env.example` dient nur zur sichtbaren Anpassung von Port, Jobanzahl, Bearbeitungszeit oder Registry-Namespace.

### Lokaler Build

Für die lokale Entwicklung oder eine Änderung am Quellcode verwenden Sie:

```bash
cp .env.example .env
docker compose config
docker compose up -d --build --scale worker=1
docker compose ps
```

Öffnen Sie anschließend <http://localhost:8080>. Der Port kann in `.env` über `DASHBOARD_PORT` geändert werden. Der technische JSON-Endpunkt ist unter <http://localhost:8080/status> erreichbar.

## Skalierung beobachten

Erzeugen Sie Jobs in der Browser-GUI oder per Kommandozeile:

```bash
curl "http://localhost:8080/enqueue?n=20"
docker compose logs --tail=30 worker
docker compose up -d --scale worker=4
curl "http://localhost:8080/enqueue?n=40"
docker compose ps
docker compose logs --tail=50 worker
```

Das Dashboard zeigt zusätzlich die erkannte Laufzeit (`Docker Compose
(--scale)`, `Docker Swarm` oder `Kubernetes`) sowie pro Worker Node, Pod/Task,
Container, Prozess, PID, aktuellen Job und die bisher verarbeitete Anzahl.
Beim normalen Compose-Betrieb sind Pod und Task leer; der Node heißt
standardmäßig `local-engine`. Mit `COMPOSE_NODE_NAME` kann für eine lokale
Engine ein eigener Name angezeigt werden:

```bash
COMPOSE_NODE_NAME=macbook-air docker compose up -d --build --scale worker=4
```

Für Swarm werden Node und Task über die Swarm-Templates gemeldet. Für
Kubernetes kommen Pod, Namespace und Node über die Downward API in die
Worker-Container. Die Zusatzdaten sind optional; ohne diese Variablen läuft
das Worker-Image weiterhin als normales Compose-Containerprogramm.

Die GUI ordnet die Worker passend zur Abbildung in verschachtelte Rahmen ein:
Bei Compose steht die lokale Docker Engine außen und die Worker-Container
innen. Bei Swarm entspricht jeder äußere Rahmen einem Host/Node und jede Karte
einem Swarm-Task mit Container. Bei Kubernetes/KIND werden die Worker erst
nach KIND-Host/Node und darin nach Pod gruppiert; die Karte zeigt den
Container, den Prozess und den aktuellen Job.

Wenn die geänderten Python- und Dashboard-Dateien in Swarm oder Kubernetes
genutzt werden sollen, müssen die drei Images neu gebaut und in die Registry
gepusht werden. Für einen neuen Teststand können Sie einen neuen Tag verwenden:

```bash
export IMAGE_TAG=1.2.8
docker buildx bake --push
```

Danach setzen Sie bei Kubernetes die drei Deployments auf die neuen Tags oder
verwenden bei Swarm den neuen `IMAGE_TAG` beim nächsten `docker stack deploy`.

Die vier Worker verwenden dasselbe Image, laufen aber in eigenen Containern.
Alle greifen per Compose-Service-DNS auf `queue:6379` zu; feste Container-IP-
Adressen sind nicht erforderlich. `BRPOP` blockiert, bis ein Auftrag vorhanden
ist. `PROCESS_SECONDS` in `.env` steuert die simulierte Bearbeitungszeit.

## Wichtige Untersuchungsbefehle

```bash
docker compose config
docker compose ps
docker compose logs --tail=50 worker
docker compose exec --index 1 worker sh
docker inspect demoscale
docker volume inspect demoscale-queue
```

`docker compose down` entfernt Container und Netzwerk, lässt das Named Volume
aber bestehen. `docker compose down -v` entfernt zusätzlich `demoscale-queue`
und damit den gespeicherten Redis-Zustand.

## Images und Docker Hub

Die Demo besteht aus drei Haupt-Images und einem Kubernetes-Bundle-Image:

| Dienst | Tag im Docker-Hub-Repository `danbu/demoscale` | Aufgabe |
|---|---|---|
| `dashboard` | `demoscale-dashboard-1.2.8` | Browser-GUI und Status |
| `producer` | `demoscale-producer-1.2.8` | Aufträge erzeugen |
| `worker` | `demoscale-worker-1.2.8` | Aufträge verarbeiten |
| `bundle-task` | `demoscale-bundle-task-1.2.8` | Task-Container im Multi-Container-Pod |

`queue` verwendet weiterhin das offizielle Image `redis:7.4.2-alpine3.21`.
Ein Docker-Hub-Repository kann mehrere Tags aufnehmen; dadurch bleiben die
Dienste im Repository `demoscale` getrennt adressierbar.
Die ausführbare Docker-Hub-Beschreibung steht in
[`README.dockerhub.md`](README.dockerhub.md).

### Multi-Platform-Images bauen und pushen

Erstellen Sie auf Docker Hub ein Repository mit dem Namen `demoscale` (Docker Hub:
`My Hub` → `Create repository`). Verwenden Sie für die drei Dienste die
unterschiedlichen Tag-Präfixe. Bei `docker login` ist ein Docker-Hub-
Access-Token als Passwort die sichere Wahl.

```bash
export REGISTRY_USER=danbu
export IMAGE_REPOSITORY=demoscale
export IMAGE_TAG=1.2.8

docker login
docker buildx create --name demoscale-builder --driver docker-container --use
docker buildx inspect --bootstrap
docker buildx bake --push
```

Der Bake-Build veröffentlicht jeweils `linux/amd64` und `linux/arm64`.
Prüfen Sie danach zum Beispiel:

```bash
docker buildx imagetools inspect \
  "$REGISTRY_USER/$IMAGE_REPOSITORY:demoscale-worker-$IMAGE_TAG"
```

Wenn der Builder bereits existiert, verwenden Sie statt `docker buildx create`
einfach `docker buildx use demoscale-builder`.

### Demo mit den veröffentlichten Images starten

Setzen Sie in `.env` den Docker-Hub-Namespace und den veröffentlichten Tag:

```dotenv
REGISTRY_USER=danbu
IMAGE_REPOSITORY=demoscale
IMAGE_TAG=1.2.8
```

Laden und starten Sie anschließend ohne lokalen Neubau:

```bash
docker compose pull
docker compose up -d --no-build --scale worker=4
docker compose ps
```

Für die lokale Entwicklung genügt dagegen `docker compose up -d --build`.

## Mehrere physische Rechner: optionaler Swarm-Transfer

Compose skaliert auf einer einzelnen Docker Engine. Für mehrere Hosts müssen
die drei Images aus Docker Hub erreichbar sein:

```bash
export REGISTRY_USER=danbu
export IMAGE_REPOSITORY=demoscale
export IMAGE_TAG=1.2.8
docker swarm init --advertise-addr <manager-ip>
docker stack deploy -c stack.swarm.yaml demoscale
docker service ps demoscale_worker
docker service scale demoscale_worker=4
```

Das Redis-Volume ist lokal und damit nicht automatisch hostübergreifend nutzbar.
Für einen echten Mehrhost-Betrieb braucht Redis Shared Storage oder eine
bewusste Platzierung auf einem Knoten.

## Kubernetes-Test: alle Bestandteile als Container

Für Kubernetes liegt unter `kubernetes/` eine bewusst direkte
Vergleichskonfiguration. Redis läuft als eigener `StatefulSet` mit einem persistenten
Volume; Producer, Dashboard und Worker laufen als `Deployment`. Die Services
`queue`, `producer` und `dashboard` liefern die gleichen DNS-Namen und Ports
wie Compose und Swarm. Auf dem Host müssen deshalb weder Python noch Redis
installiert werden.

Voraussetzung ist ein laufender Kubernetes-Cluster mit `kubectl` und einer
verfügbaren Standard-StorageClass für das Redis-Volume. Die drei eigenen
Images müssen in einer Registry erreichbar sein; das Cluster baut beim
Anwenden der Manifeste keine Images.

```bash
cd begleitmaterial/docker-skalierungsdemo
kubectl apply -k kubernetes/
kubectl -n demoscale get pods,pvc,svc
kubectl -n demoscale port-forward service/dashboard 8080:8080
```

Öffnen Sie danach <http://localhost:8080>. In einem zweiten Terminal können
Sie die Demo untersuchen und die Worker skalieren:

```bash
curl "http://localhost:8080/enqueue?n=40"
kubectl -n demoscale scale deployment worker --replicas=6
kubectl -n demoscale get pods -o wide
kubectl -n demoscale logs deployment/worker --tail=30
```

Die Manifeste verwenden standardmäßig die veröffentlichten Images aus
`danbu/demoscale` mit Tag `1.2.8`. Für ein anderes Registry-Namespace können
Sie die drei Image-Felder in `kubernetes/application.yaml` anpassen oder nach
dem Anwenden die Deployments ändern:

Die App-Container werden in Kubernetes mit `runAsNonRoot` sowie UID/GID
`10001` gestartet. Diese numerische Angabe ergänzt die nicht-root-Konfiguration
der Dockerfiles und ist für Container-Runtimes eindeutiger als nur der
Benutzername `app`.

```bash
kubectl -n demoscale set image deployment/producer \
  producer="$REGISTRY_USER/$IMAGE_REPOSITORY:demoscale-producer-$IMAGE_TAG"
kubectl -n demoscale set image deployment/dashboard \
  dashboard="$REGISTRY_USER/$IMAGE_REPOSITORY:demoscale-dashboard-$IMAGE_TAG"
kubectl -n demoscale set image deployment/worker \
  worker="$REGISTRY_USER/$IMAGE_REPOSITORY:demoscale-worker-$IMAGE_TAG"
```

### Kubernetes-Lernansicht: Auftrags-Bundles

In der GUI können Sie zwischen `Skalierung & Platzierung` und
`Auftrags-Bundles` umschalten. Die zweite Ansicht ist nur mit Docker Desktop
Kubernetes im **Kind-Modus** aktiv. Öffnen Sie dafür `Docker Desktop` →
`Einstellungen/Settings` → `Kubernetes` und wählen Sie `Kind`.

Ein erzeugtes Bundle wird als Kubernetes-`Job` gestartet. Der Job erzeugt
einen Pod mit drei parallelen Task-Containern: `collect`, `analyze` und
`assemble`. Alle drei mounten dasselbe `emptyDir`-Volume. `collect` und
`analyze` schreiben parallel Ergebnisse; `assemble` wartet auf beide Dateien
und baut daraus das Bundle-Ergebnis.

Damit wird der Unterschied sichtbar:

```text
Worker-Skalierung:  4 Pods × 1 Worker-Container
Auftrags-Bundle:    1 Pod  × 3 kooperierende Task-Container
```

Die Bundle-Ansicht ist kein zweiter unabhängiger Dienst: Die Dashboard-GUI
erzeugt die Jobs über ihre Kubernetes-ServiceAccount-Berechtigung und liest
die Task-Statusdaten aus Redis.

Fertige oder fehlgeschlagene Bundles bleiben noch 8 Sekunden sichtbar und
werden danach aus der GUI sowie durch die Kubernetes-TTL-Bereinigung entfernt.

Redis bleibt absichtlich ein einzelner, persistenter Dienst. Das ist für die
Skalierungsdemo ausreichend, aber keine Redis-HA-Konfiguration. In Swarm ist
das Named Volume lokal und wird deshalb auf dem Manager gehalten; in
Kubernetes übernimmt die StorageClass die Bereitstellung des PVCs. Für
produktiven Mehrhost-Betrieb wären Shared Storage, Redis Sentinel/Cluster
oder ein externer Redis-Dienst nötig.

Zum vollständigen Aufräumen des Kubernetes-Tests einschließlich Redis-Daten:

```bash
kubectl delete namespace demoscale
```

## Aufräumen

```bash
docker compose down
docker compose down -v
docker buildx prune
```

Der zweite Compose-Befehl löscht bewusst den Queue-Zustand; der letzte Befehl
bereinigt nicht mehr benötigte Build-Cache-Daten.

## Dateien

| Datei | Zweck |
|---|---|
| `compose.yaml` | lokale Services, Netzwerk, Healthchecks und Skalierung |
| `dashboard/app.py` | Browser-GUI und `/status` |
| `producer/app.py` | HTTP-Endpunkt und Redis-Queue |
| `worker/app.py` | blockierender Queue-Konsument und Heartbeat |
| `docker-bake.hcl` | Multi-Platform-Build und Push der drei Images |
| `stack.swarm.yaml` | optionales Swarm-Manifest |
| `kubernetes/` | Kustomize-Manifeste für Redis, Deployments und Services |
| `.env.example` | lokale Konfigurationsvorlage |
| `README.dockerhub.md` | Text für die Docker-Hub-Repository-Übersicht |

## Stand der Demo

Die aktuelle Struktur ist vollständig für die lokale Untersuchung vorbereitet:

- `queue` verwendet Redis 7.4.2 mit AOF und einem persistenten Named Volume.
- `producer`, `dashboard` und `worker` werden aus eigenen Dockerfiles gebaut.
- Die Worker sind zustandsarm und können mit `--scale worker=N` vervielfacht
  werden.
- Healthchecks und `depends_on` mit Health-Bedingung steuern den Compose-Start.
- Die eigenen Images können mit Buildx Bake als `linux/amd64`- und
  `linux/arm64`-Images veröffentlicht werden.
- `stack.swarm.yaml` zeigt den optionalen Transfer auf Docker Swarm.

Der Webcontainer heißt exakt `demoscale`. Die automatisch erzeugten Worker
heißen beispielsweise `demoscale-worker-1` bis `demoscale-worker-4`. Ein fester
`container_name` für Worker wäre ungeeignet, weil Docker dann keine mehreren
Replikas mit demselben Namen starten könnte.

## Copyright und Lizenz

Copyright © 2026 [Daniel Bunzendahl](https://www.linkedin.com/in/daniel-bunzendahl/).

Dieses Projekt steht unter der [Apache License, Version 2.0](LICENSE).
Die ergänzenden Urheber- und Attributionshinweise stehen in [NOTICE](NOTICE).