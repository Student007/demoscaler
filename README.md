# Demoscaler

Diese Demo zeigt, wie mehrere Worker-Container Aufträge aus einer gemeinsamen
Redis-Queue verarbeiten. Das Dashboard ist eine grafische Alternative zu
`docker compose ps` und `docker compose logs`.

## Schnellstart

1. Laden Sie dieses Repository über **Code → Download ZIP** herunter.
2. Entpacken Sie das ZIP und öffnen Sie ein Terminal im entpackten Ordner.
3. Starten Sie die Demo:

   ```bash
   docker compose up -d --pull always --scale worker=1
   ```

4. Öffnen Sie <http://localhost:8080>.

## Skalierung untersuchen

Erzeugen Sie Aufträge über die Schaltflächen im Dashboard und starten Sie
anschließend vier Worker:

```bash
docker compose up -d --scale worker=4
docker compose ps
docker compose logs --tail=50 worker
```

Die Worker verwenden dasselbe Image und greifen intern über `queue:6379` auf
Redis zu. Deshalb benötigen sie keine eigenen Host-Ports. Nur das Dashboard
veröffentlicht Port 8080.

## Beenden und zurücksetzen

```bash
docker compose down
docker compose down -v
```

`docker compose down -v` entfernt zusätzlich den gespeicherten Redis-Zustand.
