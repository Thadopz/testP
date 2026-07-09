# Observability

## Local Stack

Start Kafka, etcd, Prometheus, and Grafana:

```powershell
docker compose up -d
```

Default endpoints:

- Prometheus: http://localhost:9090
- Grafana: http://localhost:3000
- Grafana login: `admin` / `admin`

Prometheus scrapes local processes through `host.docker.internal`:

- node: `:9101`
- controller: `:9102`
- producer: `:9103`

Run the binaries with their default metrics ports, or set `-metrics-addr ""` to disable metrics.

## Alerts

Alert rules live in `alerts.yml` and cover:

- no controller leader
- node/controller metrics endpoint down
- shard lag over threshold
- event apply errors
- fencing rejects
- producer errors
- controller sweep errors
- shards without owners

Validate Prometheus config and rules:

```powershell
docker run --rm --entrypoint promtool -v "${PWD}\observability:/etc/prometheus:ro" prom/prometheus:v2.55.1 check config /etc/prometheus/prometheus.yml
docker run --rm --entrypoint promtool -v "${PWD}\observability:/etc/prometheus:ro" prom/prometheus:v2.55.1 check rules /etc/prometheus/alerts.yml
```
