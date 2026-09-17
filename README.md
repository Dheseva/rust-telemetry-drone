# rust-telemetry-drone

docker run --rm --name jaeger -p 16686:16686 -p 4317:4317 -p 4318:4318 jaegertracing/jaeger:latest
docker run --rm --name prometheus -p 9090:9090 -v "D:/------/app-me/docker composer/prometheus.yml:/etc/prometheus/prometheus.yml" prom/prometheus:latest