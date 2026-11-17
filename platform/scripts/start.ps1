Write-Host "Starting RailPulse DE local stack..." -ForegroundColor Cyan

docker compose pull

docker compose up -d zookeeper kafka schema-registry minio postgres prometheus grafana

Write-Host "Waiting for Kafka to be healthy..." -ForegroundColor Yellow
Start-Sleep -Seconds 20

docker compose up -d kafka-ui minio-init

Write-Host ""
Write-Host "RailPulse DE Stack is UP" -ForegroundColor Green
Write-Host "─────────────────────────────────────────"
Write-Host "  Kafka UI       → http://localhost:8080"
Write-Host "  Schema Registry→ http://localhost:8081"
Write-Host "  MinIO Console  → http://localhost:9001  (minioadmin / minioadmin)"
Write-Host "  PostgreSQL     → localhost:5432         (railpulse / railpulse_secret)"
Write-Host "  Prometheus     → http://localhost:9090"
Write-Host "  Grafana        → http://localhost:3000  (admin / railpulse)"
Write-Host "─────────────────────────────────────────"