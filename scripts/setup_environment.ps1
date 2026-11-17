# StreamForge Local Setup Script (PowerShell)
Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host "  StreamForge - High-Throughput Kafka Data Lake Pipeline" -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan

# Check Docker availability
if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    Write-Error "Docker is required but not found in PATH."
    exit 1
}

Write-Host "[1/4] Starting StreamForge multi-container cluster via Docker Compose..." -ForegroundColor Green
docker compose up -d

Write-Host "[2/4] Waiting for Kafka, MinIO, and PostgreSQL health checks..." -ForegroundColor Green
Start-Sleep -Seconds 12

Write-Host "[3/4] Registering Avro Schemas with Confluent Schema Registry..." -ForegroundColor Green
python scripts/register_schemas.py

Write-Host "[4/4] Verifying pipeline health..." -ForegroundColor Green
python scripts/verify_data_lake.py

Write-Host ""
Write-Host "StreamForge is running successfully!" -ForegroundColor Green
Write-Host "  - Go Producer API:          http://localhost:8080/api/v1/events"
Write-Host "  - Java Consumer Actuator:   http://localhost:8081/actuator/health"
Write-Host "  - Parquet Lake Sink:        http://localhost:8082/metrics"
Write-Host "  - MinIO S3 Console:         http://localhost:9001 (minioadmin / minioadmin)"
Write-Host "  - Prometheus Dashboard:     http://localhost:9090"
Write-Host "  - Grafana Dashboards:       http://localhost:3000 (admin / admin)"
Write-Host "  - Jaeger Distributed Trace: http://localhost:16686"
