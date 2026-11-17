#!/usr/bin/env bash
set -e

echo "=========================================================="
echo "  StreamForge - High-Throughput Kafka Data Lake Pipeline"
echo "=========================================================="

echo "[1/4] Starting StreamForge multi-container cluster..."
docker compose up -d

echo "[2/4] Waiting for Kafka, MinIO, and PostgreSQL health checks..."
sleep 12

echo "[3/4] Registering Avro Schemas with Schema Registry..."
python3 scripts/register_schemas.py

echo "[4/4] Verifying pipeline health..."
python3 scripts/verify_data_lake.py

echo ""
echo "StreamForge is running successfully!"
echo "  - Go Producer API:          http://localhost:8080/api/v1/events"
echo "  - Java Consumer Actuator:   http://localhost:8081/actuator/health"
echo "  - Parquet Lake Sink:        http://localhost:8082/metrics"
echo "  - MinIO S3 Console:         http://localhost:9001 (minioadmin / minioadmin)"
echo "  - Prometheus Dashboard:     http://localhost:9090"
echo "  - Grafana Dashboards:       http://localhost:3000 (admin / admin)"
echo "  - Jaeger Distributed Trace: http://localhost:16686"
