#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

echo "============================================"
echo "  Data Ingestion Framework — E2E Test"
echo "============================================"
echo ""

# 1. Clean previous E2E data
echo "[1/5] Cleaning previous E2E data..."
rm -rf /tmp/e2e-delta /tmp/e2e-checkpoints

# 2. Start Docker services (Kafka + ZK + Schema Registry only)
echo "[2/5] Starting Docker services (Kafka, ZK, Schema Registry)..."
cd "$PROJECT_DIR/docker"
docker-compose up -d zookeeper kafka schema-registry

# 3. Wait for Schema Registry to be healthy
echo "[3/5] Waiting for services to be ready..."
echo -n "  Waiting for Schema Registry"
for i in $(seq 1 30); do
    if curl -sf http://localhost:8081/subjects > /dev/null 2>&1; then
        echo " READY"
        break
    fi
    echo -n "."
    sleep 2
done

# Verify Schema Registry is actually up
if ! curl -sf http://localhost:8081/subjects > /dev/null 2>&1; then
    echo " FAILED"
    echo "ERROR: Schema Registry did not start within 60 seconds."
    echo "Check: docker-compose logs schema-registry"
    exit 1
fi

# 4. Run E2E test
echo "[4/5] Running E2E test via sbt..."
echo ""
cd "$PROJECT_DIR"
sbt "Test / runMain ingestion.E2ERunner"

# 5. Done
echo ""
echo "[5/5] E2E test complete."
echo ""
echo "To stop Docker services:"
echo "  cd docker && docker-compose down"
echo ""
echo "To re-run (services already up):"
echo "  sbt \"Test / runMain ingestion.E2ERunner\""
