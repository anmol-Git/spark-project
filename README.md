# Data Ingestion Framework

Kafka → Spark (Scala) → Open Delta Lake batch ingestion pipeline.

Consumes CDC events from Kafka, deserializes Avro via Schema Registry, applies pluggable transformations, deduplicates by primary keys, and writes to Delta Lake in append or upsert (MERGE) mode. Uses Structured Streaming with `Trigger.AvailableNow()` for built-in offset management and exactly-once semantics. Supports end-to-end schema evolution, binary payloads, and configurable JSON handling.

## Prerequisites

| Requirement | Version | Notes |
|-------------|---------|-------|
| Java (JDK) | 8 or 11 | `java -version` to verify |
| SBT | 1.9+ | Auto-downloaded if not present |
| Docker | 20+ | Required for Kafka, Schema Registry |
| Docker Compose | v2+ | Bundled with Docker Desktop / limactl |

If using **limactl**, ensure the Docker socket is available:
```bash
limactl start
export DOCKER_HOST=unix://$HOME/.lima/default/sock/docker.sock
```

## Quick Start

### 1. Compile and Test

```bash
sbt compile
sbt test        # 13 tests — config, transforms, Delta writer
```

### 2. Start Infrastructure

```bash
cd docker
docker-compose up -d zookeeper kafka schema-registry
```

Wait for services to be healthy:
```bash
# Should return [] when ready
curl -s http://localhost:8081/subjects
```

### 3. Run End-to-End Test

**Important:** Run sbt from the project root, not from `docker/`.

```bash
cd ..    # back to project root if you're still in docker/
sbt "Test / runMain ingestion.E2ERunner"
```

Or use the script (starts Docker services automatically):
```bash
./scripts/run-e2e.sh
```

### 4. Stop Infrastructure

```bash
cd docker
docker-compose down
```

## E2E Test Output

The E2E runner demonstrates the full pipeline in 7 steps:

```
============================================================
  STEP 1: Producing 10 orders to Kafka (Avro + Schema Registry)
============================================================
  Produced 10 records to topic 'orders-e2e'

============================================================
  STEP 2: Running ingestion pipeline (upsert mode)
============================================================
  Pipeline completed.

============================================================
  STEP 3: Reading Delta table — initial load
============================================================
+--------+------+--------------------------------+--------------------+----------+
|order_id|status|metadata                        |updated_at          |dt        |
+--------+------+--------------------------------+--------------------+----------+
|1       |placed|{"source":"web","item_count":1}  |2026-03-27 10:01:00|2026-03-27|
|2       |placed|{"source":"web","item_count":2}  |2026-03-27 10:02:00|2026-03-27|
|3       |placed|{"source":"web","item_count":3}  |2026-03-27 10:03:00|2026-03-27|
|...     |...   |...                              |...                |...       |
+--------+------+--------------------------------+--------------------+----------+
  Total rows: 10

============================================================
  STEP 4: Producing 3 updates (orders 1,2,3 → shipped)
============================================================
  Produced 3 update records to topic 'orders-e2e'

============================================================
  STEP 5: Re-running pipeline (MERGE will update orders 1,2,3)
============================================================
  Pipeline completed.

============================================================
  STEP 6: Verifying MERGE — orders 1,2,3 should be 'shipped'
============================================================
+--------+-------+-------------------------------------------------------+--------------------+----------+
|order_id|status |metadata                                               |updated_at          |dt        |
+--------+-------+-------------------------------------------------------+--------------------+----------+
|1       |shipped|{"source":"web","item_count":1,"shipped_by":"express"}  |2026-03-27 11:00:00|2026-03-27|
|2       |shipped|{"source":"web","item_count":2,"shipped_by":"standard"} |2026-03-27 11:05:00|2026-03-27|
|3       |shipped|{"source":"web","item_count":3,"shipped_by":"express"}  |2026-03-27 11:10:00|2026-03-27|
|4       |placed |{"source":"web","item_count":4}                         |2026-03-27 10:04:00|2026-03-27|
|...     |...    |...                                                     |...                |...       |
+--------+-------+-------------------------------------------------------+--------------------+----------+
  Total rows: 10 (unchanged — MERGE updated in place)
  Shipped: 3, Placed: 7

  E2E TEST PASSED

============================================================
  STEP 7: Delta table history (version log)
============================================================
+-------+-----------------------+---------+
|version|timestamp              |operation|
+-------+-----------------------+---------+
|1      |2026-03-27 09:21:00.348|MERGE    |  ← 3 rows updated, 6 copied
|0      |2026-03-27 09:20:53.958|WRITE    |  ← 10 rows initial load
+-------+-----------------------+---------+
```

## Inspecting the Pipeline (Hands-On)

After running the E2E test, use these commands to inspect each layer of the pipeline. This helps build a concrete understanding of how data flows through the system.

> All commands assume Docker services are running (`docker-compose up -d`).

### Kafka: View Messages in a Topic

List all topics:
```bash
docker exec docker-kafka-1 kafka-topics --bootstrap-server localhost:9092 --list
```

Expected output:
```
orders-e2e
```

View raw messages (key + value as bytes — Avro is not human-readable):
```bash
docker exec docker-kafka-1 kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic orders-e2e \
  --from-beginning \
  --max-messages 3 \
  --property print.key=true \
  --property print.offset=true
```

Check topic details (partitions, offsets, message count):
```bash
docker exec docker-kafka-1 kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list localhost:9092 \
  --topic orders-e2e
```

Expected output (partition:offset — offset = total messages produced):
```
orders-e2e:0:13
```

### Schema Registry: View Registered Schemas

List all registered subjects:
```bash
curl -s http://localhost:8081/subjects | python3 -m json.tool
```

Expected output:
```json
[
    "orders-e2e-value"
]
```

View the latest schema version for the topic:
```bash
curl -s http://localhost:8081/subjects/orders-e2e-value/versions/latest | python3 -m json.tool
```

Expected output:
```json
{
    "subject": "orders-e2e-value",
    "version": 1,
    "id": 1,
    "schema": "{\"type\":\"record\",\"name\":\"Order\",\"namespace\":\"inventory\",\"fields\":[{\"name\":\"order_id\",\"type\":\"int\"},{\"name\":\"status\",\"type\":\"string\"},{\"name\":\"metadata\",\"type\":\"string\"},{\"name\":\"updated_at\",\"type\":\"string\"}]}"
}
```

This shows the Avro schema that ABRiS downloads at runtime for deserialization. When the source adds a column, a new version appears here automatically.

Check compatibility level:
```bash
curl -s http://localhost:8081/config | python3 -m json.tool
```

### Kafka: Read Avro Messages (Human-Readable)

Use the Avro console consumer inside the Schema Registry container to decode Avro messages:
```bash
docker exec docker-schema-registry-1 kafka-avro-console-consumer \
  --bootstrap-server kafka:29092 \
  --topic orders-e2e \
  --from-beginning \
  --max-messages 3 \
  --property schema.registry.url=http://localhost:8081
```

Expected output (decoded Avro → JSON):
```json
{"order_id":1,"status":"placed","metadata":"{\"source\":\"web\",\"item_count\":1}","updated_at":"2026-03-27 10:01:00"}
{"order_id":2,"status":"placed","metadata":"{\"source\":\"web\",\"item_count\":2}","updated_at":"2026-03-27 10:02:00"}
{"order_id":3,"status":"placed","metadata":"{\"source\":\"web\",\"item_count\":3}","updated_at":"2026-03-27 10:03:00"}
```

### Spark Checkpoint: View Offset State

The checkpoint directory stores Spark's offset tracking (what has been processed):
```bash
ls /tmp/e2e-checkpoints/orders/offsets/
```

Expected output:
```
0    1
```

Each file represents a committed micro-batch. View the offsets for batch 0:
```bash
cat /tmp/e2e-checkpoints/orders/offsets/0
```

Expected output:
```json
v1
{"batchWatermarkMs":0,"batchTimestampMs":...}
{"orders-e2e":{"0":10}}
```

This shows Spark consumed up to offset 10 on partition 0 of `orders-e2e` — exactly the 10 records from step 1.

### Delta Lake: Inspect the Output Table

View the Delta transaction log (each JSON file = one commit):
```bash
ls /tmp/e2e-delta/orders/_delta_log/
```

Expected output:
```
00000000000000000000.json    ← Version 0: initial WRITE (10 rows)
00000000000000000001.json    ← Version 1: MERGE (3 updated)
```

View the commit details for the MERGE operation:
```bash
cat /tmp/e2e-delta/orders/_delta_log/00000000000000000001.json | python3 -m json.tool | head -20
```

View the actual Parquet data files:
```bash
ls /tmp/e2e-delta/orders/dt=2026-03-27/
```

Expected output (Parquet files — one per write/rewrite):
```
part-00000-...-c000.snappy.parquet
```

### Summary: Where to Look

| What | Where | Command |
|------|-------|---------|
| Kafka topics | Kafka broker | `kafka-topics --list` |
| Raw Kafka messages | Kafka broker | `kafka-console-consumer` |
| Decoded Avro messages | Schema Registry container | `kafka-avro-console-consumer` |
| Avro schema (structure) | Schema Registry API | `curl http://localhost:8081/subjects/.../versions/latest` |
| Schema versions | Schema Registry API | `curl http://localhost:8081/subjects/.../versions` |
| Spark checkpoint (offsets) | Local filesystem | `cat /tmp/e2e-checkpoints/orders/offsets/*` |
| Delta transaction log | Local filesystem | `cat /tmp/e2e-delta/orders/_delta_log/*.json` |
| Delta data files (Parquet) | Local filesystem | `ls /tmp/e2e-delta/orders/dt=*/` |

## Project Structure

```
├── build.sbt                          # Dependencies: Spark 3.5.1, Delta 3.1.0, ABRiS 6.4.0
├── LLD.md                             # Low-Level Design document
├── src/main/scala/ingestion/
│   ├── IngestionApp.scala             # Entry point — Structured Streaming orchestrator
│   ├── config/PipelineConfig.scala    # HOCON config model with global fallback
│   ├── reader/KafkaStreamReader.scala # readStream + ABRiS Avro deserialization
│   ├── writer/DeltaWriter.scala       # Append (dedup + mergeSchema) + Upsert (MERGE)
│   └── transform/
│       ├── Transformation.scala       # Pluggable trait
│       └── AddPartitionColumn.scala   # Derives dt partition column
├── src/main/resources/
│   ├── application.conf               # Global defaults
│   └── pipelines/
│       ├── orders.conf                # Production pipeline config
│       └── orders-e2e.conf            # E2E test config
├── src/test/scala/ingestion/
│   ├── SparkTestBase.scala            # Shared SparkSession for tests
│   ├── E2ERunner.scala                # End-to-end test runner
│   ├── config/PipelineConfigTest.scala
│   ├── transform/AddPartitionColumnTest.scala
│   └── writer/DeltaWriterTest.scala
├── docker/docker-compose.yml          # Kafka, ZK, Schema Registry (+ CDC profile)
└── scripts/run-e2e.sh                 # One-command E2E script
```

## Key Features

### Schema Evolution (end-to-end, zero manual intervention)
```
Source RDS adds column → Debezium registers new Avro schema in Schema Registry
    → ABRiS downloads latest schema → new column in DataFrame
    → Delta mergeSchema/autoMerge → table schema evolves automatically
```

### Deduplication
- **Upsert mode:** MERGE is naturally idempotent — duplicate keys produce the same result
- **Append mode:** Deduplicates each micro-batch by primary keys, keeping the record with the highest Kafka offset (latest event wins)

### Binary Payload Support
Avro `bytes` fields (encoded data, emojis) pass through the entire pipeline as Spark `BinaryType` → Delta `BinaryType`. No special config needed.

### Delta Table Maintenance
- **OPTIMIZE:** Compacts small files after each run (`optimize-after-write = true`, default)
- **VACUUM:** Removes orphan files beyond retention period (`vacuum-enabled = false`, enable per pipeline)

## Running a Pipeline

```bash
# Via spark-submit (production)
spark-submit \
  --class ingestion.IngestionApp \
  --master local[*] \
  target/scala-2.12/data-ingestion-framework-assembly-0.1.0.jar \
  conf/pipelines/orders.conf

# Via sbt (development — run from project root)
sbt "Test / runMain ingestion.IngestionApp src/main/resources/pipelines/orders.conf"
```

Each pipeline is configured via a HOCON file. The same JAR serves all pipelines — only the config changes.

## Configuration

Global defaults in `application.conf`, overridden per pipeline:

```hocon
pipeline {
  name = "orders"
  kafka { topic = "dbserver1.inventory.orders" }
  processing {
    mode = "upsert"                    # append | upsert
    primary-keys = ["order_id"]        # used for MERGE + append deduplication
    max-offsets-per-trigger = 50000    # micro-batch size bound
  }
  delta {
    target-path = "/data/delta/orders"
    partition-columns = ["dt"]
    checkpoint-path = "/data/checkpoints/orders"
  }
  json-columns {
    metadata = "string"                # string | struct per column
  }
  transformations = ["ingestion.transform.AddPartitionColumn"]
}
```

## Design

See [LLD.md](LLD.md) for the full Low-Level Design including:
- 6 documented design decisions (DD-1 through DD-6) with chosen/rejected/rationale
- Architecture diagrams and processing flow
- Schema evolution strategy (3-layer: Registry → ABRiS → Delta)
- Deduplication, binary handling, and JSON column strategies
- Processing guarantees and upsert (MERGE) optimization
- Delta maintenance (OPTIMIZE + VACUUM)
