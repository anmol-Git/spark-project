# Low-Level Design: Data Ingestion Framework

## 1. Objective

Design and implement a scalable, fault-tolerant, config-driven ingestion pipeline that consumes CDC events from Kafka and persists them into Open Delta Lake. The system supports both append-only and upsert (MERGE) modes, handles schema evolution end-to-end (Schema Registry → ABRiS → Delta mergeSchema), deduplicates records, and provides exactly-once processing guarantees.

**Scale:** ~3,000 msgs/sec peak, ~5-6 TB/day, scheduled batch execution.

---

## 2. Design Decisions

### DD-1: Structured Streaming + Trigger.AvailableNow() over Pure Batch with External Checkpointing

| | Chosen | Rejected |
|---|---|---|
| **Approach** | Spark Structured Streaming with `Trigger.AvailableNow()` + `foreachBatch` | Pure batch (`spark.read.format("kafka")`) with external offset management |

**Why:**

In pure batch mode (`spark.read`), Spark does **not** manage Kafka offsets. The application must manually:
1. Track `startingOffsets` / `endingOffsets` in an external store (JSON file, Delta table, etc.)
2. Read offsets before each run, compute the batch range, and commit offsets after a successful write
3. Handle the **atomicity gap**: if the Delta write succeeds but the offset commit fails, the next run reprocesses the same batch — requiring custom idempotency logic for both append and upsert modes

Structured Streaming with `Trigger.AvailableNow()` (available since Spark 3.4) eliminates all of this:
- It processes **all available Kafka data** in bounded micro-batches, then **exits** — operationally identical to a batch job
- Spark manages offsets via its **checkpoint directory** automatically
- Checkpoints are committed **only after** each `foreachBatch` callback succeeds — the atomicity problem disappears entirely
- `maxOffsetsPerTrigger` bounds each micro-batch size (same effect as "maxOffsetsPerBatch" from the HLD), preventing OOM on large backlogs
- `foreachBatch` provides a regular `DataFrame` per micro-batch, giving full control over write logic (MERGE, append, custom transforms)

**Trade-off:** We rely on Spark's internal checkpoint format (not human-readable). Acceptable because we gain exactly-once semantics with zero custom offset code.

**How it maps to the HLD concepts:**

| HLD Concept | Implementation |
|---|---|
| "Offsets externally tracked" | Spark checkpoint directory (auto-managed) |
| "maxOffsetsPerBatch" | `maxOffsetsPerTrigger` option on readStream |
| "Commit offsets after successful write" | Spark commits checkpoint after `foreachBatch` returns |
| "On failure, offsets not committed, same batch reprocessed" | Spark replays from last committed checkpoint |

---

### DD-2: ABRiS for Avro Deserialization over Manual Schema Registry Client

| | Chosen | Rejected |
|---|---|---|
| **Approach** | ABRiS library (`za.co.absa:abris`) | Manual `kafka-schema-registry-client` + custom deserialization UDF |

**Why:**
- ABRiS provides `from_avro()` that integrates directly with Confluent Schema Registry — reads the schema version from the magic byte in each Kafka message and downloads the correct Avro schema automatically
- Eliminates manual schema fetch, version tracking, and custom UDF code
- Handles schema evolution transparently — when the registry has a new version, ABRiS picks it up on the next read
- Well-maintained open-source library, standard in production Spark + Kafka + Avro pipelines

---

### DD-3: HOCON (Typesafe Config) for Configuration over YAML/Properties

| | Chosen | Rejected |
|---|---|---|
| **Approach** | HOCON with Typesafe Config | YAML, Java Properties, custom JSON |

**Why:**
- HOCON supports hierarchical config with inheritance (`withFallback`) — pipeline configs override global defaults without duplication
- Native Scala/JVM library with zero extra dependencies
- Supports comments, variable substitution, and includes
- Standard in the Spark/Scala ecosystem

---

### DD-4: Partition Pruning via Merge Condition over Full Table Scan

| | Chosen | Rejected |
|---|---|---|
| **Approach** | Include partition columns (`dt`) in the MERGE condition alongside primary keys | MERGE only on primary keys |

**Why:**

Parquet files are **immutable** — they cannot be updated in-place. Delta's MERGE must:
1. Read all files that could contain matching rows
2. Apply changes (inserts, updates)
3. Write entirely **new** Parquet files with the merged data

Without partition pruning, MERGE scans **all files** in the table. By including `dt` in the merge condition:
```
target.order_id = source.order_id AND target.dt = source.dt
```
Delta prunes to only the partitions present in the incoming batch. If today's batch contains data for `dt=2026-03-27`, only files in that partition are read and rewritten — not the entire table history.

This is the **primary optimization** for upsert performance at scale and directly addresses the Parquet immutability concern raised in the design review.

---

### DD-5: Pluggable Transformations via Trait + Reflection over Hardcoded Logic

| | Chosen | Rejected |
|---|---|---|
| **Approach** | `Transformation` trait with config-driven class loading | Hardcoded transforms, or heavyweight plugin framework (DI, service loaders) |

**Why:**
- Different pipelines need different transformations. A trait with `apply(df, config) -> df` keeps it simple and extensible.
- New transformations: implement the trait, add class name to pipeline config. No framework changes.
- `Class.forName` is sufficient for this scale — no annotation processing, no DI framework, no runtime discovery needed.

---

### DD-6: JSON Handling — Configurable String vs Struct per Column

| | Chosen | Rejected |
|---|---|---|
| **Approach** | Per-column config (`json-columns.metadata = "string"`) | Always explode JSON, or always keep as string |

**Why:**

JSON columns in RDS are stored as strings. For the database, internal schema changes are invisible. This creates two scenarios:

- **String mode** (default): Keep JSON as-is in Delta. Simple, resilient to schema drift. Downstream queries use `get_json_object()`. Trade-off: less efficient querying, no type safety at storage layer.
- **Struct mode**: Parse JSON into StructType for stable, well-known schemas. Better query performance and type safety. Trade-off: breaks if JSON structure changes unexpectedly.

**Exploding JSON into separate columns** was explicitly rejected in the design review — it creates an operational maintenance loop for deeply nested, frequently changing JSON structures.

Making this configurable per column per pipeline lets each team choose the right approach for their data.

---

## 3. Architecture

### 3.1 System Flow

```
┌─────────┐    ┌──────────┐    ┌───────┐    ┌─────────────────┐    ┌─────────────┐
│  MySQL   │───>│ Debezium │───>│ Kafka │───>│   Spark Job     │───>│ Delta Lake  │
│  (RDS)   │    │ Connect  │    │       │    │ (IngestionApp)  │    │             │
└─────────┘    └──────────┘    └───┬───┘    └────────┬────────┘    └─────────────┘
                                   │                  │
                            ┌──────┴──────┐    ┌──────┴──────┐
                            │   Schema    │    │ Checkpoint  │
                            │  Registry   │    │  Directory  │
                            └─────────────┘    └─────────────┘
```

### 3.2 Spark Job Internal Flow

```
readStream(Kafka)              ← Structured Streaming source
    │
    ▼
Trigger.AvailableNow()        ← Process all available data, then exit
    │
    ▼
foreachBatch { batchDf =>      ← One DataFrame per bounded micro-batch
    │
    ├── ABRiS from_avro()      ← Avro deserialization via Schema Registry (DD-2)
    │                            Binary payloads (bytes) pass through as BinaryType
    │
    ├── Transformations        ← Pluggable, config-driven (DD-5)
    │
    ├── Deduplication          ← By primary keys, latest offset wins (append mode)
    │
    └── DeltaWriter            ← Append (mergeSchema) or MERGE with partition pruning (DD-4)
}
    │
    ▼
Spark Checkpoint               ← Offset committed after foreachBatch succeeds (DD-1)
    │
    ▼
OPTIMIZE + VACUUM              ← File compaction + orphan file cleanup
```

---

## 4. Project Structure

```
spark-project/
├── build.sbt                                # Dependencies + assembly config
├── project/
│   ├── build.properties                     # sbt.version=1.9.7
│   └── plugins.sbt                          # sbt-assembly
├── src/
│   ├── main/
│   │   ├── scala/ingestion/
│   │   │   ├── IngestionApp.scala           # Entry point + orchestrator
│   │   │   ├── config/
│   │   │   │   └── PipelineConfig.scala     # Config case classes + HOCON loader
│   │   │   ├── reader/
│   │   │   │   └── KafkaStreamReader.scala  # readStream + ABRiS Avro deser
│   │   │   ├── writer/
│   │   │   │   └── DeltaWriter.scala        # Append (dedup) + Upsert (MERGE)
│   │   │   └── transform/
│   │   │       ├── Transformation.scala     # Trait
│   │   │       └── AddPartitionColumn.scala # dt column derivation
│   │   └── resources/
│   │       ├── application.conf             # Global defaults
│   │       └── pipelines/
│   │           ├── orders.conf              # Production pipeline config
│   │           └── orders-e2e.conf          # E2E test config
│   └── test/
│       └── scala/ingestion/
│           ├── SparkTestBase.scala          # Shared SparkSession for tests
│           ├── E2ERunner.scala              # End-to-end test runner
│           ├── config/PipelineConfigTest.scala
│           ├── transform/AddPartitionColumnTest.scala
│           └── writer/DeltaWriterTest.scala
├── docker/
│   └── docker-compose.yml                   # Kafka, ZK, Schema Registry (+ CDC profile)
├── scripts/
│   └── run-e2e.sh                           # One-command E2E script
└── conf/
    └── pipelines/                           # Production configs (mounted at runtime)
```

---

## 5. Dependencies

| Dependency | Version | Purpose |
|---|---|---|
| Apache Spark (Core + SQL) | 3.5.1 | Processing engine (provided at runtime) |
| spark-sql-kafka-0-10 | 3.5.1 | Kafka source connector for Structured Streaming |
| spark-avro | 3.5.1 | Avro deserialization support (required by ABRiS) |
| delta-spark | 3.1.0 | Open Delta Lake (ACID writes, MERGE, OPTIMIZE, VACUUM) |
| ABRiS | 6.4.0 | Avro deserialization with Schema Registry integration |
| Typesafe Config | 1.4.3 | HOCON configuration loading |
| ScalaTest | 3.2.17 | Unit and integration tests |

Scala version: 2.12.18 (matches Spark 3.5.x runtime).

---

## 6. Configuration Schema

### 6.1 Global Config (`application.conf`)

```hocon
ingestion {
  spark {
    app-name = "data-ingestion"
    master = "local[*]"                  # overridden via spark-submit in production
  }
  kafka {
    bootstrap-servers = "localhost:9092"
    schema-registry-url = "http://localhost:8081"
  }
  defaults {
    max-offsets-per-trigger = 100000     # micro-batch size bound
    processing-mode = "append"           # append | upsert
    optimize-after-write = true
    vacuum-enabled = false               # enable for periodic orphan file cleanup
    vacuum-retention-hours = 168         # 7 days
  }
}
```

### 6.2 Pipeline Config (`pipelines/orders.conf`)

```hocon
pipeline {
  name = "orders"

  kafka {
    topic = "dbserver1.inventory.orders"
    # bootstrap-servers and schema-registry-url fall back to global config
  }

  processing {
    mode = "upsert"                      # append | upsert
    primary-keys = ["order_id"]          # used for upsert MERGE + append deduplication
    max-offsets-per-trigger = 50000      # override global default
  }

  delta {
    target-path = "/data/delta/orders"
    partition-columns = ["dt"]
    checkpoint-path = "/data/checkpoints/orders"
  }

  json-columns {
    metadata = "string"                  # keep as raw JSON string (DD-6)
  }

  transformations = [
    "ingestion.transform.AddPartitionColumn"
  ]

  transform-config {
    source-timestamp-column = "updated_at"
    partition-column-name = "dt"
    partition-format = "yyyy-MM-dd"
  }
}
```

Pipeline configs override global defaults via HOCON's `withFallback` mechanism (DD-3). The same compiled JAR runs for any pipeline — only the config file changes.

---

## 7. Component Details

### 7.1 PipelineConfig (`config/PipelineConfig.scala`)

**Responsibility:** Load and merge HOCON config into typed case classes.

```scala
case class KafkaConfig(bootstrapServers: String, schemaRegistryUrl: String, topic: String)
case class ProcessingConfig(mode: String, primaryKeys: Seq[String], maxOffsetsPerTrigger: Long)
case class DeltaConfig(targetPath: String, partitionColumns: Seq[String], checkpointPath: String)
case class PipelineConfig(
  name: String, kafka: KafkaConfig, processing: ProcessingConfig,
  delta: DeltaConfig, jsonColumns: Map[String, String],
  transformations: Seq[String], transformConfig: Map[String, String],
  optimizeAfterWrite: Boolean, vacuumEnabled: Boolean, vacuumRetentionHours: Int
)
```

`PipelineConfig.load(path)` loads pipeline-specific config, falls back to `application.conf` for global defaults, and resolves into a `PipelineConfig` instance.

### 7.2 KafkaStreamReader (`reader/KafkaStreamReader.scala`)

**Responsibility:** Create a streaming DataFrame from Kafka with Avro deserialization.

```scala
object KafkaStreamReader {
  def readStream(spark: SparkSession, kafkaConfig: KafkaConfig,
                 maxOffsetsPerTrigger: Long): DataFrame
}
```

- Uses `spark.readStream.format("kafka")` with `maxOffsetsPerTrigger` (DD-1)
- `startingOffsets = "earliest"` applies only on first run (no checkpoint)
- ABRiS `from_avro()` deserializes Avro values using Schema Registry (DD-2)
- `downloadReaderSchemaByLatestVersion` — always fetches the latest Avro schema, so new columns from schema evolution are automatically included in the DataFrame
- **Binary payloads:** Avro `bytes` fields are deserialized to Spark `BinaryType` automatically by ABRiS and written to Delta as `BinaryType`. No special handling required — binary data (encoded payloads, emojis) passes through transparently.
- Preserves Kafka metadata columns (`_kafka_partition`, `_kafka_offset`, `_kafka_timestamp`) for deduplication; dropped before Delta write

### 7.3 Transformation (`transform/Transformation.scala`)

**Responsibility:** Pluggable, config-driven DataFrame transformations (DD-5).

```scala
trait Transformation {
  def apply(df: DataFrame, config: Map[String, String]): DataFrame
}
```

**Built-in: AddPartitionColumn** — derives a `dt` partition column from a timestamp source column using a configurable date format. This column is used for Delta table partitioning and MERGE partition pruning (DD-4).

### 7.4 DeltaWriter (`writer/DeltaWriter.scala`)

**Responsibility:** Write DataFrames to Delta Lake in append or upsert mode.

```scala
object DeltaWriter {
  def write(spark: SparkSession, df: DataFrame, config: PipelineConfig): Unit
}
```

**Append mode:**
1. **Deduplication** by primary/composite keys — within each micro-batch, keeps only the record with the highest Kafka offset per key (latest event wins). If no primary keys are configured, no dedup is applied.
2. Drops Kafka metadata columns (`_kafka_partition`, `_kafka_offset`, `_kafka_timestamp`)
3. Writes with `mergeSchema = true` — new columns from schema evolution are automatically added to the Delta table

**Upsert mode:**
1. First write (table doesn't exist): direct save
2. Subsequent writes: Delta `MERGE INTO` with condition built from `primary-keys + partition-columns` (DD-4)
3. `whenMatched.updateAll()` + `whenNotMatched.insertAll()`
4. Schema evolution via `spark.databricks.delta.schema.autoMerge.enabled = true` — MERGE automatically adds new columns from the source DataFrame

**Merge condition example:**
```sql
target.`order_id` = source.`order_id` AND target.`dt` = source.`dt`
```

### 7.5 IngestionApp (`IngestionApp.scala`)

**Responsibility:** Orchestrate the full pipeline.

**Execution sequence:**
1. Load pipeline config from HOCON file argument
2. Create SparkSession with Delta Lake extensions + `autoMerge.enabled` for schema evolution
3. Load transformations by class name via reflection (DD-5)
4. `readStream` from Kafka with `maxOffsetsPerTrigger` (DD-1)
5. `writeStream` with `Trigger.AvailableNow()` + `foreachBatch` (DD-1)
6. Each micro-batch: apply transforms → dedup (append) → DeltaWriter (append or MERGE)
7. Spark auto-commits checkpoint after each successful micro-batch
8. After all micro-batches: run OPTIMIZE for file compaction
9. If `vacuum-enabled`: run VACUUM to remove orphan files beyond retention period
10. Stop SparkSession and exit

---

## 8. Schema Evolution

Schema evolution is handled end-to-end across three layers with zero manual intervention:

```
Source RDS adds column (DDL)
    ↓
Debezium captures DDL → registers new Avro schema version in Schema Registry
    ↓
ABRiS (KafkaStreamReader) → downloads latest schema → new column appears in DataFrame
    ↓
Delta Writer → mergeSchema (append) / autoMerge (upsert)
    ↓
Delta table schema evolves automatically — new column added, existing rows get NULL
```

| Layer | Mechanism | Code Location |
|---|---|---|
| **Schema Registry** | Debezium auto-registers new schema versions on DDL changes. Backward/forward compatibility enforced at producer level. | External (Debezium + Registry) |
| **ABRiS deserialization** | `downloadReaderSchemaByLatestVersion` always fetches the latest Avro schema — new columns are automatically deserialized into the DataFrame. | `KafkaStreamReader.scala:36` |
| **Delta write (append)** | `mergeSchema = true` on the write options — new columns in the DataFrame are added to the Delta table schema automatically. | `DeltaWriter.scala:36` |
| **Delta write (upsert)** | `spark.databricks.delta.schema.autoMerge.enabled = true` on the SparkSession — MERGE operations automatically evolve the target table schema when source has new columns. | `IngestionApp.scala:46` |

**What types of schema changes are supported:**

| Change Type | Supported | Notes |
|---|---|---|
| Column addition | Yes | Automatic via mergeSchema/autoMerge |
| Column type widening (e.g., int → long) | Yes | Handled by Delta type evolution |
| Column rename | No | Requires manual migration |
| Column deletion | No | Old columns remain with NULL for new rows |
| Incompatible type change (e.g., string → int) | No | Schema Registry compatibility rules prevent this at producer level |

---

## 9. Data Handling

### 9.1 Deduplication

| Mode | Strategy | Implementation |
|---|---|---|
| **Upsert** | MERGE is naturally idempotent — duplicate records with the same primary key produce the same result | `DeltaWriter.upsertWrite()` |
| **Append** | Within each micro-batch, window by primary keys and keep only the record with the highest `_kafka_offset` (latest event wins). If no primary keys are configured, no dedup is applied. | `DeltaWriter.deduplicateByKeys()` |

### 9.2 Binary Payload Handling

The HLD requires support for binary payloads (encoded data, emojis). This is handled transparently:

- Avro `bytes` fields → ABRiS deserializes to Spark `BinaryType`
- Spark `BinaryType` → Delta writes as `BinaryType` (Parquet binary)
- No transformation, casting, or special config needed
- Binary data passes through the entire pipeline untouched

### 9.3 JSON Column Handling (DD-6)

Configured per column per pipeline via `json-columns` in HOCON:

| Mode | Config | Behavior |
|---|---|---|
| **String** | `metadata = "string"` | JSON kept as-is in Delta. Query via `get_json_object()`. Resilient to schema drift. |
| **Struct** | `metadata = "struct"` | JSON parsed into Spark StructType. Better query performance. Breaks on unexpected structure changes. |

---

## 10. Processing Guarantees

| Guarantee | How It Is Achieved |
|---|---|
| **Exactly-once ingestion** | Spark checkpoint commits only after `foreachBatch` succeeds (DD-1) |
| **No data loss on failure** | On failure, checkpoint not committed — Spark replays from last committed offset |
| **Idempotent upserts** | MERGE with same data produces identical result — safe on replay |
| **Idempotent appends** | Spark checkpoint ensures each micro-batch processed exactly once |
| **No duplicates within batch** | Append-mode deduplication by primary keys (latest offset wins) |
| **Bounded memory usage** | `maxOffsetsPerTrigger` limits micro-batch size |
| **Partial progress preserved** | Each micro-batch checkpointed independently — earlier batches survive later failures |

---

## 11. Upsert (MERGE) Strategy

### Problem
Upserts require read → merge → rewrite. Parquet files are immutable — the entire file must be rewritten on any update. This makes MERGE an expensive operation.

### Approach
- **Delta MERGE INTO** with primary/composite keys for correctness
- **Partition pruning** via `dt` in merge condition (DD-4) to minimize file rewrites
- Only files in partitions matching the incoming batch are read and rewritten

### First Write Handling
On first run (table doesn't exist), data is written directly without MERGE. Subsequent runs use the full MERGE path.

### CDC Operation Mapping
| CDC Operation | MERGE Behavior |
|---|---|
| Insert | `whenNotMatched.insertAll()` |
| Update | `whenMatched.updateAll()` |
| Delete | Not handled in v1 (as per design review scope) |

---

## 12. Delta Table Maintenance

| Operation | Config Flag | Frequency | Purpose |
|---|---|---|---|
| **OPTIMIZE** | `optimize-after-write = true` | After each pipeline run | Compacts small files from micro-batch writes into larger files for better read performance |
| **VACUUM** | `vacuum-enabled = true` | After each pipeline run (when enabled) | Removes orphan Parquet files not referenced in the Delta transaction log. Retention controlled by `vacuum-retention-hours` (default: 168h / 7 days) |

VACUUM is disabled by default (`vacuum-enabled = false`). Enable it for pipelines where storage efficiency matters, or run it as a separate periodic job.

---

## 13. Multi-Tenant Job Strategy

The same compiled JAR serves all pipelines. Behavior is driven entirely by the pipeline config file.

| Topic Category | Strategy | Rationale |
|---|---|---|
| High-throughput (e.g., orders, clickstream) | Dedicated jobs per topic | Isolation prevents one slow table from blocking others |
| Low-throughput (e.g., configs, lookups) | Grouped into a single job | Reduces scheduling overhead; sequential processing is fast enough |

Job scheduling is external (cron/Airflow). Each invocation passes a different config file:
```bash
spark-submit --class ingestion.IngestionApp ingestion.jar conf/pipelines/orders.conf
spark-submit --class ingestion.IngestionApp ingestion.jar conf/pipelines/clickstream.conf
```

---

## 14. Local Development Setup

### Docker Compose Services

| Service | Image | Port | Purpose |
|---|---|---|---|
| Zookeeper | confluentinc/cp-zookeeper:7.6.0 | 2181 | Kafka dependency |
| Kafka | confluentinc/cp-kafka:7.6.0 | 9092 | Message broker (dual listener: internal 29092 + external 9092) |
| Schema Registry | confluentinc/cp-schema-registry:7.6.0 | 8081 | Avro schema management |
| MySQL | mysql:8.0 (CDC profile) | 3306 | Source RDS simulator |
| Debezium Connect | debezium/connect:2.5 (CDC profile) | 8083 | CDC connector |

### E2E Test Flow
```
E2ERunner produces Avro messages → Kafka topic (Schema Registry)
    → Spark pipeline (ABRiS deser → transforms → Delta MERGE)
    → Delta files on local filesystem
    → Read and verify: 10 rows, MERGE updates 3, count unchanged
```

---

## 15. Testing Strategy

| Level | Scope | Tests | Tools |
|---|---|---|---|
| Unit | Config loading, transformation logic | 4 + 3 | ScalaTest |
| Integration | DeltaWriter (append + upsert + dedup + idempotency) | 6 | ScalaTest + local SparkSession |
| E2E | Full pipeline: Kafka Avro → Spark → Delta → verify MERGE | 7 steps | Docker Compose + E2ERunner |

---

## 16. Verification Checklist

- [ ] `sbt compile` succeeds with all dependencies resolved
- [ ] `sbt test` passes (13 tests: config, transforms, writer)
- [ ] `docker-compose up -d` starts Kafka, ZK, Schema Registry
- [ ] `sbt "Test / runMain ingestion.E2ERunner"` passes E2E
- [ ] Initial load: 10 rows written to Delta
- [ ] MERGE: 3 rows updated in place, total count unchanged
- [ ] Deduplication: duplicate records in append mode are removed by primary key
- [ ] Schema evolution: new columns added automatically without code changes
- [ ] OPTIMIZE runs and compacts small files
- [ ] VACUUM removes orphan files when enabled
- [ ] Binary payloads pass through as BinaryType
