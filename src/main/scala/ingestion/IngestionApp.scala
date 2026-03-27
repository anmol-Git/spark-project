package ingestion

import ingestion.config.PipelineConfig
import ingestion.reader.KafkaStreamReader
import ingestion.transform.Transformation
import ingestion.writer.DeltaWriter
import io.delta.tables.DeltaTable
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.slf4j.LoggerFactory

/**
 * Entry point for the data ingestion pipeline.
 *
 * Orchestrates: Kafka read -> Avro deserialization -> transformations -> Delta write.
 *
 * Design Decision DD-1: Uses Structured Streaming with Trigger.AvailableNow()
 * instead of pure batch with external offset management. This gives us:
 * - Spark-managed offset checkpointing (no custom OffsetManager)
 * - Exactly-once semantics (checkpoint committed only after foreachBatch succeeds)
 * - Batch-like behavior (processes all available data, then exits)
 * - Bounded micro-batches via maxOffsetsPerTrigger (prevents OOM)
 *
 * The job is scheduled externally (cron/Airflow). Each run processes all data
 * accumulated since the last checkpoint and exits.
 *
 * Usage: IngestionApp <path-to-pipeline-config>
 *   Example: IngestionApp src/main/resources/pipelines/orders.conf
 */
object IngestionApp {

  private val log = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    require(args.length == 1, "Usage: IngestionApp <pipeline-config-path>")
    val config = PipelineConfig.load(args(0))

    log.info(s"Starting ingestion pipeline: ${config.name}")
    log.info(s"Topic: ${config.kafka.topic}, Mode: ${config.processing.mode}")

    val spark = SparkSession.builder()
      .appName(s"ingestion-${config.name}")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
      .getOrCreate()

    try {
      run(spark, config)
    } finally {
      spark.stop()
    }
  }

  def run(spark: SparkSession, config: PipelineConfig): Unit = {
    // Load pluggable transformations (DD-5)
    val transforms: Seq[Transformation] = config.transformations.map { cls =>
      Class.forName(cls).getDeclaredConstructor().newInstance()
        .asInstanceOf[Transformation]
    }
    log.info(s"Loaded ${transforms.size} transformation(s)")

    // Read Kafka as a stream (DD-1)
    val kafkaStream = KafkaStreamReader.readStream(
      spark, config.kafka, config.processing.maxOffsetsPerTrigger)

    // Trigger.AvailableNow() processes all available data in bounded
    // micro-batches, then exits. foreachBatch gives us a regular DataFrame
    // for full control over write logic (MERGE, append, etc.)
    val query = kafkaStream.writeStream
      .trigger(Trigger.AvailableNow())
      .option("checkpointLocation", config.delta.checkpointPath)
      .foreachBatch { (batchDf: org.apache.spark.sql.DataFrame, batchId: Long) =>
        if (!batchDf.isEmpty) {
          log.info(s"Processing micro-batch $batchId")

          // Apply transformations
          var df = batchDf
          transforms.foreach { t => df = t.apply(df, config.transformConfig) }

          // Write to Delta (append or upsert with partition pruning)
          DeltaWriter.write(spark, df, config)

          log.info(s"Micro-batch $batchId written to ${config.delta.targetPath}")
        }
      }
      .start()

    query.awaitTermination()
    log.info("All micro-batches processed")

    // Post-processing: maintenance
    if (DeltaTable.isDeltaTable(spark, config.delta.targetPath)) {
      val deltaTable = DeltaTable.forPath(spark, config.delta.targetPath)

      if (config.optimizeAfterWrite) {
        log.info("Running OPTIMIZE for file compaction")
        deltaTable.optimize().executeCompaction()
      }

      if (config.vacuumEnabled) {
        log.info(s"Running VACUUM with ${config.vacuumRetentionHours}h retention")
        deltaTable.vacuum(config.vacuumRetentionHours.toDouble)
      }
    }

    log.info(s"Pipeline ${config.name} completed")
  }
}
