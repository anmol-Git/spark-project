package ingestion.config

import com.typesafe.config.{Config, ConfigFactory}
import scala.collection.JavaConverters._

case class KafkaConfig(
  bootstrapServers: String,
  schemaRegistryUrl: String,
  topic: String
)

case class ProcessingConfig(
  mode: String,
  primaryKeys: Seq[String],
  maxOffsetsPerTrigger: Long
)

case class DeltaConfig(
  targetPath: String,
  partitionColumns: Seq[String],
  checkpointPath: String
)

case class PipelineConfig(
  name: String,
  kafka: KafkaConfig,
  processing: ProcessingConfig,
  delta: DeltaConfig,
  jsonColumns: Map[String, String],
  transformations: Seq[String],
  transformConfig: Map[String, String],
  optimizeAfterWrite: Boolean,
  vacuumEnabled: Boolean,
  vacuumRetentionHours: Int
)

object PipelineConfig {

  def load(pipelineConfPath: String): PipelineConfig = {
    val global = ConfigFactory.load()
    val pipeline = ConfigFactory.parseFile(new java.io.File(pipelineConfPath))
      .withFallback(global)
      .resolve()

    val p = pipeline.getConfig("pipeline")
    val g = pipeline.getConfig("ingestion")

    PipelineConfig(
      name = p.getString("name"),
      kafka = KafkaConfig(
        bootstrapServers = getOrFallback(p, "kafka.bootstrap-servers",
          g, "kafka.bootstrap-servers"),
        schemaRegistryUrl = getOrFallback(p, "kafka.schema-registry-url",
          g, "kafka.schema-registry-url"),
        topic = p.getString("kafka.topic")
      ),
      processing = ProcessingConfig(
        mode = getOrFallback(p, "processing.mode", g, "defaults.processing-mode"),
        primaryKeys = if (p.hasPath("processing.primary-keys"))
          p.getStringList("processing.primary-keys").asScala.toSeq
        else Seq.empty,
        maxOffsetsPerTrigger = if (p.hasPath("processing.max-offsets-per-trigger"))
          p.getLong("processing.max-offsets-per-trigger")
        else g.getLong("defaults.max-offsets-per-trigger")
      ),
      delta = DeltaConfig(
        targetPath = p.getString("delta.target-path"),
        partitionColumns = p.getStringList("delta.partition-columns").asScala.toSeq,
        checkpointPath = p.getString("delta.checkpoint-path")
      ),
      jsonColumns = if (p.hasPath("json-columns"))
        p.getConfig("json-columns").entrySet().asScala
          .map(e => e.getKey -> e.getValue.unwrapped().toString).toMap
      else Map.empty,
      transformations = if (p.hasPath("transformations"))
        p.getStringList("transformations").asScala.toSeq
      else Seq.empty,
      transformConfig = if (p.hasPath("transform-config"))
        p.getConfig("transform-config").entrySet().asScala
          .map(e => e.getKey -> e.getValue.unwrapped().toString).toMap
      else Map.empty,
      optimizeAfterWrite = g.getBoolean("defaults.optimize-after-write"),
      vacuumEnabled = g.getBoolean("defaults.vacuum-enabled"),
      vacuumRetentionHours = g.getInt("defaults.vacuum-retention-hours")
    )
  }

  private def getOrFallback(primary: Config, primaryPath: String,
                             fallback: Config, fallbackPath: String): String = {
    if (primary.hasPath(primaryPath)) primary.getString(primaryPath)
    else fallback.getString(fallbackPath)
  }
}
