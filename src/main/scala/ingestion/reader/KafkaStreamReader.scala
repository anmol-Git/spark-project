package ingestion.reader

import ingestion.config.KafkaConfig
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import za.co.absa.abris.avro.functions.from_avro
import za.co.absa.abris.config.AbrisConfig

/**
 * Reads Kafka topic as a Structured Streaming source.
 *
 * Design Decision DD-1: Uses readStream (not batch read) to enable
 * Spark's built-in checkpoint-based offset management. Combined with
 * Trigger.AvailableNow() in IngestionApp, this behaves like a batch job
 * while giving us exactly-once semantics for free.
 *
 * Design Decision DD-2: Uses ABRiS for Avro deserialization, which
 * integrates natively with Confluent Schema Registry — no manual
 * schema fetch or custom UDF needed.
 *
 * Binary payload handling: Avro `bytes` fields are deserialized to Spark
 * BinaryType automatically by ABRiS and written to Delta as BinaryType.
 * No special handling required — binary data (encoded payloads, emojis)
 * passes through the pipeline transparently.
 */
object KafkaStreamReader {

  def readStream(spark: SparkSession, kafkaConfig: KafkaConfig,
                 maxOffsetsPerTrigger: Long): DataFrame = {

    val rawDf = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaConfig.bootstrapServers)
      .option("subscribe", kafkaConfig.topic)
      .option("maxOffsetsPerTrigger", maxOffsetsPerTrigger)
      .option("startingOffsets", "earliest")
      .load()

    val abrisConfig = AbrisConfig
      .fromConfluentAvro
      .downloadReaderSchemaByLatestVersion
      .andTopicNameStrategy(kafkaConfig.topic, isKey = false)
      .usingSchemaRegistry(kafkaConfig.schemaRegistryUrl)

    rawDf.select(
      col("partition").as("_kafka_partition"),
      col("offset").as("_kafka_offset"),
      col("timestamp").as("_kafka_timestamp"),
      from_avro(col("value"), abrisConfig).as("data")
    ).select("_kafka_partition", "_kafka_offset", "_kafka_timestamp", "data.*")
  }
}
