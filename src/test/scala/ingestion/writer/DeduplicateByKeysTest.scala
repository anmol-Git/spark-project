package ingestion.writer

import ingestion.SparkTestBase
import ingestion.config._
import org.scalatest.funsuite.AnyFunSuite
import java.nio.file.Files

class DeduplicateByKeysTest extends AnyFunSuite {

  private val spark = SparkTestBase.spark

  private def newConfig(targetPath: String,
                         primaryKeys: Seq[String] = Seq.empty): PipelineConfig = {
    PipelineConfig(
      name = "dedup-test",
      kafka = KafkaConfig("localhost:9092", "http://localhost:8081", "test-topic"),
      processing = ProcessingConfig("append", primaryKeys, 100000L),
      delta = DeltaConfig(targetPath, Seq("dt"), "/tmp/checkpoints/test"),
      jsonColumns = Map.empty,
      transformations = Seq.empty,
      transformConfig = Map.empty,
      optimizeAfterWrite = false,
      vacuumEnabled = false,
      vacuumRetentionHours = 168
    )
  }

  private def freshPath(): String =
    Files.createTempDirectory("dedup-test-").toFile.getAbsolutePath

  test("keeps latest record by kafka offset when duplicates exist") {
    import spark.implicits._
    val targetPath = freshPath()
    val config = newConfig(targetPath, Seq("order_id"))

    val df = Seq(
      (0, 100L, 1000L, 1, "placed", "2026-03-27"),
      (0, 200L, 2000L, 1, "shipped", "2026-03-27"),
      (0, 300L, 3000L, 2, "placed", "2026-03-27")
    ).toDF("_kafka_partition", "_kafka_offset", "_kafka_timestamp",
           "order_id", "status", "dt")

    DeltaWriter.write(spark, df, config)

    val result = spark.read.format("delta").load(targetPath)
    assert(result.count() === 2)

    val order1Status = result.filter("order_id = 1").select("status")
      .collect()(0).getString(0)
    assert(order1Status === "shipped")
  }

  test("no dedup when primary keys are empty") {
    import spark.implicits._
    val targetPath = freshPath()
    val config = newConfig(targetPath, Seq.empty)

    val df = Seq(
      (0, 100L, 1000L, 1, "placed", "2026-03-27"),
      (0, 200L, 2000L, 1, "shipped", "2026-03-27")
    ).toDF("_kafka_partition", "_kafka_offset", "_kafka_timestamp",
           "order_id", "status", "dt")

    DeltaWriter.write(spark, df, config)

    val result = spark.read.format("delta").load(targetPath)
    assert(result.count() === 2)
  }

  test("dedup with composite primary keys") {
    import spark.implicits._
    val targetPath = freshPath()
    val config = newConfig(targetPath, Seq("order_id", "item_id"))

    val df = Seq(
      (0, 100L, 1000L, 1, 10, "placed", "2026-03-27"),
      (0, 200L, 2000L, 1, 10, "shipped", "2026-03-27"),
      (0, 300L, 3000L, 1, 20, "placed", "2026-03-27")
    ).toDF("_kafka_partition", "_kafka_offset", "_kafka_timestamp",
           "order_id", "item_id", "status", "dt")

    DeltaWriter.write(spark, df, config)

    val result = spark.read.format("delta").load(targetPath)
    assert(result.count() === 2)

    val item10Status = result.filter("order_id = 1 AND item_id = 10")
      .select("status").collect()(0).getString(0)
    assert(item10Status === "shipped")
  }

  test("single record per key passes through unchanged") {
    import spark.implicits._
    val targetPath = freshPath()
    val config = newConfig(targetPath, Seq("order_id"))

    val df = Seq(
      (0, 100L, 1000L, 1, "placed", "2026-03-27"),
      (0, 200L, 2000L, 2, "shipped", "2026-03-27"),
      (0, 300L, 3000L, 3, "cancelled", "2026-03-27")
    ).toDF("_kafka_partition", "_kafka_offset", "_kafka_timestamp",
           "order_id", "status", "dt")

    DeltaWriter.write(spark, df, config)

    val result = spark.read.format("delta").load(targetPath)
    assert(result.count() === 3)
  }

  test("all duplicates for same key — keeps highest offset") {
    import spark.implicits._
    val targetPath = freshPath()
    val config = newConfig(targetPath, Seq("order_id"))

    val df = Seq(
      (0, 10L, 1000L, 1, "placed", "2026-03-27"),
      (0, 20L, 2000L, 1, "confirmed", "2026-03-27"),
      (0, 30L, 3000L, 1, "shipped", "2026-03-27"),
      (0, 40L, 4000L, 1, "delivered", "2026-03-27")
    ).toDF("_kafka_partition", "_kafka_offset", "_kafka_timestamp",
           "order_id", "status", "dt")

    DeltaWriter.write(spark, df, config)

    val result = spark.read.format("delta").load(targetPath)
    assert(result.count() === 1)

    val status = result.select("status").collect()(0).getString(0)
    assert(status === "delivered")
  }
}

