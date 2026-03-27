package ingestion.writer

import ingestion.SparkTestBase
import ingestion.config._
import io.delta.tables.DeltaTable
import org.scalatest.funsuite.AnyFunSuite
import java.nio.file.Files

class DeltaWriterTest extends AnyFunSuite {

  // Shared SparkSession — stable identifier for spark.implicits._
  private val spark = SparkTestBase.spark

  private def newConfig(mode: String, targetPath: String,
                         primaryKeys: Seq[String] = Seq.empty): PipelineConfig = {
    PipelineConfig(
      name = "test",
      kafka = KafkaConfig("localhost:9092", "http://localhost:8081", "test-topic"),
      processing = ProcessingConfig(mode, primaryKeys, 100000L),
      delta = DeltaConfig(targetPath, Seq("dt"), "/tmp/checkpoints/test"),
      jsonColumns = Map.empty,
      transformations = Seq.empty,
      transformConfig = Map.empty,
      optimizeAfterWrite = false,
      vacuumEnabled = false,
      vacuumRetentionHours = 168
    )
  }

  private def freshPath(): String = {
    Files.createTempDirectory("delta-writer-test-").toFile.getAbsolutePath
  }

  test("append mode creates Delta table with correct data") {
    import spark.implicits._
    val targetPath = freshPath()

    val df = Seq(
      (1, "placed", "2026-03-27"),
      (2, "shipped", "2026-03-27")
    ).toDF("order_id", "status", "dt")

    val config = newConfig("append", targetPath)
    DeltaWriter.write(spark, df, config)

    val result = spark.read.format("delta").load(targetPath)
    assert(result.count() === 2)
    assert(result.columns.toSeq.toSet === Set("order_id", "status", "dt"))
  }

  test("append mode adds new rows without modifying existing") {
    import spark.implicits._
    val targetPath = freshPath()
    val config = newConfig("append", targetPath)

    val batch1 = Seq((1, "placed", "2026-03-27")).toDF("order_id", "status", "dt")
    DeltaWriter.write(spark, batch1, config)

    val batch2 = Seq((2, "shipped", "2026-03-28")).toDF("order_id", "status", "dt")
    DeltaWriter.write(spark, batch2, config)

    val result = spark.read.format("delta").load(targetPath)
    assert(result.count() === 2)
  }

  test("upsert mode creates table on first write") {
    import spark.implicits._
    val targetPath = freshPath()

    val df = Seq(
      (1, "placed", "2026-03-27"),
      (2, "placed", "2026-03-27")
    ).toDF("order_id", "status", "dt")

    val config = newConfig("upsert", targetPath, Seq("order_id"))
    DeltaWriter.write(spark, df, config)

    val result = spark.read.format("delta").load(targetPath)
    assert(result.count() === 2)
  }

  test("upsert mode merges updates by primary key") {
    import spark.implicits._
    val targetPath = freshPath()
    val config = newConfig("upsert", targetPath, Seq("order_id"))

    val batch1 = Seq(
      (1, "placed", "2026-03-27"),
      (2, "placed", "2026-03-27")
    ).toDF("order_id", "status", "dt")
    DeltaWriter.write(spark, batch1, config)

    val batch2 = Seq(
      (1, "shipped", "2026-03-27"),
      (3, "placed", "2026-03-28")
    ).toDF("order_id", "status", "dt")
    DeltaWriter.write(spark, batch2, config)

    val result = spark.read.format("delta").load(targetPath)
    assert(result.count() === 3)

    val order1Status = result.filter("order_id = 1").select("status")
      .collect()(0).getString(0)
    assert(order1Status === "shipped")
  }

  test("upsert mode is idempotent — reprocessing same data produces same result") {
    import spark.implicits._
    val targetPath = freshPath()
    val config = newConfig("upsert", targetPath, Seq("order_id"))

    val df = Seq(
      (1, "placed", "2026-03-27"),
      (2, "shipped", "2026-03-27")
    ).toDF("order_id", "status", "dt")

    DeltaWriter.write(spark, df, config)
    DeltaWriter.write(spark, df, config)

    val result = spark.read.format("delta").load(targetPath)
    assert(result.count() === 2)
  }

  test("drops kafka metadata columns before writing") {
    import spark.implicits._
    val targetPath = freshPath()

    val df = Seq(
      (0, 12345L, 1711540200000L, 1, "placed", "2026-03-27")
    ).toDF("_kafka_partition", "_kafka_offset", "_kafka_timestamp",
           "order_id", "status", "dt")

    val config = newConfig("append", targetPath)
    DeltaWriter.write(spark, df, config)

    val result = spark.read.format("delta").load(targetPath)
    val cols = result.columns.toSeq
    assert(!cols.contains("_kafka_partition"))
    assert(!cols.contains("_kafka_offset"))
    assert(!cols.contains("_kafka_timestamp"))
    assert(cols.toSet === Set("order_id", "status", "dt"))
  }
}
