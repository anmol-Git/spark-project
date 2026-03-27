package ingestion.writer

import ingestion.SparkTestBase
import ingestion.config._
import io.delta.tables.DeltaTable
import org.scalatest.funsuite.AnyFunSuite
import java.nio.file.Files

class DeltaWriterEdgeCaseTest extends AnyFunSuite {

  private val spark = SparkTestBase.spark

  private def newConfig(mode: String, targetPath: String,
                         primaryKeys: Seq[String] = Seq.empty,
                         partitionColumns: Seq[String] = Seq("dt")): PipelineConfig = {
    PipelineConfig(
      name = "edge-case-test",
      kafka = KafkaConfig("localhost:9092", "http://localhost:8081", "test-topic"),
      processing = ProcessingConfig(mode, primaryKeys, 100000L),
      delta = DeltaConfig(targetPath, partitionColumns, "/tmp/checkpoints/test"),
      jsonColumns = Map.empty,
      transformations = Seq.empty,
      transformConfig = Map.empty,
      optimizeAfterWrite = false,
      vacuumEnabled = false,
      vacuumRetentionHours = 168
    )
  }

  private def freshPath(): String =
    Files.createTempDirectory("edge-case-test-").toFile.getAbsolutePath

  // ---------------------------------------------------------------------------
  // Empty DataFrame handling
  // ---------------------------------------------------------------------------

  test("append with empty dataframe creates empty Delta table") {
    import spark.implicits._
    val targetPath = freshPath()
    val config = newConfig("append", targetPath)

    val df = Seq.empty[(Int, String, String)]
      .toDF("order_id", "status", "dt")

    DeltaWriter.write(spark, df, config)

    val result = spark.read.format("delta").load(targetPath)
    assert(result.count() === 0)
    assert(result.columns.toSet === Set("order_id", "status", "dt"))
  }

  test("upsert with empty dataframe on first write creates empty Delta table") {
    import spark.implicits._
    val targetPath = freshPath()
    val config = newConfig("upsert", targetPath, Seq("order_id"))

    val df = Seq.empty[(Int, String, String)]
      .toDF("order_id", "status", "dt")

    DeltaWriter.write(spark, df, config)

    val result = spark.read.format("delta").load(targetPath)
    assert(result.count() === 0)
  }

  test("upsert with empty dataframe on existing table is a no-op") {
    import spark.implicits._
    val targetPath = freshPath()
    val config = newConfig("upsert", targetPath, Seq("order_id"))

    val batch1 = Seq((1, "placed", "2026-03-27"))
      .toDF("order_id", "status", "dt")
    DeltaWriter.write(spark, batch1, config)

    val emptyDf = Seq.empty[(Int, String, String)]
      .toDF("order_id", "status", "dt")
    DeltaWriter.write(spark, emptyDf, config)

    val result = spark.read.format("delta").load(targetPath)
    assert(result.count() === 1)
  }

  // ---------------------------------------------------------------------------
  // Composite key upsert
  // ---------------------------------------------------------------------------

  test("upsert with composite key updates only matching combination") {
    import spark.implicits._
    val targetPath = freshPath()
    val config = newConfig("upsert", targetPath, Seq("order_id", "item_id"))

    val batch1 = Seq(
      (1, 10, "placed", "2026-03-27"),
      (1, 20, "placed", "2026-03-27"),
      (2, 10, "placed", "2026-03-27")
    ).toDF("order_id", "item_id", "status", "dt")
    DeltaWriter.write(spark, batch1, config)

    val batch2 = Seq(
      (1, 10, "shipped", "2026-03-27")
    ).toDF("order_id", "item_id", "status", "dt")
    DeltaWriter.write(spark, batch2, config)

    val result = spark.read.format("delta").load(targetPath)
    assert(result.count() === 3)

    val updated = result.filter("order_id = 1 AND item_id = 10")
      .select("status").collect()(0).getString(0)
    assert(updated === "shipped")

    val unchanged = result.filter("order_id = 1 AND item_id = 20")
      .select("status").collect()(0).getString(0)
    assert(unchanged === "placed")
  }

  // ---------------------------------------------------------------------------
  // Partition pruning in merge
  // ---------------------------------------------------------------------------

  test("upsert only modifies rows in matching partition") {
    import spark.implicits._
    val targetPath = freshPath()
    val config = newConfig("upsert", targetPath, Seq("order_id"))

    val batch1 = Seq(
      (1, "placed", "2026-03-27"),
      (2, "placed", "2026-03-28")
    ).toDF("order_id", "status", "dt")
    DeltaWriter.write(spark, batch1, config)

    // Update order 1 with same partition
    val batch2 = Seq((1, "shipped", "2026-03-27"))
      .toDF("order_id", "status", "dt")
    DeltaWriter.write(spark, batch2, config)

    val result = spark.read.format("delta").load(targetPath)
    assert(result.count() === 2)

    val order1 = result.filter("order_id = 1").collect()(0)
    assert(order1.getAs[String]("status") === "shipped")
    assert(order1.getAs[String]("dt") === "2026-03-27")

    val order2 = result.filter("order_id = 2").collect()(0)
    assert(order2.getAs[String]("status") === "placed")
  }

  // ---------------------------------------------------------------------------
  // Null value handling
  // ---------------------------------------------------------------------------

  test("append preserves null values in non-key columns") {
    import spark.implicits._
    val targetPath = freshPath()
    val config = newConfig("append", targetPath)

    val df = Seq((1, null: String, "2026-03-27"))
      .toDF("order_id", "status", "dt")
    DeltaWriter.write(spark, df, config)

    val result = spark.read.format("delta").load(targetPath)
    val row = result.collect()(0)
    assert(row.isNullAt(row.fieldIndex("status")))
  }

  test("upsert preserves null values in non-key columns") {
    import spark.implicits._
    val targetPath = freshPath()
    val config = newConfig("upsert", targetPath, Seq("order_id"))

    val batch1 = Seq((1, "placed", "2026-03-27"))
      .toDF("order_id", "status", "dt")
    DeltaWriter.write(spark, batch1, config)

    val batch2 = Seq((1, null: String, "2026-03-27"))
      .toDF("order_id", "status", "dt")
    DeltaWriter.write(spark, batch2, config)

    val result = spark.read.format("delta").load(targetPath)
    assert(result.count() === 1)

    val row = result.collect()(0)
    assert(row.isNullAt(row.fieldIndex("status")))
  }

  // ---------------------------------------------------------------------------
  // Multiple partitions
  // ---------------------------------------------------------------------------

  test("append writes across multiple partitions correctly") {
    import spark.implicits._
    val targetPath = freshPath()
    val config = newConfig("append", targetPath)

    val df = Seq(
      (1, "placed", "2026-03-27"),
      (2, "placed", "2026-03-28"),
      (3, "placed", "2026-03-29")
    ).toDF("order_id", "status", "dt")
    DeltaWriter.write(spark, df, config)

    val result = spark.read.format("delta").load(targetPath)
    val partitions = result.select("dt").distinct().collect().map(_.getString(0)).sorted
    assert(partitions.toSeq === Seq("2026-03-27", "2026-03-28", "2026-03-29"))
  }

  test("Delta table version increments on each write") {
    import spark.implicits._
    val targetPath = freshPath()
    val config = newConfig("append", targetPath)

    val batch1 = Seq((1, "placed", "2026-03-27")).toDF("order_id", "status", "dt")
    DeltaWriter.write(spark, batch1, config)

    val batch2 = Seq((2, "placed", "2026-03-28")).toDF("order_id", "status", "dt")
    DeltaWriter.write(spark, batch2, config)

    val history = DeltaTable.forPath(spark, targetPath).history()
    assert(history.count() === 2)
  }
}

