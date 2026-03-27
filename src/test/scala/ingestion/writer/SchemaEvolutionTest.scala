package ingestion.writer

import ingestion.SparkTestBase
import ingestion.config._
import org.scalatest.funsuite.AnyFunSuite
import java.nio.file.Files

class SchemaEvolutionTest extends AnyFunSuite {

  private val spark = SparkTestBase.spark

  private def newConfig(mode: String, targetPath: String,
                         primaryKeys: Seq[String] = Seq.empty): PipelineConfig = {
    PipelineConfig(
      name = "schema-evolution-test",
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

  private def freshPath(): String =
    Files.createTempDirectory("schema-evo-test-").toFile.getAbsolutePath

  // ---------------------------------------------------------------------------
  // Append mode — uses mergeSchema
  // ---------------------------------------------------------------------------

  test("append: new column in later batch is added to the table") {
    import spark.implicits._
    val targetPath = freshPath()
    val config = newConfig("append", targetPath)

    val batch1 = Seq((1, "placed", "2026-03-27"))
      .toDF("order_id", "status", "dt")
    DeltaWriter.write(spark, batch1, config)

    val batch2 = Seq((2, "shipped", 42.0, "2026-03-27"))
      .toDF("order_id", "status", "amount", "dt")
    DeltaWriter.write(spark, batch2, config)

    val result = spark.read.format("delta").load(targetPath)
    assert(result.columns.toSet === Set("order_id", "status", "amount", "dt"))
    assert(result.count() === 2)
  }

  test("append: existing rows get null for newly added column") {
    import spark.implicits._
    val targetPath = freshPath()
    val config = newConfig("append", targetPath)

    val batch1 = Seq((1, "placed", "2026-03-27"))
      .toDF("order_id", "status", "dt")
    DeltaWriter.write(spark, batch1, config)

    val batch2 = Seq((2, "shipped", "express", "2026-03-27"))
      .toDF("order_id", "status", "shipping_type", "dt")
    DeltaWriter.write(spark, batch2, config)

    val result = spark.read.format("delta").load(targetPath)
    val order1 = result.filter("order_id = 1").select("shipping_type")
      .collect()(0)
    assert(order1.isNullAt(0))
  }

  test("append: multiple new columns across successive batches") {
    import spark.implicits._
    val targetPath = freshPath()
    val config = newConfig("append", targetPath)

    val batch1 = Seq((1, "placed", "2026-03-27"))
      .toDF("order_id", "status", "dt")
    DeltaWriter.write(spark, batch1, config)

    val batch2 = Seq((2, "shipped", 42.0, "2026-03-27"))
      .toDF("order_id", "status", "amount", "dt")
    DeltaWriter.write(spark, batch2, config)

    val batch3 = Seq((3, "delivered", 55.0, "premium", "2026-03-28"))
      .toDF("order_id", "status", "amount", "tier", "dt")
    DeltaWriter.write(spark, batch3, config)

    val result = spark.read.format("delta").load(targetPath)
    assert(result.columns.toSet === Set("order_id", "status", "amount", "tier", "dt"))
    assert(result.count() === 3)

    val order1 = result.filter("order_id = 1").collect()(0)
    assert(order1.getAs[Any]("amount") === null)
    assert(order1.getAs[Any]("tier") === null)
  }

  test("append: column dropped in later batch does not remove it from table") {
    import spark.implicits._
    val targetPath = freshPath()
    val config = newConfig("append", targetPath)

    val batch1 = Seq((1, "placed", 10.0, "2026-03-27"))
      .toDF("order_id", "status", "amount", "dt")
    DeltaWriter.write(spark, batch1, config)

    val batch2 = Seq((2, "shipped", "2026-03-27"))
      .toDF("order_id", "status", "dt")
    DeltaWriter.write(spark, batch2, config)

    val result = spark.read.format("delta").load(targetPath)
    assert(result.columns.toSet === Set("order_id", "status", "amount", "dt"))

    val order2 = result.filter("order_id = 2").select("amount").collect()(0)
    assert(order2.isNullAt(0))
  }

  // ---------------------------------------------------------------------------
  // Upsert mode — no mergeSchema; schema set on first write
  // ---------------------------------------------------------------------------

  test("upsert: matching schema across batches merges correctly") {
    import spark.implicits._
    val targetPath = freshPath()
    val config = newConfig("upsert", targetPath, Seq("order_id"))

    val batch1 = Seq((1, "placed", 10.0, "2026-03-27"))
      .toDF("order_id", "status", "amount", "dt")
    DeltaWriter.write(spark, batch1, config)

    val batch2 = Seq((1, "shipped", 10.0, "2026-03-27"))
      .toDF("order_id", "status", "amount", "dt")
    DeltaWriter.write(spark, batch2, config)

    val result = spark.read.format("delta").load(targetPath)
    assert(result.count() === 1)

    val status = result.filter("order_id = 1").select("status")
      .collect()(0).getString(0)
    assert(status === "shipped")
  }

  test("upsert: new column in later batch is silently dropped (no mergeSchema)") {
    import spark.implicits._
    val targetPath = freshPath()
    val config = newConfig("upsert", targetPath, Seq("order_id"))

    val batch1 = Seq((1, "placed", "2026-03-27"))
      .toDF("order_id", "status", "dt")
    DeltaWriter.write(spark, batch1, config)

    val batch2 = Seq((2, "shipped", 42.0, "2026-03-27"))
      .toDF("order_id", "status", "amount", "dt")
    DeltaWriter.write(spark, batch2, config)

    val result = spark.read.format("delta").load(targetPath)
    // Schema does not evolve — "amount" column is not added to the table
    assert(result.columns.toSet === Set("order_id", "status", "dt"))
    assert(result.count() === 2)
  }
}
