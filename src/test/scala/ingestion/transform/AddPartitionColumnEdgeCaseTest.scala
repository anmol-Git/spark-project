package ingestion.transform

import ingestion.SparkTestBase
import org.apache.spark.sql.functions.lit
import org.scalatest.funsuite.AnyFunSuite

class AddPartitionColumnEdgeCaseTest extends AnyFunSuite {

  private val spark = SparkTestBase.spark
  private val transform = new AddPartitionColumn()

  private val defaultConfig = Map(
    "source-timestamp-column" -> "updated_at",
    "partition-column-name" -> "dt",
    "partition-format" -> "yyyy-MM-dd"
  )

  test("null timestamp produces null partition value") {
    import spark.implicits._

    val input = Seq((1, null: String)).toDF("order_id", "updated_at")
    val result = transform.apply(input, defaultConfig)

    val dtValue = result.select("dt").collect()(0)
    assert(dtValue.isNullAt(0))
  }

  test("empty string timestamp produces null partition value") {
    import spark.implicits._

    val input = Seq((1, "")).toDF("order_id", "updated_at")
    val result = transform.apply(input, defaultConfig)

    val dtValue = result.select("dt").collect()(0)
    assert(dtValue.isNullAt(0))
  }

  test("mixed null and valid timestamps") {
    import spark.implicits._

    val input = Seq(
      (1, "2026-03-27 10:00:00"),
      (2, null: String),
      (3, "2026-03-28 12:00:00")
    ).toDF("order_id", "updated_at")

    val result = transform.apply(input, defaultConfig)
    val rows = result.orderBy("order_id").select("dt").collect()

    assert(rows(0).getString(0) === "2026-03-27")
    assert(rows(1).isNullAt(0))
    assert(rows(2).getString(0) === "2026-03-28")
  }

  test("timestamps crossing midnight partition correctly") {
    import spark.implicits._

    val input = Seq(
      (1, "2026-03-27 23:59:59"),
      (2, "2026-03-28 00:00:00"),
      (3, "2026-03-28 00:00:01")
    ).toDF("order_id", "updated_at")

    val result = transform.apply(input, defaultConfig)
    val rows = result.orderBy("order_id").select("dt").collect()

    assert(rows(0).getString(0) === "2026-03-27")
    assert(rows(1).getString(0) === "2026-03-28")
    assert(rows(2).getString(0) === "2026-03-28")
  }

  test("hourly partition format") {
    import spark.implicits._

    val config = defaultConfig + ("partition-format" -> "yyyy-MM-dd-HH")
    val input = Seq(
      (1, "2026-03-27 14:30:00"),
      (2, "2026-03-27 15:45:00")
    ).toDF("order_id", "updated_at")

    val result = transform.apply(input, config)
    val rows = result.orderBy("order_id").select("dt").collect()

    assert(rows(0).getString(0) === "2026-03-27-14")
    assert(rows(1).getString(0) === "2026-03-27-15")
  }

  test("missing config key throws NoSuchElementException") {
    import spark.implicits._

    val input = Seq((1, "2026-03-27 10:00:00")).toDF("order_id", "updated_at")
    val incompleteConfig = Map("source-timestamp-column" -> "updated_at")

    assertThrows[NoSuchElementException] {
      transform.apply(input, incompleteConfig)
    }
  }

  test("nonexistent source column throws AnalysisException") {
    import spark.implicits._

    val input = Seq((1, "2026-03-27 10:00:00")).toDF("order_id", "updated_at")
    val config = defaultConfig + ("source-timestamp-column" -> "nonexistent_col")

    assertThrows[org.apache.spark.sql.AnalysisException] {
      transform.apply(input, config).collect()
    }
  }

  test("already existing partition column gets overwritten") {
    import spark.implicits._

    val input = Seq((1, "2026-03-27 10:00:00", "old_value"))
      .toDF("order_id", "updated_at", "dt")

    val result = transform.apply(input, defaultConfig)
    val dt = result.select("dt").collect()(0).getString(0)

    assert(dt === "2026-03-27")
  }
}

