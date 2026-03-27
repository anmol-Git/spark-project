package ingestion.transform

import ingestion.SparkTestBase
import org.scalatest.funsuite.AnyFunSuite

class AddPartitionColumnTest extends AnyFunSuite {

  // Shared SparkSession — stable identifier for spark.implicits._
  private val spark = SparkTestBase.spark

  test("derives dt partition column from timestamp column") {
    import spark.implicits._

    val input = Seq(
      (1, "2026-03-27 14:30:00"),
      (2, "2026-03-28 09:15:00"),
      (3, "2026-03-27 23:59:59")
    ).toDF("order_id", "updated_at")

    val config = Map(
      "source-timestamp-column" -> "updated_at",
      "partition-column-name" -> "dt",
      "partition-format" -> "yyyy-MM-dd"
    )

    val transform = new AddPartitionColumn()
    val result = transform.apply(input, config)

    assert(result.columns.toSeq.contains("dt"))

    val dtValues = result.select("dt").collect().map(_.getString(0)).sorted
    assert(dtValues.toSeq === Seq("2026-03-27", "2026-03-27", "2026-03-28"))
  }

  test("preserves all original columns") {
    import spark.implicits._

    val input = Seq(
      (1, "pending", "2026-03-27 10:00:00")
    ).toDF("order_id", "status", "updated_at")

    val config = Map(
      "source-timestamp-column" -> "updated_at",
      "partition-column-name" -> "dt",
      "partition-format" -> "yyyy-MM-dd"
    )

    val result = new AddPartitionColumn().apply(input, config)

    assert(result.columns.toSeq.toSet === Set("order_id", "status", "updated_at", "dt"))
  }

  test("supports custom partition format") {
    import spark.implicits._

    val input = Seq(
      (1, "2026-03-27 14:30:00")
    ).toDF("id", "created_at")

    val config = Map(
      "source-timestamp-column" -> "created_at",
      "partition-column-name" -> "month",
      "partition-format" -> "yyyy-MM"
    )

    val result = new AddPartitionColumn().apply(input, config)

    assert(result.columns.toSeq.contains("month"))
    assert(result.select("month").collect()(0).getString(0) === "2026-03")
  }
}
