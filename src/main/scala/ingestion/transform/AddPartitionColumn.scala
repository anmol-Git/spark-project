package ingestion.transform

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * Derives a partition column (e.g., `dt`) from a timestamp source column.
 *
 * Required config keys:
 *   - source-timestamp-column: the source column containing a timestamp
 *   - partition-column-name: the name of the derived partition column (e.g., "dt")
 *   - partition-format: date format string (e.g., "yyyy-MM-dd")
 *
 * This is the standard transformation for creating the `dt` partition column
 * used in Delta table partitioning and MERGE partition pruning (DD-4).
 */
class AddPartitionColumn extends Transformation {

  override def apply(df: DataFrame, config: Map[String, String]): DataFrame = {
    val sourceCol = config("source-timestamp-column")
    val partCol = config("partition-column-name")
    val fmt = config("partition-format")
    df.withColumn(partCol, date_format(col(sourceCol), fmt))
  }
}
