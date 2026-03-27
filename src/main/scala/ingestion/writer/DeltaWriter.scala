package ingestion.writer

import ingestion.config.PipelineConfig
import io.delta.tables.DeltaTable
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, row_number}

/**
 * Writes DataFrames to Delta Lake in either append or upsert mode.
 *
 * Append mode: Deduplicates by primary keys (keeping latest by Kafka offset),
 * then appends to Delta. Schema evolution via mergeSchema.
 *
 * Upsert mode: Uses Delta MERGE INTO with primary/composite keys.
 * Design Decision DD-4: Partition columns (e.g., `dt`) are included in the
 * merge condition to enable partition pruning. This is critical because Parquet
 * files are immutable — MERGE must rewrite entire files. Partition pruning
 * limits the rewrite scope to only affected partitions.
 */
object DeltaWriter {

  def write(spark: SparkSession, df: DataFrame, config: PipelineConfig): Unit = {
    config.processing.mode match {
      case "append" => appendWrite(df, config)
      case "upsert" => upsertWrite(spark, df, config)
    }
  }

  private def appendWrite(df: DataFrame, config: PipelineConfig): Unit = {
    val deduped = deduplicateByKeys(df, config.processing.primaryKeys)
    val cleanDf = dropKafkaMetadata(deduped)
    cleanDf.write
      .format("delta")
      .mode("append")
      .option("mergeSchema", "true")
      .partitionBy(config.delta.partitionColumns: _*)
      .save(config.delta.targetPath)
  }

  private def upsertWrite(spark: SparkSession, df: DataFrame,
                           config: PipelineConfig): Unit = {
    val cleanDf = dropKafkaMetadata(df)

    if (!DeltaTable.isDeltaTable(spark, config.delta.targetPath)) {
      cleanDf.write
        .format("delta")
        .partitionBy(config.delta.partitionColumns: _*)
        .save(config.delta.targetPath)
      return
    }

    val deltaTable = DeltaTable.forPath(spark, config.delta.targetPath)
    val keys = config.processing.primaryKeys
    val partCols = config.delta.partitionColumns

    // Primary keys for correctness + partition columns for pruning (DD-4)
    val mergeCondition = (keys ++ partCols).map { c =>
      s"target.`$c` = source.`$c`"
    }.mkString(" AND ")

    deltaTable.as("target")
      .merge(cleanDf.as("source"), mergeCondition)
      .whenMatched.updateAll()
      .whenNotMatched.insertAll()
      .execute()
  }

  /**
   * Deduplicates a batch by primary/composite keys, keeping the record
   * with the highest Kafka offset (latest event wins).
   * If no primary keys are configured, returns the DataFrame as-is.
   */
  private def deduplicateByKeys(df: DataFrame, primaryKeys: Seq[String]): DataFrame = {
    if (primaryKeys.isEmpty) return df

    val window = Window
      .partitionBy(primaryKeys.map(col): _*)
      .orderBy(col("_kafka_offset").desc)

    df.withColumn("_row_num", row_number().over(window))
      .filter(col("_row_num") === 1)
      .drop("_row_num")
  }

  private def dropKafkaMetadata(df: DataFrame): DataFrame = {
    df.drop("_kafka_partition", "_kafka_offset", "_kafka_timestamp")
  }
}
