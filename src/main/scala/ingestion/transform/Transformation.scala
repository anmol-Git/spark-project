package ingestion.transform

import org.apache.spark.sql.DataFrame

/**
 * Pluggable transformation interface (Design Decision DD-5).
 *
 * Implementations are loaded by fully qualified class name from pipeline config
 * and applied sequentially to each micro-batch DataFrame before writing to Delta.
 *
 * New transformations are added by:
 * 1. Implementing this trait
 * 2. Adding the class name to the pipeline's `transformations` list in HOCON config
 *
 * No framework changes needed — just config + Class.forName.
 */
trait Transformation {
  def apply(df: DataFrame, config: Map[String, String]): DataFrame
}
