package ingestion

import org.apache.spark.sql.SparkSession

/**
 * Shared SparkSession for all tests. Using a singleton ensures
 * the session is created once and reused across test suites
 * running in the same JVM (sbt test).
 */
object SparkTestBase {

  lazy val spark: SparkSession = {
    SparkSession.builder()
      .master("local[*]")
      .appName("ingestion-tests")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
  }
}
