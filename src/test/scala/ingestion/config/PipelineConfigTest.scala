package ingestion.config

import org.scalatest.funsuite.AnyFunSuite
import java.io.{File, PrintWriter}
import java.nio.file.Files

class PipelineConfigTest extends AnyFunSuite {

  private def writeTempConfig(content: String): String = {
    val file = Files.createTempFile("pipeline-test-", ".conf").toFile
    file.deleteOnExit()
    val pw = new PrintWriter(file)
    pw.write(content)
    pw.close()
    file.getAbsolutePath
  }

  test("loads pipeline config with all fields") {
    val confPath = writeTempConfig(
      """
        |pipeline {
        |  name = "orders"
        |  kafka {
        |    topic = "dbserver1.inventory.orders"
        |  }
        |  processing {
        |    mode = "upsert"
        |    primary-keys = ["order_id"]
        |    max-offsets-per-trigger = 50000
        |  }
        |  delta {
        |    target-path = "/data/delta/orders"
        |    partition-columns = ["dt"]
        |    checkpoint-path = "/data/checkpoints/orders"
        |  }
        |  json-columns {
        |    metadata = "string"
        |  }
        |  transformations = ["ingestion.transform.AddPartitionColumn"]
        |  transform-config {
        |    source-timestamp-column = "updated_at"
        |    partition-column-name = "dt"
        |    partition-format = "yyyy-MM-dd"
        |  }
        |}
      """.stripMargin)

    val config = PipelineConfig.load(confPath)

    assert(config.name === "orders")
    assert(config.kafka.topic === "dbserver1.inventory.orders")
    assert(config.processing.mode === "upsert")
    assert(config.processing.primaryKeys === Seq("order_id"))
    assert(config.processing.maxOffsetsPerTrigger === 50000L)
    assert(config.delta.targetPath === "/data/delta/orders")
    assert(config.delta.partitionColumns === Seq("dt"))
    assert(config.delta.checkpointPath === "/data/checkpoints/orders")
    assert(config.jsonColumns === Map("metadata" -> "string"))
    assert(config.transformations === Seq("ingestion.transform.AddPartitionColumn"))
    assert(config.transformConfig("source-timestamp-column") === "updated_at")
    assert(config.transformConfig("partition-column-name") === "dt")
    assert(config.transformConfig("partition-format") === "yyyy-MM-dd")
  }

  test("falls back to global defaults when pipeline omits optional fields") {
    val confPath = writeTempConfig(
      """
        |pipeline {
        |  name = "minimal"
        |  kafka {
        |    topic = "dbserver1.inventory.users"
        |  }
        |  delta {
        |    target-path = "/data/delta/users"
        |    partition-columns = ["dt"]
        |    checkpoint-path = "/data/checkpoints/users"
        |  }
        |}
      """.stripMargin)

    val config = PipelineConfig.load(confPath)

    // Falls back to application.conf global defaults
    assert(config.kafka.bootstrapServers === "localhost:9092")
    assert(config.kafka.schemaRegistryUrl === "http://localhost:8081")
    assert(config.processing.mode === "append")
    assert(config.processing.primaryKeys === Seq.empty)
    assert(config.processing.maxOffsetsPerTrigger === 100000L)
    assert(config.jsonColumns === Map.empty)
    assert(config.transformations === Seq.empty)
    assert(config.transformConfig === Map.empty)
    assert(config.optimizeAfterWrite === true)
    assert(config.vacuumEnabled === false)
    assert(config.vacuumRetentionHours === 168)
  }

  test("pipeline-level kafka config overrides global defaults") {
    val confPath = writeTempConfig(
      """
        |pipeline {
        |  name = "custom-kafka"
        |  kafka {
        |    bootstrap-servers = "kafka-prod:9093"
        |    schema-registry-url = "http://registry-prod:8081"
        |    topic = "dbserver1.inventory.products"
        |  }
        |  delta {
        |    target-path = "/data/delta/products"
        |    partition-columns = ["dt"]
        |    checkpoint-path = "/data/checkpoints/products"
        |  }
        |}
      """.stripMargin)

    val config = PipelineConfig.load(confPath)

    assert(config.kafka.bootstrapServers === "kafka-prod:9093")
    assert(config.kafka.schemaRegistryUrl === "http://registry-prod:8081")
  }

  test("supports composite primary keys") {
    val confPath = writeTempConfig(
      """
        |pipeline {
        |  name = "composite"
        |  kafka { topic = "dbserver1.inventory.order_items" }
        |  processing {
        |    mode = "upsert"
        |    primary-keys = ["order_id", "item_id"]
        |  }
        |  delta {
        |    target-path = "/data/delta/order_items"
        |    partition-columns = ["dt"]
        |    checkpoint-path = "/data/checkpoints/order_items"
        |  }
        |}
      """.stripMargin)

    val config = PipelineConfig.load(confPath)

    assert(config.processing.primaryKeys === Seq("order_id", "item_id"))
  }
}
