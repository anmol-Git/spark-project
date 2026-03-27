package ingestion

import ingestion.config.PipelineConfig
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, NewTopic}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.sql.SparkSession
import io.delta.tables.DeltaTable

import java.util.{Collections, Properties}

/**
 * End-to-end test runner.
 *
 * Produces Avro messages to Kafka → runs the ingestion pipeline → verifies Delta output.
 * Demonstrates both initial insert and upsert (MERGE) behavior.
 *
 * Prerequisites: Kafka + Schema Registry running (docker-compose up -d)
 * Usage: sbt "runMain ingestion.E2ERunner"
 */
object E2ERunner {

  private val SCHEMA_JSON =
    """{
      |  "type": "record",
      |  "name": "Order",
      |  "namespace": "inventory",
      |  "fields": [
      |    {"name": "order_id", "type": "int"},
      |    {"name": "status", "type": "string"},
      |    {"name": "metadata", "type": "string"},
      |    {"name": "updated_at", "type": "string"}
      |  ]
      |}""".stripMargin

  private val TOPIC = "orders-e2e"
  private val CONFIG_PATH = "src/main/resources/pipelines/orders-e2e.conf"

  def main(args: Array[String]): Unit = {
    // Clean previous run data (Delta + checkpoints + Kafka topic)
    deleteDirectory("/tmp/e2e-delta")
    deleteDirectory("/tmp/e2e-checkpoints")
    resetKafkaTopic()

    val config = PipelineConfig.load(CONFIG_PATH)

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("E2E-Runner")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.ui.enabled", "false")
      .getOrCreate()

    try {
      // === Step 1: Produce initial orders ===
      println("\n" + "=" * 60)
      println("  STEP 1: Producing 10 orders to Kafka (Avro + Schema Registry)")
      println("=" * 60)

      val initialOrders = (1 to 10).map { i =>
        (i, "placed", s"""{"source":"web","item_count":$i}""", s"2026-03-27 10:0$i:00")
      }
      produceAvroRecords(initialOrders)
      println(s"  Produced ${initialOrders.size} records to topic '$TOPIC'")

      // === Step 2: Run pipeline ===
      println("\n" + "=" * 60)
      println("  STEP 2: Running ingestion pipeline (upsert mode)")
      println("=" * 60)

      IngestionApp.run(spark, config)
      println("  Pipeline completed.")

      // === Step 3: Verify initial load ===
      println("\n" + "=" * 60)
      println("  STEP 3: Reading Delta table — initial load")
      println("=" * 60)

      val df1 = spark.read.format("delta").load(config.delta.targetPath)
      df1.orderBy("order_id").show(20, truncate = false)
      println(s"  Total rows: ${df1.count()}")

      // === Step 4: Produce updates ===
      println("\n" + "=" * 60)
      println("  STEP 4: Producing 3 updates (orders 1,2,3 → shipped)")
      println("=" * 60)

      val updates = Seq(
        (1, "shipped", """{"source":"web","item_count":1,"shipped_by":"express"}""", "2026-03-27 11:00:00"),
        (2, "shipped", """{"source":"web","item_count":2,"shipped_by":"standard"}""", "2026-03-27 11:05:00"),
        (3, "shipped", """{"source":"web","item_count":3,"shipped_by":"express"}""", "2026-03-27 11:10:00")
      )
      produceAvroRecords(updates)
      println(s"  Produced ${updates.size} update records to topic '$TOPIC'")

      // === Step 5: Re-run pipeline ===
      println("\n" + "=" * 60)
      println("  STEP 5: Re-running pipeline (MERGE will update orders 1,2,3)")
      println("=" * 60)

      IngestionApp.run(spark, config)
      println("  Pipeline completed.")

      // === Step 6: Verify MERGE ===
      println("\n" + "=" * 60)
      println("  STEP 6: Verifying MERGE — orders 1,2,3 should be 'shipped'")
      println("=" * 60)

      val df2 = spark.read.format("delta").load(config.delta.targetPath)
      df2.orderBy("order_id").show(20, truncate = false)
      println(s"  Total rows: ${df2.count()} (should still be 10 — MERGE updated in place)")

      // Verify specific rows
      val shippedCount = df2.filter("status = 'shipped'").count()
      val placedCount = df2.filter("status = 'placed'").count()
      println(s"  Shipped: $shippedCount, Placed: $placedCount")

      if (df2.count() == 10 && shippedCount == 3 && placedCount == 7) {
        println("\n  E2E TEST PASSED — upsert MERGE working correctly!")
      } else {
        println("\n  E2E TEST FAILED — unexpected row counts")
      }

      // === Step 7: Show Delta history ===
      println("\n" + "=" * 60)
      println("  STEP 7: Delta table history (version log)")
      println("=" * 60)

      DeltaTable.forPath(spark, config.delta.targetPath)
        .history()
        .select("version", "timestamp", "operation", "operationMetrics")
        .show(truncate = false)

    } finally {
      spark.stop()
    }
  }

  private def produceAvroRecords(records: Seq[(Int, String, String, String)]): Unit = {
    val schema = new Schema.Parser().parse(SCHEMA_JSON)

    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "io.confluent.kafka.serializers.KafkaAvroSerializer")
    props.put("schema.registry.url", "http://localhost:8081")

    val producer = new KafkaProducer[String, GenericRecord](props)

    try {
      records.foreach { case (orderId, status, metadata, updatedAt) =>
        val record = new GenericData.Record(schema)
        record.put("order_id", orderId)
        record.put("status", status)
        record.put("metadata", metadata)
        record.put("updated_at", updatedAt)

        val producerRecord = new ProducerRecord[String, GenericRecord](
          TOPIC, orderId.toString, record)
        producer.send(producerRecord).get() // synchronous for E2E reliability
      }
    } finally {
      producer.close()
    }
  }

  private def resetKafkaTopic(): Unit = {
    val props = new Properties()
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    val admin = AdminClient.create(props)
    try {
      val topics = admin.listTopics().names().get()
      if (topics.contains(TOPIC)) {
        admin.deleteTopics(Collections.singletonList(TOPIC)).all().get()
        Thread.sleep(2000) // wait for topic deletion to propagate
      }
      val newTopic = new NewTopic(TOPIC, 1, 1.toShort)
      admin.createTopics(Collections.singletonList(newTopic)).all().get()
      Thread.sleep(1000) // wait for topic creation
    } finally {
      admin.close()
    }
  }

  private def deleteDirectory(path: String): Unit = {
    import java.io.File
    def delete(f: File): Unit = {
      if (f.isDirectory) f.listFiles().foreach(delete)
      f.delete()
    }
    val dir = new File(path)
    if (dir.exists()) delete(dir)
  }
}
