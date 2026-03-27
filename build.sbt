name := "data-ingestion-framework"
version := "0.1.0"
scalaVersion := "2.12.18"

val sparkVersion = "3.5.1"
val deltaVersion = "3.1.0"
val abrisVersion = "6.4.0"

libraryDependencies ++= Seq(
  // Spark
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql"  % sparkVersion % "provided",

  // Kafka connector (streaming)
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,

  // Avro support (required by ABRiS for deserialization)
  "org.apache.spark" %% "spark-avro" % sparkVersion,

  // Open Delta Lake
  "io.delta" %% "delta-spark" % deltaVersion,

  // Schema Registry + Avro deserialization
  "za.co.absa" %% "abris" % abrisVersion,

  // Config
  "com.typesafe" % "config" % "1.4.3",

  // Testing
  "org.scalatest" %% "scalatest" % "3.2.17" % Test
)

// ABRiS requires Confluent Maven repo
resolvers += "Confluent" at "https://packages.confluent.io/maven/"

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", _*) => MergeStrategy.discard
  case _                        => MergeStrategy.first
}
