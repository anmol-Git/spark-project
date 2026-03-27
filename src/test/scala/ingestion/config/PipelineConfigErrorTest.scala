package ingestion.config

import com.typesafe.config.ConfigException
import org.scalatest.funsuite.AnyFunSuite
import java.io.PrintWriter
import java.nio.file.Files

class PipelineConfigErrorTest extends AnyFunSuite {

  private def writeTempConfig(content: String): String = {
    val file = Files.createTempFile("pipeline-error-test-", ".conf").toFile
    file.deleteOnExit()
    val pw = new PrintWriter(file)
    pw.write(content)
    pw.close()
    file.getAbsolutePath
  }

  test("missing pipeline name throws ConfigException") {
    val path = writeTempConfig(
      """
        |pipeline {
        |  kafka { topic = "test-topic" }
        |  delta {
        |    target-path = "/tmp/delta"
        |    partition-columns = ["dt"]
        |    checkpoint-path = "/tmp/checkpoint"
        |  }
        |}
      """.stripMargin)

    assertThrows[ConfigException] {
      PipelineConfig.load(path)
    }
  }

  test("missing kafka topic throws ConfigException") {
    val path = writeTempConfig(
      """
        |pipeline {
        |  name = "test"
        |  kafka { }
        |  delta {
        |    target-path = "/tmp/delta"
        |    partition-columns = ["dt"]
        |    checkpoint-path = "/tmp/checkpoint"
        |  }
        |}
      """.stripMargin)

    assertThrows[ConfigException] {
      PipelineConfig.load(path)
    }
  }

  test("missing delta target-path throws ConfigException") {
    val path = writeTempConfig(
      """
        |pipeline {
        |  name = "test"
        |  kafka { topic = "test-topic" }
        |  delta {
        |    partition-columns = ["dt"]
        |    checkpoint-path = "/tmp/checkpoint"
        |  }
        |}
      """.stripMargin)

    assertThrows[ConfigException] {
      PipelineConfig.load(path)
    }
  }

  test("missing delta checkpoint-path throws ConfigException") {
    val path = writeTempConfig(
      """
        |pipeline {
        |  name = "test"
        |  kafka { topic = "test-topic" }
        |  delta {
        |    target-path = "/tmp/delta"
        |    partition-columns = ["dt"]
        |  }
        |}
      """.stripMargin)

    assertThrows[ConfigException] {
      PipelineConfig.load(path)
    }
  }

  test("missing delta partition-columns throws ConfigException") {
    val path = writeTempConfig(
      """
        |pipeline {
        |  name = "test"
        |  kafka { topic = "test-topic" }
        |  delta {
        |    target-path = "/tmp/delta"
        |    checkpoint-path = "/tmp/checkpoint"
        |  }
        |}
      """.stripMargin)

    assertThrows[ConfigException] {
      PipelineConfig.load(path)
    }
  }

  test("nonexistent config file throws exception") {
    assertThrows[Exception] {
      PipelineConfig.load("/nonexistent/path/pipeline.conf")
    }
  }

  test("empty config file throws ConfigException") {
    val path = writeTempConfig("")

    assertThrows[ConfigException] {
      PipelineConfig.load(path)
    }
  }

  test("malformed HOCON throws ConfigException") {
    val path = writeTempConfig("this is not { valid hocon {{}")

    assertThrows[ConfigException] {
      PipelineConfig.load(path)
    }
  }
}

