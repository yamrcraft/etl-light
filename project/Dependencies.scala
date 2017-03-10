import sbt.Keys._
import sbt._

object Dependencies {

  def compileDependencies(deps: ModuleID*): Seq[Setting[_]] = Def.settings(libraryDependencies ++= deps map (_ % "compile"))

  def testDependencies(deps: ModuleID*): Seq[Setting[_]] = Def.settings(libraryDependencies ++= deps map (_ % "test"))

  def providedDependencies(deps: ModuleID*): Seq[Setting[_]] = Def.settings(libraryDependencies ++= deps map (_ % "provided"))

  val typesafeConfig = "com.typesafe" % "config" % "1.3.0"

  val jodaTime = "joda-time" % "joda-time" % "2.8.1"
  val jodaConvert = "org.joda" % "joda-convert" % "1.8.1"

  val parquetAvro = "org.apache.parquet" % "parquet-avro" % "1.9.0"

  val parquetProto = "org.apache.parquet" % "parquet-protobuf" % "1.9.0"

  val protobuf = "com.google.protobuf" % "protobuf-java" % "3.1.0"

  val playJson = "com.typesafe.play" %% "play-json" % "2.5.8"

  val json2avro = "tech.allegro.schema.json2avro" % "converter" % "0.2.3"

  //val httpcore =  "org.apache.httpcomponents" % "httpcore" % "4.4.1"

  object curator {
    val version = "2.4.0"
    val framework = "org.apache.curator" % "curator-framework" % version
    val test = "org.apache.curator" % "curator-test" % version
  }

  object spark {
    val version = "2.0.0"
    val core = "org.apache.spark" %% "spark-core" % version
    val streaming = "org.apache.spark" %% "spark-streaming" % version
    val streamingKafka = "org.apache.spark" %% "spark-streaming-kafka-0-8" % version
    val testingBase = "com.holdenkarau" % "spark-testing-base_2.11" % "1.5.2_0.3.1"
  }

  object enumeratum {
    val version = "1.3.7"
    val enumeratum = "com.beachape" %% "enumeratum" % version
    val enumeratumPlay = "com.beachape" %% "enumeratum-play" % version
  }

  val kafka_client = "org.apache.kafka" % "kafka-clients" % "0.10.1.0"
}
