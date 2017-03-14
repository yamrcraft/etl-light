import Dependencies._

lazy val commonSettings = Seq(
  organization := "yamrcraft",
  version := "0.1.0",
  scalaVersion := "2.11.8"
)

lazy val integrationTestSettings: Seq[Setting[_]] = Defaults.itSettings ++ Seq(
  fork in IntegrationTest := true,
  parallelExecution in IntegrationTest := false,
  testOptions in IntegrationTest += Tests.Argument(TestFrameworks.ScalaTest, "-oDF")
)

lazy val etlite = (project in file("etlite"))
  .dependsOn(`proto-messages` % "it->compile")
  .configs(IntegrationTest)
  .settings(commonSettings, integrationTestSettings)
  .settings(
    compileDependencies(
      typesafeConfig,
      protobuf,
      jodaTime,
      jodaConvert,
      playJson,
      json2avro,
      curator.framework,
      enumeratum.enumeratum,
      enumeratum.enumeratumPlay,
      spark.streamingKafka.exclude("org.spark-project.spark", "unused"),
      parquetAvro,
      parquetProto
    ),
    providedDependencies(
      spark.core,
      spark.streaming
    ),
    testDependencies(
      kafka_client
    ),
    assemblyMergeStrategy in assembly := {
      case "application.conf" => MergeStrategy.discard
      case x =>
        val oldStrategy = (assemblyMergeStrategy in assembly).value
        oldStrategy(x)
    }
  )

lazy val `proto-example` = (project in file("examples/protobuf"))
  .dependsOn(`etlite`, `proto-messages`)
  .settings(commonSettings: _*)
  .settings(
    compileDependencies(
      protobuf
    ),
    mainClass in assembly := Some("yamrcraft.etlite.Main"),
    assemblyShadeRules in assembly := Seq(
      ShadeRule.rename("com.google.protobuf.*" -> "shadeproto.@1").inAll
    )
  )

lazy val `proto-messages` = (project in file("examples/protobuf-messages"))
  .settings(commonSettings: _*)
  .settings(
    compileDependencies(
      protobuf
    )
  )
