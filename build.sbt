import Dependencies._

//resolvers += "Maven Central" at "https://repo1.maven.org/maven2/"

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

lazy val etlight = (project in file("."))
  .aggregate(core, demoapp)

lazy val core = (project in file("core"))
  .configs(IntegrationTest)
  .settings(commonSettings, integrationTestSettings)
  .settings(
    compileDependencies(
      typesafeConfig,
      parquet,
      jodaTime,
      jodaConvert,
      playJson,
      json2avro,
      curator.framework,
      enumeratum.enumeratum,
      enumeratum.enumeratumPlay,
      httpcore,
      spark.streamingKafka.exclude("org.spark-project.spark", "unused")),
    providedDependencies(spark.core, spark.streaming),
    testDependencies(kafka_client),
    assemblyMergeStrategy in assembly := {
      case "application.conf"  => MergeStrategy.discard
      case x =>
        val oldStrategy = (assemblyMergeStrategy in assembly).value
        oldStrategy(x)
    }
  )

lazy val demoapp = (project in file("demoapp"))
  .dependsOn(`core`)
  .settings(commonSettings: _*)
  .settings(
    compileDependencies(
      playJson,
      json2avro
    ),
    assemblyMergeStrategy in assembly := {
      case "application.conf"  => MergeStrategy.discard
      case x =>
        val oldStrategy = (assemblyMergeStrategy in assembly).value
        oldStrategy(x)
    }
  )
