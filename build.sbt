import Dependencies._

lazy val commonSettings = Seq(
  organization := "yamrcrafts",
  version := "0.1.0",
  scalaVersion := "2.11.8"
)

lazy val etlight = (project in file(".")).
  aggregate(core, demoapp)

lazy val core = (project in file("core")).
  settings(commonSettings: _*).
  settings(
    compileDependencies(
      typesafeConfig,
      parquet,
      jodaTime,
      curator.framework,
      enumeratum.enumeratum,
      enumeratum.enumeratumPlay,
      spark.streamingKafka.exclude("org.spark-project.spark", "unused")),
    providedDependencies(spark.core, spark.streaming)
  )

lazy val demoapp = (project in file("demoapp")).
  settings(commonSettings: _*).
  settings(
    // other
  )

