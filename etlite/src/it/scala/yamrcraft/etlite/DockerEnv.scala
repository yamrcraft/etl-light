package yamrcraft.etlite

import scala.sys.process._

object DockerEnv {

  def dockerComposeUp: Int = "src/it/docker-compose.sh".!

  def dockerComposeDown: Int = "docker-compose -f src/it/docker-compose.yml down".!

  def runSparkJob(jarFileName: String, confFileName: String, extraLibraryPath: Option[String] = None): Int = {
    val properties = extraLibraryPath.map(path => s"--jars=$path").getOrElse("")
    s"docker exec it_spark_1 /usr/spark-2.1.0/bin/spark-submit $properties /usr/etl-light/$jarFileName /usr/etl-light/resources/$confFileName".!
  }

  def readFileFromDocker(stateFile: String): String =
    s"docker exec it_spark_1 cat $stateFile".!!

}
