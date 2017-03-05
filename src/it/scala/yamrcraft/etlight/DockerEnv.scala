package yamrcraft.etlight

import scala.sys.process._

object DockerEnv {

  def dockerComposeUp: Int = "src/it/docker-compose.sh".!

  def dockerComposeDown: Int = "docker-compose -f src/it/docker-compose.yml down".!

  def runSparkJob(confFile: String): Int = {
    s"docker exec  it_spark_1 /usr/spark-2.1.0/bin/spark-submit /usr/etl-light/etlight-assembly-0.1.0.jar  /usr/etl-light/resources/$confFile".!
  }

  def readFileFromDocker(stateFile: String): String =
    s"docker exec it_spark_1 cat $stateFile".!!

}
