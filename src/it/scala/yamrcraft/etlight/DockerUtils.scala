package yamrcraft.etlight

import scala.sys.process._

object DockerUtils {

  def dockerComposeUp: Int = "docker-compose -f src/it/docker-compose.yml up -d".!

  def dockerComposeDown: Int = "docker-compose -f src/it/docker-compose.yml down".!


}
