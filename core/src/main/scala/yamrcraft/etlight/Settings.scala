package yamrcraft.etlight

import com.typesafe.config.{Config, ConfigFactory}
import yamrcraft.etlight.pipeline.PipelineFactory
import yamrcraft.etlight.utils.ConfigConversions._

import scala.collection.JavaConversions._

class Settings(configContent: String) extends Serializable {
  val config = ConfigFactory.parseString(configContent)
  val kafka = KafkaSettings(config.getConfig("kafka"))
  val spark = SparkSettings(config.getConfig("spark"))
  val etl = EtlSettings(config.getConfig("etl"))
}

case class KafkaSettings(
  topics: Set[String],
  properties: Map[String, String]
)

object KafkaSettings {
  def apply(config: Config) = {
    new KafkaSettings(
      topics = config.getStringList("topics").toSet,
      properties = config.getConfig("config").asMap
    )
  }
}

case class SparkSettings(
  appName: String,
  conf: Map[String, String]
)

object SparkSettings {
  def apply(config: Config) = {
    new SparkSettings(
      appName = config.getString("app-name"),
      conf = config.getConfig("config").asMap
    )
  }
}

case class EtlSettings(
  lock: LockSettings,
  state: StateSettings,
  errorsFolder: String,
  pipeline: PipelineSettings
)

object EtlSettings {
  def apply(config: Config) = {
    new EtlSettings(
      lock = LockSettings(
        config.getBoolean("lock.enabled"),
        config.getString("lock.zookeeper-connect"),
        config.getString("lock.path"),
        config.getInt("lock.wait-for-lock-seconds")
      ),
      state = StateSettings(
        config.getString("state.folder"),
        config.getInt("state.files-to-keep")
      ),
      errorsFolder = config.getString("errors-folder"),
      pipeline = PipelineSettings(
        config.getString("pipeline.factory-class"),
        config.getConfig("pipeline.transformer.config"),
        config.getConfig("pipeline.writer.config")
      )
    )
  }
}

case class LockSettings(
  enabled: Boolean,
  zookeeperConnect: String,
  path: String,
  waitForLockSeconds: Int
)

case class StateSettings(
  stateFolder: String,
  stateFilesToKeep: Int
)

case class PipelineSettings(
  factoryClass: String,
  transformerConfig: Config,
  writerConfig: Config
) {
  def createFactory: PipelineFactory[_] =
    Class.forName(factoryClass).newInstance().asInstanceOf[PipelineFactory[_]]
}
