package yamrcraft.etlight

import com.typesafe.config.{Config, ConfigFactory}
import yamrcraft.etlight.utils.ConfigConversions._
import yamrcraft.etlight.writers.AvroEventsWriter

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
			properties = config.getConfig("conf").asMap
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
			conf = config.getConfig("conf").asMap
		)
	}
}

case class EtlSettings(
	lockEnabled: Boolean,
	lockZookeeperConnect: String,
	lockPath: String,
	waitForLockSeconds: Int,
	stateFilesToKeep: Int,
	stateFolder: String,
	errorsFolder: String,
	transformerClass: String,
	transformerConfig: Config,
	writerClass: String,
	writerConfig: Config
) {

	def createTransformer: Transformer =
		Class.forName(transformerClass)
			.getConstructor(classOf[Config])
			.newInstance(transformerConfig)
			.asInstanceOf[Transformer]

	def createWriter(jobId: Long, partitionId: Int): AvroEventsWriter =
		Class.forName(writerClass)
			.getConstructor(classOf[Config], classOf[Long], classOf[Int])
			.newInstance(writerConfig)
			.asInstanceOf[AvroEventsWriter]
}

object EtlSettings {
	def apply(config: Config) = {
		new EtlSettings(
			lockEnabled = config.getBoolean("lock.enabled"),
			lockZookeeperConnect = config.getString("lock.zookeeper-connect"),
			lockPath = config.getString("lock.path"),
			waitForLockSeconds = config.getInt("lock.wait-for-lock-seconds"),
			stateFolder = config.getString("state.folder"),
			stateFilesToKeep = config.getInt("state.files-to-keep"),
			errorsFolder = config.getString("errors-folder"),
			transformerClass = config.getString("transformer.class"),
			transformerConfig = config.getConfig("transformer.config"),
			writerClass = config.getString("writer.class"),
			writerConfig = config.getConfig("writer.config")
		)
	}
}

