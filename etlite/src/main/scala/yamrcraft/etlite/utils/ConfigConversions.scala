package yamrcraft.etlite.utils

import java.util.Properties

import com.typesafe.config.Config

import scala.collection.JavaConversions._

object ConfigConversions {

  implicit class RichConfig(config: Config) {

    def asProperties: Properties = {
      val props = new Properties()
      for (entry <- config.entrySet) {
        props.put(entry.getKey, entry.getValue.unwrapped)
      }
      props
    }

    def asMap[T <: Any]: Map[String, String] = {
      config.entrySet.collect {
        case entry => entry.getKey -> entry.getValue.unwrapped().toString
      }.toMap
    }
  }

}

