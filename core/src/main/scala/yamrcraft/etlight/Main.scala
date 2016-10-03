package yamrcraft.etlight

import java.io.{File, FileInputStream}
import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.slf4j.LoggerFactory
import yamrcraft.etlight.processors.EtlProcessor
import yamrcraft.etlight.utils.{FakeLock, DLock}

import scala.io.Source

object Main {

  val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {

    if (args.length != 1) {
      println(
        s"""
           |Usage: Main <config>
           |  <config> path to the application configuration file, use 'file://' prefix in case file located on local storage.
         """.stripMargin)
      System.exit(1)
    }

    val configPath = args(0)
    val settings = new Settings(readConfigContent(configPath))

    val lock = {
      if (settings.etl.lock.enabled)
        new DLock(settings.etl.lock.zookeeperConnect, settings.etl.lock.path, settings.etl.lock.waitForLockSeconds)
      else
        new FakeLock
    }

    if (lock.tryLock()) {
      EtlProcessor.run(settings)
      lock.release()
    } else {
      logger.error("can't acquire zookeeper lock!")
    }
  }

  /**
    * read configuration content (application.conf) either from:
    * - local disk if path is 'file:///local-path'
    * - or from hadoop storage otherwise
    *
    * @param configPath configuration file path
    * @return content of configuration file
    */
  private def readConfigContent(configPath: String) = {
    val configURI = new URI(configPath)
    logger.info(s"Configuration file = '$configPath', scheme='${configURI.getScheme}'")

    val fs = FileSystem.get(new Configuration())

    configURI.getScheme match {
      case "file" => Source.fromInputStream(new FileInputStream(new File(configURI))).mkString
      case _ => Source.fromInputStream(fs.open(new Path(configPath))).mkString
    }
  }

}
