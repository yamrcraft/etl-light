package yamrcraft.etlight

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.slf4j.LoggerFactory
import yamrcraft.etlight.processors.EtlProcessor
import yamrcraft.etlight.utils.{FakeLock, Lock}

import scala.io.Source

object Main {

  val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {

    if (args.length != 1) {
      println(
        s"""
           |Usage: Main <config>
           |  <config> is a path to application configuration file
         """.stripMargin)
      System.exit(1)
    }

    val fs = FileSystem.get(new Configuration())

    val configPath = args(0)
    val content = Source.fromInputStream(fs.open(new Path(configPath))).mkString
    val settings = new Settings(content)

    val lock = {
      if (settings.etl.lockEnabled)
        new Lock(settings.etl.lockZookeeperConnect, settings.etl.lockPath, settings.etl.waitForLockSeconds)
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

}
