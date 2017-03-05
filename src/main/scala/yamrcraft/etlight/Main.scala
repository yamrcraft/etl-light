package yamrcraft.etlight

import org.slf4j.LoggerFactory
import yamrcraft.etlight.processors.EtlProcessor
import yamrcraft.etlight.utils.{DLock, FakeLock, FileUtils}

object Main {

  val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {

    if (args.length != 1) {
      println(
        s"""
           |Usage: Main <config>
           |  <config> path to the application configuration file, use 'file:///' prefix in case file located on local file system.
         """.stripMargin)
      System.exit(1)
    }

    val configPath = args(0)
    logger.info(s"Configuration file = '$configPath'")
    val settings = new Settings(FileUtils.readContent(configPath))

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

}
