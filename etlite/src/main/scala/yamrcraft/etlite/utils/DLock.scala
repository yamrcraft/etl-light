package yamrcraft.etlite.utils

import java.util.concurrent.TimeUnit

import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry
import org.slf4j.LoggerFactory

/**
  * Inter process distributed lock using zookeeper.
  */
class DLock(zkConnect: String, lockFile: String, waitForLockSeconds: Int) {

  val logger = LoggerFactory.getLogger(this.getClass)

  private var zkClient: Option[CuratorFramework] = None
  private var lock: Option[InterProcessSemaphoreMutex] = None

  def tryLock(): Boolean = {
    require(lock.isEmpty, "lock can't be reused")
    logger.info("acquiring lock...")
    zkClient = Some(CuratorFrameworkFactory.newClient(zkConnect, new ExponentialBackoffRetry(1000, 3)))
    zkClient.get.start()
    lock = Some(new InterProcessSemaphoreMutex(zkClient.get, lockFile))
    lock.get.acquire(waitForLockSeconds, TimeUnit.SECONDS)
  }

  def release() = {
    require(lock.nonEmpty, "lock wasn't acquired")
    logger.info("releasing lock")
    lock.foreach(_.release())
    zkClient.foreach(_.close())
  }

}

class FakeLock extends DLock("", "", 0) {
  override def tryLock() = true

  override def release() = {}
}
