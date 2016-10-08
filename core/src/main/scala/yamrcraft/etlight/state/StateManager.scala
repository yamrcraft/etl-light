package yamrcraft.etlight.state

import org.apache.hadoop.fs.{FileSystem, Path}
import org.slf4j.LoggerFactory
import yamrcraft.etlight.Settings
import yamrcraft.etlight.utils.{FileUtils, HdfsUtils}

import scala.io.Source

trait State {
  val jobId: Long
}

trait StateManager[T <: State] {

  def readState: Option[T]

  def fetchNextState(lastState: Option[T], settings: Settings): T

  def commitState(state: T)

}

private[state] object StatePersistency {

  val PREFIX = "state-"
  val REGEX = (PREFIX + """([\d]+)([\w\.]*)""").r

  def stateFile(stateFolder: String, stateId: Long) = new Path(stateFolder, PREFIX + stateId)

  def stateBackupFile(stateFolder: String, stateId: Long) = new Path(stateFolder, PREFIX + stateId + ".bak")


  /* list state files sorted by state id, oldest first  */
  def listStateFiles(stateFolder: String): Seq[Path] = {
    def sortFunc(path1: Path, path2: Path): Boolean = {
      val (time1, bk1) = path1.getName match {
        case REGEX(x, y) => (x.toLong, !y.isEmpty)
      }
      val (time2, bk2) = path2.getName match {
        case REGEX(x, y) => (x.toLong, !y.isEmpty)
      }
      (time1 < time2) || (time1 == time2 && bk1)
    }

    val fs = FileUtils.getFS(stateFolder)
    val statePath = new Path(stateFolder)
    if (fs.exists(statePath)) {
      val allFiles = HdfsUtils.listFiles(statePath, fs)
      val filtered = allFiles.filter(p => REGEX.findFirstIn(p.toString).nonEmpty)
      filtered.sortWith(sortFunc)
    } else {
      Seq.empty
    }
  }
}

private[state] class StateReader(stateFolder: String) {
  val logger = LoggerFactory.getLogger(this.getClass)

  import StatePersistency._

  def readLastState: Option[String] = {
    val oldestFirstStateFiles = listStateFiles(stateFolder)
    logger.debug(s"state files: $oldestFirstStateFiles")
    if (oldestFirstStateFiles.isEmpty) {
      None
    } else {
      val file = oldestFirstStateFiles.last
      logger.info(s"state file being read: $file")
      val fs = FileUtils.getFS(stateFolder)
      val fis = fs.open(file)
      val content = Source.fromInputStream(fis).mkString
      Some(content)
    }
  }


}

private[state] class StateWriter(stateFolder: String, stateFilesToKeep: Int) {

  val logger = LoggerFactory.getLogger(this.getClass)
  val MAX_ATTEMPTS = 3

  def write(state: Array[Byte], stateId: Long) = {
    val tempFile = new Path(stateFolder, "temp")
    val stateFile = StatePersistency.stateFile(stateFolder, stateId)
    val backupFile = StatePersistency.stateBackupFile(stateFolder, stateId)

    val fs = FileUtils.getFS(stateFolder)

    // write to temp file and rename to avoid state file corruption in case of a crash

    // write to temp file
    if (fs.exists(tempFile)) fs.delete(tempFile, true)
    val tempOS = fs.create(tempFile)
    tempOS.write(state)
    tempOS.close()
    logger.info(s"state was written to: $tempFile")

    // backup the state file if already exists
    if (fs.exists(stateFile)) {
      if (fs.exists(backupFile)) fs.delete(backupFile, true)
      if (!fs.rename(stateFile, backupFile)) {
        logger.warn(s"Could not rename $stateFile to $backupFile")
      }
    }

    // rename temp file to state file
    if (!fs.rename(tempFile, stateFile)) {
      logger.warn(s"Could not rename $tempFile to $stateFile")
    }
    logger.info(s"state was renamed from $tempFile to $stateFile")

    // delete old state files
    val oldestFirstStateFiles = StatePersistency.listStateFiles(stateFolder)
    if (oldestFirstStateFiles.size > stateFilesToKeep) {
      oldestFirstStateFiles.take(oldestFirstStateFiles.size - stateFilesToKeep).foreach { file =>
        logger.info(s"deleting old state file: $file")
        fs.delete(file, true)
      }
    }
  }
}
