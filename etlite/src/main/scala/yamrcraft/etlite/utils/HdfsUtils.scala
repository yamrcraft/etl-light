package yamrcraft.etlite.utils

import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}

import scala.collection.mutable

object HdfsUtils {

  def renameFiles(fromBase: Path, toBase: Path, fs: FileSystem) = {
    if (fs.exists(fromBase)) {
      val filesToMove = listFiles(fromBase, fs)
      println("files to move:")
      filesToMove foreach (p => println("+++" + p.toString))
      filesToMove foreach { file =>
        val relPath = relativize(fromBase, file)
        val toPath = new Path(toBase, relPath)
        fs.mkdirs(toPath.getParent)
        fs.rename(file, toPath)
        println(" file renamed to: " + toPath.toString)
      }
    }
  }

  def relativize(base: Path, files: List[Path]) = {
    files map (file => new Path(base.toUri.relativize(file.toUri).getPath))
  }

  def relativize(base: Path, file: Path): Path = {
    new Path(base.toUri.relativize(file.toUri).getPath)
  }

  def listFiles(path: Path, fs: FileSystem): List[Path] = {
    val statusList = mutable.MutableList[FileStatus]()
    traverse(path, statusList, fs)
    statusList.map(status => new Path(status.getPath.toUri.getPath)).toList
  }

  private def traverse(path: Path, list: mutable.MutableList[FileStatus], fs: FileSystem): Unit = {
    fs.listStatus(path) foreach { status =>
      if (!status.isDirectory) {
        list += status
      } else {
        traverse(status.getPath, list, fs)
      }
    }
  }

}
