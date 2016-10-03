package yamrcraft.etlight.writers

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}

class StringWriter(tempFile: String, outputFile: String) extends Writer[String] {

  // lazy initialization
  var writer: Option[FSDataOutputStream] = None

  val fs = FileSystem.get(new Configuration())

  val tempPath = new Path(tempFile + ".txt")
  val outputPath = new Path(outputFile + ".txt")

  override def write(event: String): Unit = {
    if (writer.isEmpty) {
      writer = Some(createWriter(tempPath.toString))
    }

    writer.get.writeUTF(event)
  }

  override def commit(): Unit = {
    writer.get.close()

    fs.mkdirs(outputPath.getParent)
    if (fs.exists(outputPath)) {
      fs.rename(outputPath, new Path(outputPath.getParent, s"__${outputPath.getName}.${System.currentTimeMillis()}.old.__"))
    }
    fs.rename(tempPath, outputPath)
  }

  private def createWriter(file: String) = {
    val path = new Path(file)
    if (fs.exists(path)) {
      fs.delete(path, true)
    }
    fs.create(path)
  }

}
