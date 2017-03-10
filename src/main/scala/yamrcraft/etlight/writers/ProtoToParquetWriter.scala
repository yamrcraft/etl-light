package yamrcraft.etlight.writers

import com.google.protobuf
import org.apache.hadoop.fs.Path
import org.slf4j.LoggerFactory
import yamrcraft.etlight.utils.FileUtils
import org.apache.parquet.proto.ProtoParquetWriter

/**
  * Generic protobuf to parquet writer, accepts a protobuf.Message type and writes it to Parquet file.
  *
  * This class works on any protobuf concrete type extending protobuf.Message interface.
  *
  * @param tempFile a temporary file to write events into
  * @param outputFile a final 'commit' output file
  */
class ProtoToParquetWriter(tempFile: String, outputFile: String) extends Writer[protobuf.Message] {

  val logger = LoggerFactory.getLogger(this.getClass)

  // lazy initialization
  var writer: Option[ProtoParquetWriter[protobuf.Message]] = None

  val tempPath = new Path(tempFile + ".parquet")
  val outputPath = new Path(outputFile + ".parquet")
  logger.info(s"creating writer for working file: ${tempPath.toString}, outputFile: ${outputPath.toString}")

  /**
    * writes the given protobuf message to tmp file in parquet format.
    *
    * @param event protobuf message to write.
    */
  override def write(event: protobuf.Message): Unit = {
    logger.debug(s"writing event of type: ${event.getDescriptorForType.getName}")
    if (writer.isEmpty) {
      writer = Some(createWriter(tempPath.toString, event))
    }

    writer.get.write(event)
  }

  /**
    * Closes the tmp file and copies it into final output file.
    */
  override def commit(): Unit = {
    writer.get.close()

    val fs = FileUtils.getFS(outputPath.toString)
    fs.mkdirs(outputPath.getParent)
    if (fs.exists(outputPath)) {
      fs.rename(outputPath, new Path(outputPath.getParent, s"__${outputPath.getName}.${System.currentTimeMillis()}.old.__"))
    }
    // copy temp file to output file (typically temp file would be on local file system).
    if (tempFile.startsWith("file")) {
      logger.info(s"copy file from: ${tempPath.toString} to $outputPath")
      fs.copyFromLocalFile(true, true, tempPath, outputPath)
    } else {
      logger.info(s"renaming file from: ${tempPath.toString} to $outputPath")
      fs.rename(tempPath, outputPath)
    }
  }

  private def createWriter(file: String, msg: protobuf.Message): ProtoParquetWriter[protobuf.Message] = {
    val fs = FileUtils.getFS(file)
    val path = new Path(file)
    if (fs.exists(path)) {
      fs.delete(path, true)
    }
    fs.mkdirs(path.getParent)
    new ProtoParquetWriter[protobuf.Message](path, msg.getClass)
  }

}
