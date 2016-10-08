package yamrcraft.etlight.writers

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.fs.Path
import org.slf4j.LoggerFactory
import parquet.avro.AvroParquetWriter
import yamrcraft.etlight.utils.FileUtils

class ParquetWriter(tempFile: String, outputFile: String) extends Writer[GenericRecord] {

  val logger = LoggerFactory.getLogger(this.getClass)

  // lazy initialization
  var writer: Option[AvroParquetWriter[GenericRecord]] = None

  val tempPath = new Path(tempFile + ".parquet")
  val outputPath = new Path(outputFile + ".parquet")
  logger.info(s"creating writer for working file: ${tempPath.toString}, outputFile: ${outputPath.toString}")

  override def write(event: GenericRecord): Unit = {
    logger.info(s"ParquetWriter.write, event type: ${event.getSchema.getName}")
    if (writer.isEmpty) {
      writer = Some(createWriter(tempPath.toString, event.getSchema))
    }

    writer.get.write(event)
  }

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

  private def createWriter(file: String, schema: Schema) = {
    val fs = FileUtils.getFS(file)
    val path = new Path(file)
    if (fs.exists(path)) {
      fs.delete(path, true)
    }
    fs.mkdirs(path.getParent)
    new AvroParquetWriter[GenericRecord](path, schema)
  }

}
