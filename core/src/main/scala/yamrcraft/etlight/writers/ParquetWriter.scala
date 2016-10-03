package yamrcraft.etlight.writers

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import parquet.avro.AvroParquetWriter

class ParquetWriter(tempFile: String, outputFile: String) extends Writer[GenericRecord] {

  // lazy initialization
  var writer: Option[AvroParquetWriter[GenericRecord]] = None

  val fs = FileSystem.get(new Configuration())

  val tempPath = new Path(tempFile + ".parquet")
  val outputPath = new Path(outputFile + ".parquet")

  override def write(event: GenericRecord): Unit = {
    if (writer.isEmpty) {
      writer = Some(createWriter(tempPath.toString, event.getSchema))
    }

    writer.get.write(event)
  }

  override def commit(): Unit = {
    writer.get.close()

    fs.mkdirs(outputPath.getParent)
    if (fs.exists(outputPath)) {
      fs.rename(outputPath, new Path(outputPath.getParent, s"__${outputPath.getName}.${System.currentTimeMillis()}.old.__"))
    }
    fs.rename(tempPath, outputPath)
  }

  private def createWriter(file: String, schema: Schema) = {
    val path = new Path(file)
    if (fs.exists(path)) {
      fs.delete(path, true)
    }
    new AvroParquetWriter[GenericRecord](path, schema)
  }

}
