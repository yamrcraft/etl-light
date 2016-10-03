package yamrcraft.etlight.writers

import java.io.IOException

import com.typesafe.config.Config
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.joda.time.format.DateTimeFormat
import org.slf4j.LoggerFactory
import yamrcraft.etlight.EtlException
import yamrcraft.etlight.transformers.Message
import yamrcraft.etlight.utils.ConfigConversions._

import scala.collection.mutable

/**
  * Lazy events writer - file writer is created on the first call to write.
  */
class TimePartitioningWriter[T](config: Config, jobId: Long, partitionId: Int,
  writerFactory: (String, String) => Writer[T]) extends Writer[Message[T]] {

  val logger = LoggerFactory.getLogger(this.getClass)

  // config settings
  val workingFolder: String = config.getString("working-folder")
  val outputFolder: String = config.getString("output-folder")
  val partitionPattern: String = config.getString("partition.pattern")
  val folderMapping: Map[String, String] = config.getConfig("record-name-to-folder-mapping").asMap

  val fs = FileSystem.get(new Configuration())

  val partitionFormat = DateTimeFormat.forPattern(partitionPattern)

  val partitionsWriters = mutable.Map[String, Writer[T]]()

  @throws(classOf[EtlException])
  @throws(classOf[IOException])
  override def write(event: Message[T]): Unit = {
    val timestamp = event.msgTimestamp
    val baseFolder = folderMapping.getOrElse(event.msgType, event.msgType)

    val writer = writerFor(baseFolder, timestamp)
    writer.write(event.msg)
  }

  override def commit() = {
    // close all writers
    partitionsWriters foreach { case (file, writer) =>
      writer.commit()
    }
  }

  @throws(classOf[EtlException])
  private def writerFor(baseFolder: String, timestamp: Long): Writer[T] = {
    val relativeFileName = new Path(s"$baseFolder/${partitionFormat.print(timestamp)}/events_${baseFolder}_job${jobId}_part$partitionId")
    val tempFile = new Path(workingFolder, relativeFileName)
    val outputFile = new Path(outputFolder, relativeFileName)

    partitionsWriters.getOrElseUpdate(tempFile.toString, writerFactory(tempFile.toString, outputFile.toString))
  }

}
