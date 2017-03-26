package yamrcraft.etlite.writers

import java.io.OutputStream

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{BytesWritable, IntWritable, SequenceFile, Text}
import org.json4s.jackson.Serialization
import org.json4s.{DefaultFormats, Formats, ShortTypeHints}
import yamrcraft.etlite.utils.FileUtils


case class ErrorInfo(
  errorType: String,
  errorMsg: Option[String]
)

/**
  * Lazy error events writer - file writer is created on the first call to write.
  */
class ErrorEventWriter(folder: String, jobId: Long, partitionId: Int)
  extends ErrorEventsWriter {

  // incremental record id
  var recordId = 1

  val fs = FileUtils.getFS(folder)

  val seqPath = new Path(folder, s"errors_job${jobId}_part$partitionId.seq")
  if (fs.exists(seqPath)) {
    fs.delete(seqPath, false)
  }

  val metaPath = new Path(folder, s"errors_job${jobId}_part$partitionId.meta.seq")
  if (fs.exists(metaPath)) {
    fs.delete(metaPath, false)
  }

  private var seqWriter: Option[SequenceFile.Writer] = None
  private var metaWriter: Option[SequenceFile.Writer] = None

  implicit val formats =
    new Formats {
      val dateFormat = DefaultFormats.lossless.dateFormat
      override val typeHints = ShortTypeHints(List(classOf[ErrorInfo]))
      override val typeHintFieldName = "type"
    }

  override def write(errorEvent: (Array[Byte], ErrorInfo)) = {

    if (seqWriter.isEmpty) {
      seqWriter = createSequenceFile(seqPath, classOf[IntWritable], classOf[BytesWritable])
      metaWriter = createSequenceFile(metaPath, classOf[IntWritable], classOf[Text])
    }

    val id = new IntWritable(recordId)
    seqWriter.get.append(id, new BytesWritable(errorEvent._1))
    metaWriter.get.append(id, new Text(Serialization.write(errorEvent._2)))

    recordId += 1
  }

  override def commit() = {
    seqWriter.foreach(p => p.close())
    metaWriter.foreach(p => p.close())
  }

  private def createSequenceFile(path: Path, keyClass: Class[_], valueClass: Class[_]) = {
    val optPath = SequenceFile.Writer.file(path)
    val optKey = SequenceFile.Writer.keyClass(keyClass)
    val optVal = SequenceFile.Writer.valueClass(valueClass)
    Some(SequenceFile.createWriter(fs.getConf, optPath, optKey, optVal))
  }
}
