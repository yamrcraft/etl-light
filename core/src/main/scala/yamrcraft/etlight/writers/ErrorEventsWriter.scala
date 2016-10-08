package yamrcraft.etlight.writers

import java.io.OutputStream

import org.apache.hadoop.fs.Path
import org.json4s.jackson.Serialization
import org.json4s.{DefaultFormats, Formats, ShortTypeHints}
import yamrcraft.etlight.utils.FileUtils


case class ErrorEvent(
  timestamp: Long = System.currentTimeMillis(),
  errorType: String,
  errorMsg: Option[String],
  event: String
)

/**
  * Lazy error events writer - file writer is created on the first call to write.
  */
class ErrorEventWriter(folder: String, jobId: Long, partitionId: Int)
  extends ErrorEventsWriter {

  val fs = FileUtils.getFS(folder)

  private var writer: Option[OutputStream] = None

  implicit val formats =
    new Formats {
      val dateFormat = DefaultFormats.lossless.dateFormat
      override val typeHints = ShortTypeHints(List(classOf[ErrorEvent]))
      override val typeHintFieldName = "type"
    }

  override def write(errorEvent: ErrorEvent) = {

    if (writer.isEmpty) {
      val path = new Path(folder, s"errors_job${jobId}_part$partitionId")
      writer = Some(fs.create(path, true))
    }

    val eventSer = Serialization.write(errorEvent)
    writer.get.write(eventSer.getBytes)
  }

  override def commit() = {
    writer.foreach(p => p.close())
  }
}
