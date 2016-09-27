package yamrcraft.etlight.processors

import java.io.IOException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.slf4j.LoggerFactory
import yamrcraft.etlight.writers.{AvroEventsWriter, ErrorEvent, ErrorEventWriter}
import yamrcraft.etlight.{ErrorType, EtlException, EtlSettings}

class PartitionProcessor(jobId: Long, partitionId: Int, settings: EtlSettings) {

  val logger = LoggerFactory.getLogger(this.getClass)

  val fs = FileSystem.get(new Configuration())

  val transformer = settings.createTransformer

  val writer: AvroEventsWriter = settings.createWriter(jobId, partitionId)

  val errorsWriter: ErrorEventWriter = new ErrorEventWriter(settings.errorsFolder, jobId, partitionId, fs)

  def processPartition(partition: Iterator[(String, Array[Byte])]): Unit = {
    logger.info(s"partition processing started [jobId=$jobId, partitionId=$partitionId]")

    partition foreach { event =>
      val (key, value) = (event._1, event._2)
      try {
        val transformedEvent = transformer.transform(key, value)
        writer.write(transformedEvent)

      } catch {
        case e@(_: EtlException | _: IOException) =>
          logger.error("event processing error", e)
          val errorType = e match {
            case ex: EtlException => ex.errorType.toString
            case _ => ErrorType.WriteError.toString
          }
          val errorEvent = ErrorEvent(System.currentTimeMillis(), errorType, Some(e.getMessage), transformer.toString(value))
          errorsWriter.write(errorEvent)
      }
    }

    writer.commit()
    errorsWriter.commit()

    logger.info(s"partition processing ended [jobId=$jobId, partitionId=$partitionId]")
  }

}

object PartitionProcessor {
  def apply(jobId: Long, partitionId: Int, settings: EtlSettings): PartitionProcessor =
    new PartitionProcessor(jobId, partitionId, settings)
}
