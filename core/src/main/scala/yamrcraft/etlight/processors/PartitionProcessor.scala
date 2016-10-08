package yamrcraft.etlight.processors

import java.io.IOException

import org.slf4j.LoggerFactory
import yamrcraft.etlight.writers.{ErrorEvent, ErrorEventWriter}
import yamrcraft.etlight.{ErrorType, EtlException, EtlSettings}

import scala.util.Try

class PartitionProcessor(jobId: Long, partitionId: Int, settings: EtlSettings) {

  val logger = LoggerFactory.getLogger(this.getClass)

  val pipeline = settings.pipeline.createFactory.createPipeline(settings.pipeline, jobId, partitionId)

  val errorsWriter: ErrorEventWriter = new ErrorEventWriter(settings.errorsFolder, jobId, partitionId)

  def processPartition(partition: Iterator[(Array[Byte], Array[Byte])]): Unit = {
    logger.info(s"partition processing started [jobId=$jobId, partitionId=$partitionId]")

    partition foreach { event =>
      val (key, value) = (event._1, event._2)

      try {
        pipeline.processMessage(key, value)

      } catch {
        case e@(_: EtlException) =>
          logger.error("event processing error", e)
          val errorType = e match {
            case ex: EtlException => ex.errorType.toString
//            case ex: IOException => ErrorType.WriteError.toString
//            case _ => ErrorType.SystemError.toString
          }
          val cause = Try(e.getCause.getMessage).getOrElse("")
          val errorEvent = ErrorEvent(System.currentTimeMillis(), errorType, Some(cause), pipeline.transformer.toString(value))
          errorsWriter.write(errorEvent)
      }
    }

    pipeline.writer.commit()
    errorsWriter.commit()

    logger.info(s"partition processing ended [jobId=$jobId, partitionId=$partitionId]")
  }

}
