package yamrcraft.etlite.processors

import java.io.IOException

import org.slf4j.LoggerFactory
import yamrcraft.etlite.transformers.InboundMessage
import yamrcraft.etlite.writers.{ErrorInfo, ErrorEventWriter}
import yamrcraft.etlite.{ErrorType, EtlException, EtlSettings}

import scala.util.Try

class PartitionProcessor(jobId: Long, partitionId: Int, settings: EtlSettings) {

  val logger = LoggerFactory.getLogger(this.getClass)

  val pipeline = settings.pipeline.createFactory.createPipeline(settings.pipeline, jobId, partitionId)

  val errorsWriter: ErrorEventWriter = new ErrorEventWriter(settings.errorsFolder, jobId, partitionId)

  def processPartition(partition: Iterator[InboundMessage]): Unit = {
    logger.info(s"partition processing started [jobId=$jobId, partitionId=$partitionId]")

    partition foreach { inbound =>

      try {
        pipeline.processMessage(inbound)

      } catch {
        case e@(_: Exception) =>
          logger.error("event processing error", e)
          val errorType = e match {
            case ex: EtlException => ex.errorType.toString
            case _ : IOException => ErrorType.WriteError.toString
            case _ => ErrorType.SystemError.toString
          }
          val cause = Try(e.getCause.getMessage).getOrElse("")
          val errorInfo = ErrorInfo(errorType, Some(cause))
          errorsWriter.write((inbound.msg, errorInfo))
      }
    }

    pipeline.writer.commit()
    errorsWriter.commit()

    logger.info(s"partition processing ended [jobId=$jobId, partitionId=$partitionId]")
  }

}
