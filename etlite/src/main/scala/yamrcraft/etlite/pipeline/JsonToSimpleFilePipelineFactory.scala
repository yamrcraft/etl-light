package yamrcraft.etlite.pipeline

import yamrcraft.etlite.PipelineSettings
import yamrcraft.etlite.transformers.{JsonTransformer, Message}
import yamrcraft.etlite.writers.{StringWriter, TimePartitioningWriter}

class JsonToSimpleFilePipelineFactory extends PipelineFactory[Message[String]] {

  def createPipeline(settings: PipelineSettings, jobId: Long, partitionId: Int): Pipeline[Message[String]] =
    new Pipeline(
      new JsonTransformer(settings.transformerConfig),
      new TimePartitioningWriter(
        settings.writerConfig,
        jobId,
        partitionId,
        (tempFile, outputFile) => new StringWriter(tempFile, outputFile))
    )

}


