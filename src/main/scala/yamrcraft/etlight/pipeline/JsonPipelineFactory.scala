package yamrcraft.etlight.pipeline

import yamrcraft.etlight.PipelineSettings
import yamrcraft.etlight.transformers.{JsonTransformer, Message}
import yamrcraft.etlight.writers.{StringWriter, TimePartitioningWriter}

class JsonPipelineFactory extends PipelineFactory[Message[String]] {

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


