package yamrcraft.etlight.pipeline

import org.apache.avro.generic.GenericRecord
import yamrcraft.etlight.PipelineSettings
import yamrcraft.etlight.transformers.{Message, JsonTransformer}
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


