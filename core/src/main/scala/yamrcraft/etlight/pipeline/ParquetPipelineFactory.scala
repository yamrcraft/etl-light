package yamrcraft.etlight.pipeline

import org.apache.avro.generic.GenericRecord
import yamrcraft.etlight.PipelineSettings
import yamrcraft.etlight.transformers.{AvroTransformer, Message}
import yamrcraft.etlight.writers.{ParquetWriter, TimePartitioningWriter}

class ParquetPipelineFactory extends PipelineFactory[Message[GenericRecord]] {

  def createPipeline(settings: PipelineSettings, jobId: Long, partitionId: Int): Pipeline[Message[GenericRecord]] =
    new Pipeline(
      new AvroTransformer(settings.transformerConfig),
      new TimePartitioningWriter(
        settings.writerConfig,
        jobId,
        partitionId,
        (tempFile, outputFile) => new ParquetWriter(tempFile, outputFile))
    )

}


