package yamrcraft.etlite.pipeline

import org.apache.avro.generic.GenericRecord
import yamrcraft.etlite.PipelineSettings
import yamrcraft.etlite.transformers.{JsonToAvroTransformer, Message}
import yamrcraft.etlite.writers.{AvroToParquetWriter, TimePartitioningWriter}

class JsonToParquetPipelineFactory extends PipelineFactory[Message[GenericRecord]] {

  def createPipeline(settings: PipelineSettings, jobId: Long, partitionId: Int): Pipeline[Message[GenericRecord]] =
    new Pipeline(
      new JsonToAvroTransformer(settings.transformerConfig),
      new TimePartitioningWriter(
        settings.writerConfig,
        jobId,
        partitionId,
        (tempFile, outputFile) => new AvroToParquetWriter(tempFile, outputFile))
    )

}


