package examples.protobuf

import com.google.protobuf
import yamrcraft.etlite.PipelineSettings
import yamrcraft.etlite.pipeline.{Pipeline, PipelineFactory}
import yamrcraft.etlite.transformers.Message
import yamrcraft.etlite.writers.{ProtoToParquetWriter, TimePartitioningWriter}

class ProtoToParquetPipelineFactory extends PipelineFactory[Message[protobuf.Message]] {

  def createPipeline(settings: PipelineSettings, jobId: Long, partitionId: Int): Pipeline[Message[protobuf.Message]] =
    new Pipeline(
      new UsersProtoEventsTransformer(settings.transformerConfig),
      new TimePartitioningWriter(
        settings.writerConfig,
        jobId,
        partitionId,
        (tempFile, outputFile) => new ProtoToParquetWriter(tempFile, outputFile))
    )

}
