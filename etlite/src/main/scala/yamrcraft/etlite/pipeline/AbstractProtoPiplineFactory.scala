package yamrcraft.etlite.pipeline

import com.google.protobuf
import com.typesafe.config.Config
import yamrcraft.etlite.PipelineSettings
import yamrcraft.etlite.transformers.{Message, ProtoTransformer, Transformer}
import yamrcraft.etlite.writers.{ProtoToParquetWriter, TimePartitioningWriter}

abstract class AbstractProtoPipelineFactory extends PipelineFactory[Message[protobuf.Message]] {

  def transformer(config: Config): ProtoTransformer

  final def createPipeline(settings: PipelineSettings, jobId: Long, partitionId: Int): Pipeline[Message[protobuf.Message]] =
    new Pipeline(
      transformer(settings.transformerConfig),
      new TimePartitioningWriter(
        settings.writerConfig,
        jobId,
        partitionId,
        (tempFile, outputFile) => new ProtoToParquetWriter(tempFile, outputFile)
      )
    )

}
