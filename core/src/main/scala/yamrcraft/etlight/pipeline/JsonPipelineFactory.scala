package yamrcraft.etlight.pipeline
import yamrcraft.etlight.PipelineSettings

class JsonPipelineFactory extends PipelineFactory[String] {
  override def createPipeline(settings: PipelineSettings, jobId: Long, partitionId: Int): Pipeline[String] = ???
}
