package yamrcraft.etlight.pipeline

import yamrcraft.etlight.PipelineSettings

trait PipelineFactory[T] {

  def createPipeline(settings: PipelineSettings, jobId: Long, partitionId: Int): Pipeline[T]

}
