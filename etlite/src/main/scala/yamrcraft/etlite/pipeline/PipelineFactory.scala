package yamrcraft.etlite.pipeline

import yamrcraft.etlite.PipelineSettings

trait PipelineFactory[T] {

  def createPipeline(settings: PipelineSettings, jobId: Long, partitionId: Int): Pipeline[T]

}
