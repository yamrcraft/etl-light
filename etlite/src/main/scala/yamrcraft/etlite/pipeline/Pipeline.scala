package yamrcraft.etlite.pipeline

import yamrcraft.etlite.transformers.{InboundMessage, Transformer}
import yamrcraft.etlite.writers.Writer

final class Pipeline[T](val transformer: Transformer[T], val writer: Writer[T]) {

  def processMessage(message: InboundMessage) = {
    val transformedEvent = transformer.transform(message)
    writer.write(transformedEvent)
  }

}
