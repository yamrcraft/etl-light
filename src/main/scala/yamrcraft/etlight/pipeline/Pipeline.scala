package yamrcraft.etlight.pipeline

import yamrcraft.etlight.transformers.Transformer
import yamrcraft.etlight.writers.Writer

final class Pipeline[T](val transformer: Transformer[T], val writer: Writer[T]) {

  def processMessage(key: Array[Byte], msg: Array[Byte]) = {
    val transformedEvent = transformer.transform(key, msg)
    writer.write(transformedEvent)
  }

}
