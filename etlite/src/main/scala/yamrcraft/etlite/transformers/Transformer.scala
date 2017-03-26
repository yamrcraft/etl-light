package yamrcraft.etlite.transformers

import yamrcraft.etlite.EtlException

/**
  * Implementation must have a constructor with a single parameter of type com.typesafe.config.Config
  *
  * e.g. class MyTranslator(config: Config) extends Transformer { ... }
  */
trait Transformer[T] {

  @throws(classOf[EtlException])
  def transform(inbound: InboundMessage): T

}
