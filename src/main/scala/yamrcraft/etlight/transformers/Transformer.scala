package yamrcraft.etlight.transformers

import yamrcraft.etlight.EtlException

/**
  * Implementation must have a constructor with a single parameter of type com.typesafe.config.Config
  *
  * e.g. class MyTranslator(config: Config) extends Transformer { ... }
  */
trait Transformer[T] {

  @throws(classOf[EtlException])
  def transform(key: Array[Byte], msg: Array[Byte]): T

  def toString(event: Array[Byte]): String

}
