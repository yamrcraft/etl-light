package yamrcraft.etlite.transformers

import com.google.protobuf
import yamrcraft.etlite.EtlException

abstract class ProtoTransformer extends Transformer[Message[protobuf.Message]] {

  @throws(classOf[EtlException])
  override def transform(message: InboundMessage): Message[protobuf.Message]

  override def toString(event: Array[Byte]): String

}
