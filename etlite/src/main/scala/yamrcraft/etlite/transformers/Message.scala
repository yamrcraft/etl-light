package yamrcraft.etlite.transformers

case class Message[T](
  msg: T,
  msgType: String,
  msgTimestamp: Long
)

case class InboundMessage(
  topic: String,
  key: Array[Byte],
  msg: Array[Byte]
)
