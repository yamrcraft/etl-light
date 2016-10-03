package yamrcraft.etlight.transformers

case class Message[T](
  msg: T,
  msgType: String,
  msgTimestamp: Long
)
