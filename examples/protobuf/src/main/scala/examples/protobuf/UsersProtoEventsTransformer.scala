package examples.protobuf

import com.google.protobuf
import com.typesafe.config.Config
import yamrcraft.etlite.EtlException
import yamrcraft.etlite.transformers.{InboundMessage, Message, ProtoTransformer}

class UsersProtoEventsTransformer(config: Config) extends ProtoTransformer {

  @throws(classOf[EtlException])
  override def transform(message: InboundMessage): Message[protobuf.Message] = {
    val userProto = UserOuterClass.User.parseFrom(message.msg)
    Message(
      msg = userProto,
      msgType = userProto.getDescriptorForType.getName,
      msgTimestamp = userProto.getTime.getSeconds * 1000
    )
  }

  override def toString(event: Array[Byte]): String = {
    "None"
  }
}
