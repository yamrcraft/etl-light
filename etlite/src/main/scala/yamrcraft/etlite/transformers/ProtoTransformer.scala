package yamrcraft.etlite.transformers

import com.google.protobuf
import com.google.protobuf.{MessageOrBuilder, Timestamp}
import com.typesafe.config.Config
import org.slf4j.LoggerFactory
import yamrcraft.etlite.{ErrorType, EtlException}
import yamrcraft.etlite.utils.ConfigConversions._

/**
  * Generic protobuf events transformations.
  *
  * requires the following configuration:
  *
  * timestamp-field = "time"
  * topics-to-proto-class {
  * "events" = "examples.protobuf.UserOuterClass$User"
  * }
  *
  */
class ProtoTransformer(config: Config) extends Transformer[Message[protobuf.Message]] {

  val logger = LoggerFactory.getLogger(this.getClass)

  val timestampFieldName: String = config.getString("timestamp-field")

  val topicsToProtoClass: Map[String, Class[_ <: MessageOrBuilder]] = {
    val topicsToProtoType = config.getConfig("topic-to-proto-class").asMap
    topicsToProtoType.map { e => (e._1, Class.forName(e._2).asInstanceOf[Class[MessageOrBuilder]]) }
  }

  @throws(classOf[EtlException])
  override def transform(inbound: InboundMessage): Message[protobuf.Message] = {

    val protoClass: Option[Class[_]] = topicsToProtoClass.get(inbound.topic)
    if (!protoClass.isDefined)
      throw new EtlException(ErrorType.TransformationError)

    val parseMethod = protoClass.get.getMethod("parseFrom", classOf[Array[Byte]])

    val protoMsg = parseMethod.invoke(null, inbound.msg).asInstanceOf[protobuf.Message]

    val descriptor = protoMsg.getDescriptorForType.findFieldByName(timestampFieldName)
    val timestamp = protoMsg.getField(descriptor).asInstanceOf[Timestamp]

    Message(
      msg = protoMsg,
      msgType = protoMsg.getDescriptorForType.getName,
      msgTimestamp = timestamp.getSeconds * 1000
    )
  }

}
