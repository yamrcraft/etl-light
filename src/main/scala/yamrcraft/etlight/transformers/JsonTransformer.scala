package yamrcraft.etlight.transformers

import com.typesafe.config.Config
import play.api.libs.json.Json
import yamrcraft.etlight.utils.TimeUtils
import yamrcraft.etlight.{ErrorType, EtlException}

class JsonTransformer(config: Config) extends Transformer[Message[String]] {

  // config settings
  val timestampField = config.getString("timestamp-field")
  val timestampFieldFormat = config.getString("timestamp-field-format")
  val defaultMessageType = config.getString("default-message-type")
  val typeSelectionField = config.getString("message-type-selection-field")

  @throws(classOf[EtlException])
  override def transform(key: Array[Byte], msg: Array[Byte]): Message[String] = {
    try {
      val msgStr = toString(msg)
      val msgJson = Json.parse(msgStr)
      val msgType: String = (msgJson \ typeSelectionField).asOpt[String].getOrElse(defaultMessageType)
      val timestamp: Option[String] = (msgJson \ timestampField).asOpt[String]
      timestamp match {
        case Some(event) => Message(msgStr, msgType, TimeUtils.stringTimeToLong(event, timestampFieldFormat))
        case None => throw new EtlException(ErrorType.TransformationError)
      }
    } catch {
      case e: Exception => throw new EtlException(ErrorType.TransformationError, e)
    }
  }

  override def toString(msg: Array[Byte]) = new String(msg, "UTF8")
}
