package yamrcraft.etlight.transformers

import com.typesafe.config.Config
import play.api.libs.json.Json
import yamrcraft.etlight.utils.TimeUtils
import yamrcraft.etlight.{ErrorType, EtlException}

class JsonTransformer(config: Config) extends Transformer[Message[String]] {

  // config settings
  val timestampField = config.getString("timestamp-field")
  val timestampFieldFormat = config.getString("timestamp-field-format")

  @throws(classOf[EtlException])
  override def transform(key: Array[Byte], msg: Array[Byte]): Message[String] = {
    try {
      val msgStr = toString(msg)
      val msgJson = Json.parse(msgStr)
      val timestamp: Option[String] = (msgJson \ timestampField).asOpt[String]
      timestamp match {
        case Some(event) => Message(msgStr, "AuditEvent", TimeUtils.stringTimeToLong(event, timestampFieldFormat))
        case None => throw new EtlException(ErrorType.TransformationError)
      }
    } catch {
      case e: Exception => throw new EtlException(ErrorType.TransformationError, e)
    }
  }

  override def toString(msg: Array[Byte]) = new String(msg, "UTF8")
}
