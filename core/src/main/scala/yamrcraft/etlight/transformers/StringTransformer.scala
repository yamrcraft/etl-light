package yamrcraft.etlight.transformers

import com.typesafe.config.Config
import play.api.libs.json.Json
import yamrcraft.etlight.utils.TimeUtils
import yamrcraft.etlight.{ErrorType, EtlException}

class StringTransformer(config: Config) extends Transformer[Message[String]] {

  // config settings
  val timestampField = config.getString("timestamp-field")
  val timestampFieldFormat = config.getString("timestamp-field-format")

  @throws(classOf[EtlException])
  override def transform(key: Array[Byte], msg: Array[Byte]): Message[String] = {
    val msgStr = toString(msg)
    val msgJson = Json.parse(msgStr)
    val timestamp: Option[String] = (msgJson \ timestampField).asOpt[String]
    timestamp match {
      case ts: Option[String] => Message(msgStr, "AuditEvent", TimeUtils.stringTimeToLong(ts.get, timestampFieldFormat))
      case _ => throw new EtlException(ErrorType.TransformationError)
    }

  }

  override def toString(msg: Array[Byte]) = new String(msg, "UTF8")
}
