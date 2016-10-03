package yamrcraft.etlight.transformers

import com.typesafe.config.Config
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import play.api.libs.json.Json
import yamrcraft.etlight.utils.TimeUtils
import yamrcraft.etlight.{ErrorType, EtlException}

class StringTransformer(config: Config) extends Transformer[Message[String]] {

  // config settings
  val timestampField = config.getString("timestamp-field")

  @throws(classOf[EtlException])
  override def transform(key: Array[Byte], msg: Array[Byte]): Message[String] = {
    val msgStr = toString(msg)
    val msgJson = Json.parse(msgStr)
    val timestamp: Option[String] = (msgJson \ timestampField).asOpt[String]
    timestamp match {
      case ts: Option[String] => Message(msgStr, "AuditEvent", TimeUtils.stringTimeToLong(ts.get, "yyyy-MM-dd HH:mm:ss"))
      case _ => throw new EtlException(ErrorType.TransformationError)
    }

  }

  override def toString(event: Array[Byte]): String = new String(event, "UTF8")
}
