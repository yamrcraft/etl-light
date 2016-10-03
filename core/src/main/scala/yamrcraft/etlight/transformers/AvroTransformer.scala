package yamrcraft.etlight.transformers

import com.typesafe.config.Config
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import tech.allegro.schema.json2avro.converter.JsonAvroConverter
import yamrcraft.etlight.utils.TimeUtils
import yamrcraft.etlight.{ErrorType, EtlException}

class AvroTransformer(config: Config) extends Transformer[Message[GenericRecord]] {

  val converter = new JsonAvroConverter()

  // config settings
  val timestampField = config.getString("timestamp-field")

  //val defaultSchemaFileName: String = config.getString("default-schema-file")
  //val defaultSchema: Schema = new Schema.Parser().parse(new File(defaultSchemaFileName))
  // TODO: read from file - use configuration
  val defaultSchema: Schema = new Schema.Parser().parse(
    """
      |{
      |  "name": "AuditEvent",
      |  "type": "record",
      |  "fields": [
      |    {"name": "resource_owner", "type": ["null", "string"], "default": null},
      |    {"name": "trip_id", "type": ["null", "string"], "default": null},
      |    {"name": "additional_info_dict", "type": ["null", "string"], "default": null},
      |    {"name": "http_response", "type": ["null", "string"], "default": null},
      |    {"name": "device_id", "type": ["null", "string"], "default": null},
      |    {"name": "supplier_id", "type": ["null", "string"], "default": null},
      |    {"name": "resource_type", "type": ["null", "string"], "default": null},
      |    {"name": "elapsed", "type": ["null", "int"], "default": null},
      |    {"name": "server_fqdn", "type": ["null", "string"], "default": null},
      |    {"name": "ts", "type": ["null", "string"], "default": null},
      |    {"name": "user_id", "type": ["null", "int" ], "default": null},
      |    {"name": "uuid", "type": ["null", "string"], "default": null},
      |    {"name": "result", "type": ["null", "string"], "default": null},
      |    {"name": "suffix", "type": ["null", "string"], "default": null},
      |    {"name": "filename", "type": ["null", "string"], "default": null},
      |    {"name": "filepath", "type": ["null", "string"], "default": null},
      |    {"name": "fqn", "type": ["null", "string"], "default": null}
      |  ]
      |}
    """.stripMargin)

  @throws(classOf[EtlException])
  override def transform(key: Array[Byte], msg: Array[Byte]): Message[GenericRecord] = {

    try {
      val record = converter.convertToGenericDataRecord(msg, defaultSchema)

      Message[GenericRecord](
        record,
        defaultSchema.getName,
        extractTimestamp(record)
      )

    } catch {
      case e: EtlException => throw e
      case e: Exception => throw new EtlException(ErrorType.TransformationError, e)
    }
  }

  override def toString(event: Array[Byte]) = new String(event, "UTF8")

  @throws(classOf[EtlException])
  private def extractTimestamp(event: GenericRecord): Long = {
    try {
      (event.get(timestampField): Any) match {
        case ts: Long => ts.asInstanceOf[Long]
        case ts: String => TimeUtils.stringTimeToLong(ts, "yyyy-MM-dd HH:mm:ss")
        case _ => throw new RuntimeException("timestamp field is not of either Long or String types.")
      }
    } catch {
      case e: Exception => throw new EtlException(ErrorType.PartitionTimestampError, e)
    }
  }
}
