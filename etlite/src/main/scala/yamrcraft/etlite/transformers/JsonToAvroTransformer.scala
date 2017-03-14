package yamrcraft.etlite.transformers

import com.typesafe.config.Config
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import play.api.libs.json.Json
import yamrcraft.etlite.utils.ConfigConversions._
import yamrcraft.etlite.utils.{FileUtils, JsonAvroConverter, TimeUtils}
import yamrcraft.etlite.{ErrorType, EtlException}

class JsonToAvroTransformer(config: Config) extends Transformer[Message[GenericRecord]] {

  val converter = new JsonAvroConverter()

  // config settings
  val timestampField = config.getString("timestamp-field")
  val timestampFieldFormat = config.getString("timestamp-field-format")
  val defaultSchemaFileName = config.getString("default-schema-file")
  val (schemaSelectionField, schemas) = {
    config.hasPath("schema-selection") match {
      case true =>
        (Some(config.getString("schema-selection.field")),
          Some(config.getConfig("schema-selection.schemas").asMap.map {case (k,v) => (k, createSchema(v))}) )
      case false => (None, None)
    }
  }

  val defaultSchema: Schema = createSchema(defaultSchemaFileName)

  @throws(classOf[EtlException])
  override def transform(message: InboundMessage): Message[GenericRecord] = {

    try {
      val schema = getSchema(message.msg)
      val record = converter.convertToGenericDataRecord(message.msg, schema)

      Message[GenericRecord](
        record,
        schema.getName,
        extractTimestamp(record)
      )

    } catch {
      case e: EtlException => throw e
      case e: Exception => throw new EtlException(ErrorType.TransformationError, e)
    }
  }

  override def toString(event: Array[Byte]) = new String(event, "UTF8")

  private def createSchema(path: String): Schema = new Schema.Parser().parse(FileUtils.readContent(path))

  private def getSchema(msg: Array[Byte]): Schema = {
    if (schemaSelectionField.isEmpty) {
      defaultSchema
    } else {
      val msgJson = Json.parse(toString(msg))
      val selectionValue = (msgJson \ schemaSelectionField.get).asOpt[String]
      schemas.get.getOrElse(selectionValue.get, defaultSchema)
    }
  }

  @throws(classOf[EtlException])
  private def extractTimestamp(event: GenericRecord): Long = {
    try {
      (event.get(timestampField): Any) match {
        case ts: Long => ts.asInstanceOf[Long]
        case ts: String => TimeUtils.stringTimeToLong(ts, timestampFieldFormat)
        case _ => throw new RuntimeException("timestamp field is not of either Long or String types.")
      }
    } catch {
      case e: Exception => throw new EtlException(ErrorType.PartitionTimestampError, e)
    }
  }
}
