package yamrcraft.etlite.utils

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData

class JsonAvroConverter {

  val recordReader = new CustomJsonGenericRecordReader

  def convertToGenericDataRecord(data: Array[Byte], schema: Schema): GenericData.Record = recordReader.read(data, schema)

}
