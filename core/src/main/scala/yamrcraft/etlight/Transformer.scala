package yamrcraft.etlight

import org.apache.avro.generic.GenericRecord

trait Transformer {

	@throws(classOf[EtlException])
	def transform(key: String, event: Array[Byte]): GenericRecord

	def toString(event: Array[Byte]): String

}
