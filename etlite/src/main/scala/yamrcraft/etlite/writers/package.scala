package yamrcraft.etlite

import org.apache.avro.generic.GenericRecord
import yamrcraft.etlite.transformers.Message

package object writers {

  type AvroEventsWriter = Writer[Message[GenericRecord]]

  type ErrorEventsWriter = Writer[ErrorEvent]

}
