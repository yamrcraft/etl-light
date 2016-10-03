package yamrcraft.etlight

import org.apache.avro.generic.GenericRecord
import yamrcraft.etlight.transformers.Message

package object writers {

  type AvroEventsWriter = Writer[Message[GenericRecord]]

  type ErrorEventsWriter = Writer[ErrorEvent]

}
