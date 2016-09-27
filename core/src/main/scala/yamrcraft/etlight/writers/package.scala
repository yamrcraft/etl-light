package yamrcraft.etlight

import org.apache.avro.generic.GenericRecord

package object writers {

	type AvroEventsWriter = EventsWriter[GenericRecord]

	type ErrorEventsWriter = EventsWriter[ErrorEvent]

}
