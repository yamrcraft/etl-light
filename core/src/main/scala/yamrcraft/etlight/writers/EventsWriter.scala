package yamrcraft.etlight.writers

trait EventsWriter[T] {

	def write(event: T): Unit

	def commit(): Unit

}
