package yamrcraft.etlite.writers

trait Writer[T] {

  def write(event: T): Unit

  def commit(): Unit

}
