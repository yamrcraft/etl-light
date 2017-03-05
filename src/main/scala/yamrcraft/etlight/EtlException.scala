package yamrcraft.etlight

import enumeratum.{Enum, EnumEntry}

sealed abstract class ErrorType(override val entryName: String) extends EnumEntry

object ErrorType extends Enum[ErrorType] {
  val values = findValues

  case object ParseError extends ErrorType("PARSE_ERROR")

  case object TransformationError extends ErrorType("TRANSFORMATION_ERROR")

  case object MissingSchemaError extends ErrorType("MISSING_SCHEMA_ERROR")

  case object PartitionTimestampError extends ErrorType("PARTITION_TIMESTAMP_ERROR")

  case object WriteError extends ErrorType("WRITE_ERROR")

  case object SystemError extends ErrorType("SYSTEM_ERROR")

}

class EtlException(val errorType: ErrorType, e: Exception) extends RuntimeException(errorType.toString, e) {

  def this(errorType: ErrorType) = this(errorType, null)

}
