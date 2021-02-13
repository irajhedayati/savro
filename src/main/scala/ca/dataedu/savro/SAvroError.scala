package ca.dataedu.savro

import io.circe.Json
import org.apache.avro.Schema

sealed trait SAvroError extends Throwable

sealed trait AvroSchemaError extends SAvroError {
  val input: Object
  val message: String
  override def toString: String =
    s"""Input: ${input.toString}
       |Message: $message""".stripMargin
}

object AvroSchemaError {

  /** When the input to infer Avro schema is not what we expected */
  final case class IllegalJsonInput(override val input: Json, override val message: String) extends AvroSchemaError

  /** When an illegal operation on an Avro schema is called */
  final case class IllegalOperationError(override val input: Schema, override val message: String)
      extends AvroSchemaError

  /** When unable to get Avro schema from an IDL definition */
  final case class IdlParseError(override val input: String, override val message: String) extends AvroSchemaError

  /** When the input IDl is not provided properly to parse an Avro schema.
    * We don't know the details of the errors that could happen at the moment an it is just a place holder. */
  final case class IllegalIdlInput(override val input: String, override val message: String) extends AvroSchemaError

  /** The only union that we support is nullable type where it is union of NULL and any other types. */
  final case class NonNullableUnionTypeError(override val input: Schema, override val message: String)
      extends AvroSchemaError

  /** If we fail to register a schema under a subject in Confluent Schema Registry */
  final case class SchemaRegistrationError(override val input: Schema, subject: String, override val message: String)
      extends AvroSchemaError {
    override def toString: String =
      s"""${super.toString}
         |Subject: $subject""".stripMargin
  }

  /** When an error happens parsing JSON representation of an Avro schema. As the operation are under control,
    * this error never happens and is just to complete the types. */
  final case class AvroSchemaJsonError(override val input: String, override val message: String) extends AvroSchemaError

  /** An error raised when trying to do an operation on types that are not supported. The definition of "unsupported"
    * depends on the direction. For example, in converting Avro schema to Hive schema, Hive doesn't support type of
    * 'NULL' that Avro has it. */
  final case class UnsupportedTypeError(override val input: String, override val message: String)
      extends AvroSchemaError
}

sealed trait AvroError extends SAvroError {
  val input: AnyRef
  val message: String
  val cause: Option[Throwable]

  override def toString: String =
    s"""Input: $input
       |Error Message: $message
       |cause: ${cause.map(_.getMessage)}""".stripMargin
}

object AvroError {
  final case class ToNumberError(input: String, message: String, cause: Option[Throwable]) extends AvroError
  final case class ToBooleanError(input: String, message: String, cause: Option[Throwable]) extends AvroError
}
