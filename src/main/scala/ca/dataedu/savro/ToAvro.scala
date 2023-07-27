package ca.dataedu.savro

import org.apache.avro.Schema
import org.apache.avro.generic.{ GenericRecord, GenericRecordBuilder }
import org.apache.avro.Schema.Type._

import scala.collection.JavaConverters._

object ToAvro {

  import AvroImplicits._

  def from[T <: Product](input: T, schema: Schema): GenericRecord =
    Option(input)
      .map(convertObjectToMap)
      .map(inputValues => buildAvro(schema, inputValues).asInstanceOf[GenericRecord])
      .orNull

  private def getNullableType(schema: Schema): Schema =
    schema.getTypeWithoutNull.getOrElse(throw new RuntimeException("Invalid type"))

  def buildAvro(schema: Schema, input: Any): Any = {
    schema.getType match {
      case ARRAY if input.isInstanceOf[Iterable[_]] =>
        input
          .asInstanceOf[Iterable[Any]]
          .map(element => buildAvro(getNullableType(schema.getElementType), element))
          .toList
          .asJava
      case MAP if input.isInstanceOf[Map[String, _]] =>
        val elementSchema = getNullableType(schema.getValueType)
        input
          .asInstanceOf[Map[String, Any]]
          .map { case (key, value) => key -> buildAvro(elementSchema, value) }
          .asJava
      case RECORD if input.isInstanceOf[Map[String, _]] =>
        val record = input.asInstanceOf[Map[String, _]]
        val builder = new GenericRecordBuilder(schema)
        schema.getFields.asScala
          .map(f => f.name() -> getNullableType(f.schema()))
          .foreach {
            case (fieldName, schema) => builder.set(fieldName, buildAvro(schema, record.get(fieldName).orNull))
          }
        builder.build()
      case STRING | INT | LONG | FLOAT | DOUBLE | BOOLEAN => input
      case NULL                                           => null
      case _ =>
        throw new RuntimeException(s"Unable to convert type $schema with value $input")
    }
  }

  def convertObjectToMap(cc: Product): Map[String, Any] = {
    val fields = cc.getClass.getDeclaredFields.toList.filter(_.getName != "$outer").map(_.getName)
    val values = cc.productIterator.toList

    fields
      .zip(values)
      .map {
        case (name, value) if value == null => name -> null
        case (name, Some(value))            => name -> value
        case (name, None)                   => name -> null
        case (name: String, x :: xs) if x.isInstanceOf[Product] =>
          name -> (x :: xs).asInstanceOf[List[Product]].map(convertObjectToMap)
        case (name: String, value: List[_])                   => name -> value
        case (name: String, p: Product) if p.productArity > 0 => name -> convertObjectToMap(p)
        case (name: String, value)                            => name -> value
      }
      .toMap
  }

}
