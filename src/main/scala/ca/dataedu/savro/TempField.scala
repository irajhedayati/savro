package ca.dataedu.savro

import ca.dataedu.savro.AvroSchemaError.NonNullableUnionTypeError
import org.apache.avro.Schema.Type._
import org.apache.avro.{ JsonProperties, Schema }

import scala.collection.JavaConverters._

/** The Avro fields are final. We use this case class to carry information and do modifications before instantiating
  * the final Avro field in a schema */
final case class TempField(name: String, schema: Schema, doc: String, defaultValue: Any) {

  import AvroImplicits._
  import implicits._

  /**
    * Returns the flat version of a field.
    * The name of field is constructed from the `name` and the schema of the field. If it is a primitive type,
    * then the final name is equal to `name`. But if it is a nested record, then the final name is concatenation of
    * `name` with child field name separated by `_` character.
    * If the field is an array (or contains a nested array), it returns the schema of the items after flattening them
    *
    * In case of UNION, we only support nullable types i.e. when it is the union of NULL and one other type. Any
    * other combination will cause `NonNullableUnionTypeError`
    */
  def flatten: Either[NonNullableUnionTypeError, List[TempField]] = schema.getType match {
    case STRING | FIXED | BOOLEAN | DOUBLE | FLOAT | LONG | INT | NULL => Right(List(this))
    case UNION =>
      schema.getTypeWithoutNull
        .map(actualSchema => copy(schema = actualSchema).flatten)
        .joinRight
        .map(_.map(_field => _field.copy(schema = _field.schema.makeNullable)))
    case RECORD =>
      schema.getFields.asScala
        .map { _field =>
          TempField(s"${name}_${_field.name()}", _field.schema(), _field.doc(), _field.defaultVal()).flatten
        }
        .toList
        .toEitherOfList(identity)
        .map(_.flatten)
    case ARRAY =>
      //  flatten the element type and make sure it is nullable as the array could be empty
      schema.getElementType.getTypeWithoutNull.flatMap { itemSchema =>
        copy(schema = itemSchema).flatten.map {
          _.map(
            _field => _field.copy(schema = _field.schema.makeNullable).copy(defaultValue = JsonProperties.NULL_VALUE)
          )
        }
      }
    case _ =>
      // ignore types that are not supported yet
      Right(List())
  }
}
