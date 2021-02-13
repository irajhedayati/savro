package ca.dataedu.savro

import ca.dataedu.savro.implicits._
import ca.dataedu.savro.AvroSchemaError.UnsupportedTypeError
import org.apache.avro.Schema
import org.apache.avro.Schema.Type._

import scala.collection.JavaConverters._

object HiveSchema {

  import AvroImplicits._

  private val SPACE = "  "
  private def leadingSpace(level: Int): String = SPACE * level

  /**
    * Returns a CREATE TABLE statement from the input Avro schema
    */
  def apply(avroSchema: Schema): Either[AvroSchemaError, String] =
    avroFieldToHiveField(avroSchema.getFields.asScala.toList, 0).map { fields =>
      s"""CREATE TABLE ${avroSchema.getName.toSnakeCase} (
         |$fields
         |)""".stripMargin
    }

  /**
    * Gets a list of Avro field definitions and returns a single string that represents comma+new_line
    * separated of the Hive compatible column definitions.
    *
    * For example,
    *
    * {{{ [{"name":"ruleName","type":"string"},{"name":"ruleVersion","type":"int"}]} }}}
    *
    * returns
    *
    * {{{
    *     ruleName        string,
    *     ruleVersion        int
    * }}}
    *
    * @param fields list of Avro field definitions
    * @param level  0 for top level and more for the nested records
    * @return comma+new_line separated of Hive definitions
    */
  def avroFieldToHiveField(fields: List[Schema.Field], level: Int): Either[AvroSchemaError, String] =
    fields.map(field => avroFieldToHiveField(field, level)).toEitherOfList(identity).map(_.mkString(",\n"))

  /**
    * Converts an Avro field definition to an equivalent column definition compatible with HiveQL
    *
    * @param field input Avro field definition
    * @param level the level of nested records for formatting
    * @return a HiveQL compatible column definition
    */
  def avroFieldToHiveField(field: Schema.Field, level: Int): Either[AvroSchemaError, String] =
    avroTypeToHiveType(field.schema, level).map { hiveType =>
      val nameTypeDel = if (level > 0) ": " else ""
      leadingSpace(level + 1) + String.format("%-30s", s"`${field.name}`") + nameTypeDel + hiveType
    }

  /**
    * Gets an AVRO type definition of array and converts it to ARRAY in HiveQL compatible field definition.
    *
    * @param schema field schema
    * @return column type in HiveQL
    */
  def avroTypeToHiveType(schema: Schema, level: Int): Either[AvroSchemaError, String] = schema.getType match {
    case STRING         => Right("string")
    case INT            => Right("int")
    case LONG           => Right("bigint")
    case BOOLEAN        => Right("boolean")
    case FLOAT | DOUBLE => Right("float")
    case UNION          => schema.getTypeWithoutNull.flatMap(avroTypeToHiveType(_, level))
    case ARRAY =>
      avroTypeToHiveType(schema.getElementType, level).map(elementType => s"array<$elementType>")
    case MAP =>
      avroTypeToHiveType(schema.getValueType, level).map(valueType => s"map<string,$valueType>")
    case RECORD =>
      avroFieldToHiveField(schema.getFields.asScala.toList, level + 1)
        .map(recordDefinition => s"struct<\n$recordDefinition\n${leadingSpace(level)}>")
    case _ => Left(UnsupportedTypeError(schema.toString, s"The type ${schema.getType} is not supported in HiveQL."))
  }
}
