package ca.dataedu.savro

import ca.dataedu.savro.AvroError._
import ca.dataedu.savro.AvroSchemaError._
import org.apache.avro.Schema.Field
import org.apache.avro.{ Schema, SchemaBuilder }
import org.apache.avro.Schema.Type._
import ca.dataedu.savro.AvroSchema._
import org.apache.avro.generic.GenericRecord

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.util.{ Failure, Success, Try }

object AvroImplicits {

  import implicits._

  /**
    * Add some functionalities to facilitate extracting values from an Avro message
    */
  implicit class GenericRecordOps(record: GenericRecord) {

    /** Tries to extract the value of the field name from the Avro message.
      *
      * The provided function accepts a value (a JVM object) with the schema of the field should return an optional
      * value. The function doesn't need to be concern about the null values because `as` will return a `None` if the
      * value of the field is `null` and won't call the function.
      */
    def as[T](
        fieldName: String
    )(f: (AnyRef, Schema) => Either[SAvroError, Option[T]]): Either[SAvroError, Option[T]] =
      Option(record.get(fieldName))
        .map(value => fieldSchema(fieldName).flatMap(schema => f(value, schema)))
        .pushDownOption()

    /** Returns the value of `fieldName` as long if exists */
    def asLong(fieldName: String): Either[SAvroError, Option[Long]] =
      as(fieldName) { (value, schema) =>
        schema.getType match {
          case INT | LONG => convert(value, _.asInstanceOf[Number].longValue())
          case STRING     => convert(value, _.toString.toLong)
          case _          => Left(ToNumberError(value.toString, "Field is not a supported type", None))
        }
      }

    /** Returns the value of `fieldName` as integer if exists */
    def asInt(fieldName: String): Either[SAvroError, Option[Int]] =
      as(fieldName) { (value, schema) =>
        schema.getType match {
          case INT    => convert(value, _.asInstanceOf[Number].intValue())
          case STRING => convert(value, _.toString.toInt)
          case _      => Left(ToNumberError(value.toString, "Field is not a supported type", None))
        }
      }

    /** Returns the value of `fieldName` as double if exists */
    def asDouble(fieldName: String): Either[SAvroError, Option[Double]] =
      as(fieldName) { (value, schema) =>
        schema.getType match {
          case INT | LONG | DOUBLE | FLOAT => convert(value, _.asInstanceOf[Number].doubleValue())
          case STRING                      => convert(value, _.toString.toDouble)
          case _                           => Left(ToNumberError(value.toString, "Field is not a supported type", None))
        }
      }

    /** Returns the value of `fieldName` as string if exists */
    def asString(fieldName: String): Option[String] = Option(record.get(fieldName)).map(_.toString)

    /** Returns the value of `fieldName` as boolean if exists */
    def asBoolean(fieldName: String): Either[SAvroError, Option[Boolean]] =
      as(fieldName) { (value, schema) =>
        schema.getType match {
          case BOOLEAN =>
            Try(value.asInstanceOf[Boolean]) match {
              case Failure(exception) =>
                Left(ToBooleanError(value.toString, "Unable to cast to boolean", Option(exception)))
              case Success(value) => Right(Option(value))
            }
          case _ => Left(ToBooleanError(value.toString, "Field type is not boolean", None))
        }
      }

    /** Returns the actual schema of the given field stripping "null" if it is an optional field.
      * If will return an error if the "actual" schema is a union. */
    private def fieldSchema(fieldName: String): Either[NonNullableUnionTypeError, Schema] =
      record.getSchema.getField(fieldName).schema().getTypeWithoutNull

    /** A wrapper for `GenericRecord#put` that supports null values.
      * Set the value of a field with an optional value. It puts `null` if the value is empty.
      * Note that due to GenericRecord limitation, the record is not immutable. The return value is the same record
      * and not a new on.
      * */
    def set[T](fieldName: String, value: Option[T]): GenericRecord = {
      //noinspection GetOrElseNull "orNull" is not compatible with Avro
      record.put(fieldName, value.getOrElse(null))
      record
    }

    /** A wrapper for `GenericRecord#put` that supports null values.
      * Set the value of a field with an optional value. It puts `null` if the value is empty.
      * */
    def set[T](fieldName: String, value: T): GenericRecord = {
      //noinspection GetOrElseNull "orNull" is not compatible with Avro
      record.put(fieldName, value)
      record
    }

    /** Converts the value to number based on the function or return an error if it's not a number */
    private def convert[T](fieldValue: AnyRef, f: AnyRef => T): Either[ToNumberError, Option[T]] =
      Try(f(fieldValue)) match {
        case Failure(error) => Left(ToNumberError(fieldValue.toString, "Failed to cast to a number", Option(error)))
        case Success(value) => Right(Option(value))
      }

  }

  implicit class SchemaFieldOps(field: Field) {

    /** Checks if it has same schema regardless of being optional */
    def hasSameSchema(other: Field): Boolean =
      field.schema().getTypesWithoutNull.equals(other.schema().getTypesWithoutNull)
  }

  /*  Schema implicits */
  implicit class SchemaImprovement(schema: Schema) {

    /**
      * Converts the schema to its flatten version.
      * It supports primitives, arrays and nested complex data structures.
      *
      * Rules:
      *
      * - If a primitive, keeps it as is
      * - If a record, flatten all the fields so that they become primitive. Then appends the field name to the
      *   children's field name and use their type as the output type. For example,
      *   {{{
      *    {
      *      "type": "record",
      *      "name": "PaymentRecord",
      *      "fields": [
      *        {
      *           "name": "paymentView",
      *           "type": "record",
      *           "name": "PaymentView",
      *           "fields": [
      *             { "name": "payment", "type": "long" },
      *             { "name": "revoked", "type": ["null", "boolean"], "default": null },
      *           ]
      *        }
      *      ]
      *    }
      *   }}}
      *   becomes
      *   {{{
      *    {
      *      "type": "record",
      *      "name": "PaymentRecord",
      *      "fields": [
      *        { "name": "paymentView_payment", "type": "long" },
      *        { "name": "paymentView_revoked", type": ["null", "boolean"], "default": null }
      *      ]
      *    }
      *   }}}
      * - If an array, it first makes sure that the type is a primitive with any flattening rule and then use it as
      *   the output. As an array could be empty, regardless of being optional or not, we always set the fields as
      *   optional.
      *   An array, in the process of flattening will cause an explosion.
      */
    final def flat: Either[AvroSchemaError, Schema] =
      schema.getFields.asScala
        .map(field => TempField(field.name, field.schema, field.doc, field.defaultVal))
        .map(_.flatten)
        .toList
        .toEitherOfList(identity)
        .map(_.flatten)
        .map(_.map(field => new Field(field.name, field.schema, field.doc, field.defaultValue)))
        .map { fields =>
          val outputSchema = Schema.createRecord(s"${schema.getName}Flatten", schema.getDoc, schema.getNamespace, false)
          outputSchema.setFields(fields.asJava)
          outputSchema
        }

    final def getTypesWithoutNull: Schema = schema match {
      case _ if schema.isNullable && schema.isUnion => // It could be just NULL type
        schema.getTypes.asScala.toList.filter(_.getType != NULL) match {
          case singleType :: Nil => singleType
          case multipleTypes     => Schema.createUnion(multipleTypes.asJava)
        }
      case _ => schema
    }

    /**
      * In case of a nullable schema, it returns the type if it's not null.
      * A nullable schema in Avro is the `UNION` of `NULL` and the actual type. In this method,
      * we assume that there is no more than one type (besides `NULL`) in the union.
      */
    final def getTypeWithoutNull: Either[NonNullableUnionTypeError, Schema] = schema match {
      case _ if schema.isNullable && schema.isUnion =>
        schema.getTypes.asScala.toList.filter(_.getType != NULL) match {
          case actualType :: Nil => Right(actualType)
          case _                 => Left(NonNullableUnionTypeError(schema, "Found a UNION schema that is not nullable"))
        }
      case _ => Right(schema)
    }

    final def mergeWith(otherSchema: Schema): Schema = (schema, otherSchema) match {
      // Same
      case (_, _) if schema.equals(otherSchema) => schema
      // Record with Record
      case (_, _) if schema.isRecord && otherSchema.isRecord => mergeRecordSchema(schema, otherSchema)
      // Record with a union of having same record
      case (_, _)
          if schema.isRecord && otherSchema.isUnion &&
          otherSchema.getTypes.asScala.exists(recordMatcher(_)(schema.getFullName)) =>
        val matchingRecord = otherSchema.getTypes.asScala.find(recordMatcher(_)(schema.getFullName)).get
        val unionWithOutMatchingRecord =
          Schema.createUnion(otherSchema.getTypes.asScala.filter(!recordMatcher(_)(schema.getFullName)).asJava)
        schema.mergeWith(matchingRecord).unionWithUnion(unionWithOutMatchingRecord)
      // Record with a union of having same record
      case (_, _)
          if otherSchema.isRecord && schema.isUnion &&
          schema.getTypes.asScala.exists(recordMatcher(_)(otherSchema.getFullName)) =>
        val matchingRecord = schema.getTypes.asScala.find(recordMatcher(_)(otherSchema.getFullName)).get
        val unionWithOutMatchingRecord =
          Schema.createUnion(schema.getTypes.asScala.filter(!recordMatcher(_)(otherSchema.getFullName)).asJava)
        otherSchema.mergeWith(matchingRecord).unionWithUnion(unionWithOutMatchingRecord)
      // nullable with nullable
      case (_, _) if schema.isUnion && schema.isNullable && otherSchema.isUnion && otherSchema.isNullable =>
        schema.getTypesWithoutNull.mergeWith(otherSchema.getTypesWithoutNull).makeNullable
      // null with nullable
      case (_, _) if !schema.isUnion && schema.isNullable && otherSchema.isUnion && otherSchema.isNullable =>
        otherSchema
      // nullable with null
      case (_, _) if schema.isUnion && schema.isNullable && !otherSchema.isUnion && otherSchema.isNullable => schema
      // Array with nonArray and not nullable and not union
      case (_, _) if schema.isArray && !otherSchema.isArray && !otherSchema.isUnion && !otherSchema.isNullable =>
        SchemaBuilder.unionOf().`type`(schema).and().`type`(otherSchema).endUnion()
      case (_, _) if otherSchema.isArray && !schema.isArray && !schema.isUnion && !schema.isNullable =>
        SchemaBuilder.unionOf().`type`(schema).and().`type`(otherSchema).endUnion()
      // Array with nonArray and     nullable and not union
      case (_, _) if schema.isArray && !otherSchema.isArray && !otherSchema.isUnion && otherSchema.isNullable =>
        schema.makeNullable
      case (_, _) if otherSchema.isArray && !schema.isArray && !schema.isUnion && schema.isNullable =>
        otherSchema.makeNullable
      // Array with nonArray and     nullable and     union
      case (_, _) if schema.isArray && !otherSchema.isArray && otherSchema.isUnion && otherSchema.isNullable =>
        schema.mergeWith(otherSchema.getTypesWithoutNull).makeNullable
      case (_, _) if otherSchema.isArray && !schema.isArray && schema.isUnion && schema.isNullable =>
        schema.getTypesWithoutNull.mergeWith(otherSchema).makeNullable
      // Array with nonArray and not nullable and     union
      case (_, _) if schema.isArray && !otherSchema.isArray && otherSchema.isUnion && !otherSchema.isNullable =>
        unionWithUnion(otherSchema)
      case (_, _) if otherSchema.isArray && !schema.isArray && schema.isUnion && !schema.isNullable =>
        unionWithNonUnion(otherSchema)
      // Array with Array
      case (_, _) if schema.isArray && otherSchema.isArray =>
        SchemaBuilder.array().items(schema.getElementType.mergeWith(otherSchema.getElementType).makeNullable)
    }

    /** If schema is not nullable, it will make it nullable with a default value of 'null' */
    final def makeNullable: Schema =
      if (schema.isNullable) schema
      else if (schema.isUnion) SchemaBuilder.builder().nullType().unionWithUnion(schema)
      else SchemaBuilder.builder().unionOf().nullType().and().`type`(schema).endUnion()

    final def isRecord: Boolean = schema.getType.equals(Schema.Type.RECORD)
    final def isArray: Boolean = schema.getType.equals(Schema.Type.ARRAY)

    final def toIdl(protocol: String): Either[IllegalOperationError, String] =
      if (!schema.isRecord) Left(IllegalOperationError(schema, "The input schema is not a record"))
      else Right(new AvroSchemaToIdl(schema, protocol).convert())

    /** Union of two schema
      * It makes sure that the "null" schema comes first. But if both of them are null, it returns only one of them.
      * Otherwise, returns the union of the types of both schema.
      * */
    @tailrec
    final def union(other: Schema): Schema =
      if (schema.getType.equals(NULL) && other.getType.equals(NULL)) schema
      else if (other.getType.equals(NULL)) other.union(schema)
      else if (!schema.isUnion && !other.isUnion) SchemaBuilder.unionOf().`type`(schema).and().`type`(other).endUnion()
      else if (schema.isUnion && !other.isUnion) unionWithNonUnion(other)
      else if (!schema.isUnion && other.isUnion) other.unionWithNonUnion(schema)
      else Schema.createUnion((schema.getTypes.asScala ++ other.getTypes.asScala).distinct.asJava)

    /** Union with a non-union assuming this schema is a union itself */
    def unionWithNonUnion(nonUnion: Schema): Schema = {
      /*
      We only support simple cases where union is only for nullable options.
      This is a bit of hack but if needed can be improved in future to support
      edge cases as well.
       */
      val temp = schema.getTypes.asScala.filter(_.isRecord).filter(_.getFullName.equals(nonUnion.getFullName)).toList
      val nonUnionAfterMerge = if (temp.isEmpty) nonUnion else mergeRecordSchema(temp.head, nonUnion)
      val types = schema.getTypes.asScala
        .filterNot(t => t.isRecord && t.getFullName.equals(nonUnion.getFullName)) :+ nonUnionAfterMerge
      Schema.createUnion(types.distinct.asJava)
    }

    def unionWithUnion(union: Schema): Schema = {
      /*
      We only support simple cases where union is only for nullable options.
      This is a bit of hack but if needed can be improved in future to support
      edge cases as well.
       */
      val temp = union.getTypes.asScala.filter(_.isRecord).filter(_.getFullName.equals(schema.getFullName)).toList
      val nonUnionAfterMerge = if (temp.isEmpty) schema else mergeRecordSchema(temp.head, schema)
      val types = Seq(nonUnionAfterMerge) ++ union.getTypes.asScala
        .filterNot(t => t.isRecord && t.getFullName.equals(schema.getFullName))
      Schema.createUnion(types.asJava)
    }

    private def recordMatcher(schema: Schema)(fullName: String): Boolean =
      schema.isRecord && schema.getFullName.equals(fullName)

  }
}
