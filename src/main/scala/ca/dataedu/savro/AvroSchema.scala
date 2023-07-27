package ca.dataedu.savro

import ca.dataedu.savro.AvroSchemaError._
import io.circe.{ Json, JsonObject }
import io.circe.parser._
import org.apache.avro.Schema.Field
import org.apache.avro.{ Schema, SchemaBuilder }

import scala.collection.JavaConverters._
import scala.util.Try
import scala.util.matching.Regex

/** A set of constructors to generate Avro schema */
object AvroSchema {

  import AvroImplicits._
  import implicits._

  private val FloatingPointPattern: Regex = "[-+]?[0-9]*\\.[0-9]+".r

  def apply(idl: String, namespace: String, recordName: String): Schema =
    AvroProtocol(idl).getType(s"$namespace.$recordName")

  def apply(json: String, name: String, namespace: Option[String]): Either[SAvroError, Schema] =
    parse(json) match {
      case Left(error)  => Left(AvroSchemaJsonError(json, error.message))
      case Right(value) => apply(value, name, namespace)
    }

  /**
    * Infer an Avro schema from a JSON object. It only accepts an object or an array of objects.
    * In any cases, the output is the Avro schema of a record.
    * If the input is an array of objects, then generated schema will try to match with all the objects.
    */
  def apply(json: Json, name: String, namespace: Option[String]): Either[SAvroError, Schema] = json match {
    case _ if json.isObject => inferRecord(json, name, namespace)
    case _ if json.isArray =>
      json.asArray match {
        case Some(value) if value.headOption.fold(false)(_.isObject) =>
          inferRecordFromArray(value, name, namespace)
        case Some(_) => Left(IllegalJsonInput(json, "The input message should be a JSON array"))
        case None    => Left(IllegalJsonInput(json, "Unable to parse input to a JSON array"))
      }
    case _ => Left(IllegalJsonInput(json, "The input message should be a JSON object or a JSON array"))
  }

  def apply(json: Json): Either[AvroSchemaJsonError, Schema] =
    Try(new Schema.Parser().parse(json.toString()))
      .fold(
        error =>
          Left(
            AvroSchemaJsonError(
              json.toString(),
              s"Unable to parse the input JSON to an Avro schema. Error ${error.getMessage}"
            )
        ),
        schema => Right(schema)
      )

  /**
    * Infer Avro schema from an array of JSON objects. The final result should accept all the input objects normally
    * using UNION and optional fields
    */
  def inferRecordFromArray(
      jsonObjects: Vector[Json],
      name: String,
      namespace: Option[String]
  ): Either[SAvroError, Schema] =
    jsonObjects
      .foldLeft(createDummyRecordSchema(name, namespace).asRight[SAvroError]) { (schema, aRecord) =>
        for {
          schemaOfPreviousObjects <- schema
          schemaOfThisObject <- inferRecord(aRecord, name, namespace)
        } yield mergeRecordSchema(schemaOfPreviousObjects, schemaOfThisObject)
      }

  /**
    * Tries to create the schema for the input JSON object as a RECORD.
    * First, it will sanitize the field names to be compatible with Avro standard.
    * Then, creates the schema for each field value and accumulates them in one and returns it.
    *
    * @param json the input JSON object
    * @param name the name of the object. It will makes sue that the first letter is capital
    * @param nameSpace an optional namespace to add to the record. If not provided, it will use empty string
    * @return the schema corresponding to the input JSON or an error if the input JSON is not an object ot it fails to
    *         infer the schema of any field
    */
  def inferRecord(json: Json, name: String, nameSpace: Option[String]): Either[IllegalJsonInput, Schema] =
    json.asObject match {
      case Some(value) => inferRecord(value, name, nameSpace)
      case None        => Left(IllegalJsonInput(json, "Unable to parse input to a JSON object"))
    }

  /**
    * Tries to create the schema for the input JSON object as a RECORD.
    * First, it will sanitize the field names to be compatible with Avro standard.
    * Then, creates the schema for each field value and accumulates them in one and returns it.
    *
    * @param jsonObject the input JSON object
    * @param name the name of the object. It will makes sue that the first letter is capital
    * @param nameSpace an optional namespace to add to the record. If not provided, it will use empty string
    * @return the schema corresponding to the input JSON or an error if the input JSON is not an object ot it fails to
    *         infer the schema of any field
    */
  def inferRecord(
      jsonObject: JsonObject,
      name: String,
      nameSpace: Option[String]
  ): Either[IllegalJsonInput, Schema] = {
    val recordName = s"${name.head.toUpper.toString}${name.tail}"
    val builder = SchemaBuilder.record(recordName).namespace(nameSpace.getOrElse("")).fields()
    jsonObject.toList
      .filter(_._1.nonEmpty)
      .map {
        case (fieldName, fieldValue) =>
          val sanitizedFieldName = SanitizedFieldName(fieldName)
          (sanitizedFieldName.validFieldName, fieldValue, sanitizedFieldName.explanation.orNull)
      }
      .sortBy(_._1)
      .foreach {
        case (fieldName, fieldValue, doc) =>
          inferSchema(fieldValue, fieldName, nameSpace, isArrayItem = false) match {
            case Left(error)        => return Left(error)
            case Right(fieldSchema) => builder.name(fieldName).doc(doc).`type`(fieldSchema).withDefault(null)
          }
      }
    Right(builder.endRecord())
  }

  /**
    * Merges the fields of the right side into the fields of the left side.
    * It will keep the union of the fields and makes sure the fields with the same name maintain compatible schema
    *
    * @param left  the base record
    * @param right the new record to be merged with the base record
    * @return
    */
  def mergeRecordSchema(left: Schema, right: Schema): Schema = {
    //noinspection RedundantCollectionConversion
    val leftKeyed: Seq[(String, Field)] = left.getFields.asScala.toSeq.map(f => f.name() -> f)
    //noinspection RedundantCollectionConversion
    val rightKeyed: Seq[(String, Field)] = right.getFields.asScala.toSeq.map(f => f.name() -> f)
    val builder = SchemaBuilder.record(left.getName).namespace(left.getNamespace).fields()
    val x: Map[String, Seq[(String, Field)]] = (leftKeyed ++ rightKeyed).groupBy(_._1)
    val y: Seq[(String, List[Field])] = x.map(a => (a._1, a._2.map(_._2).toList)).toSeq.sortBy(_._1)
    y.foldLeft(builder) {
      /* Field is in the left not in the right */
      case (b, (fieldName, field :: Nil)) =>
        b.name(fieldName).`type`(field.schema()).withDefault(null)
      /* Field is in both left and right but has the same schema */
      case (b, (fieldName, existingField :: newField :: Nil)) if newField.hasSameSchema(existingField) =>
        b.name(fieldName).`type`(existingField.schema()).withDefault(null)
      /* Field is in both left and right and both are records */
      case (b, (fieldName, existingField :: newField :: Nil))
          if existingField.schema().getTypesWithoutNull.isRecord && newField.schema().getTypesWithoutNull.isRecord =>
        b.name(fieldName)
          .`type`(
            mergeRecordSchema(
              existingField.schema().getTypesWithoutNull,
              newField.schema().getTypesWithoutNull
            ).makeNullable
          )
          .withDefault(null)
      /* Field is in both left and right and both are array */
      case (b, (fieldName, existingField :: newField :: Nil)) if existingField.schema().getTypesWithoutNull.isArray =>
        val itemSchema = existingField.schema().mergeWith(newField.schema()).makeNullable
        b.name(fieldName).`type`(itemSchema).withDefault(null)
      /* Field is in both left and right with different schema */
      case (b, (fieldName, existingField :: newField :: Nil)) =>
        b.name(fieldName)
          .`type`(existingField.schema().getTypesWithoutNull.union(newField.schema().getTypesWithoutNull).makeNullable)
          .withDefault(null)
    }
    builder.endRecord()
  }

  def inferSchema(
      json: Json,
      name: String,
      namespace: Option[String],
      isArrayItem: Boolean
  ): Either[IllegalJsonInput, Schema] =
    json match {
      case _ if json.isNull    => Right(SchemaBuilder.builder().nullType())
      case _ if json.isString  => Right(SchemaBuilder.builder().stringType().makeNullable)
      case _ if json.isBoolean => Right(SchemaBuilder.builder().booleanType().makeNullable)
      case _ if json.isNumber && FloatingPointPattern.findFirstIn(json.toString()).nonEmpty =>
        Right(SchemaBuilder.builder().doubleType().makeNullable)
      case _ if json.isNumber && json.asNumber.fold(false)(_.toInt.isDefined) =>
        Right(SchemaBuilder.builder().intType().makeNullable)
      case _ if json.isNumber =>
        Right(SchemaBuilder.builder().longType().makeNullable)
      case _ if json.isObject && isArrayItem && name.endsWith("s") =>
        inferRecord(json, name.init, namespace).map(_.makeNullable)
      case _ if json.isObject => inferRecord(json, name, namespace).map(_.makeNullable)
      case _ if json.isArray  => inferArray(json, name, namespace).map(_.makeNullable)
    }

  /**
    * Assuming that the input JSON is an array, it will take the first element and infers the schema as array of that
    * type. If the array is empty, it will return array of "null". If the value is not an array, it will return an
    * error.
    */
  def inferArray(json: Json, name: String, namespace: Option[String]): Either[IllegalJsonInput, Schema] =
    json.asArray match {
      case Some(value) =>
        value.headOption match {
          case Some(head) => inferSchema(head, name, namespace, isArrayItem = true).map(SchemaBuilder.array().items(_))
          case None       => Right(SchemaBuilder.array().items(SchemaBuilder.builder().nullType()))
        }
      case None => Left(IllegalJsonInput(json, "Unable to parse input to a JSON array"))
    }

  private def createDummyRecordSchema(name: String, nameSpace: Option[String]): Schema =
    SchemaBuilder.record(name).namespace(nameSpace.getOrElse("")).fields().endRecord()

}
