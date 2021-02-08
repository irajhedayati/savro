package ca.dataedu.savro

import org.apache.avro.{ JsonProperties, Schema }

import scala.collection.mutable
import scala.collection.JavaConverters._

class AvroSchemaToIdl(schema: Schema, protocol: String) {

  import AvroImplicits._

  private val recordsToProcessStack: mutable.Stack[Schema] = mutable.Stack[Schema]()

  /** A set of records already processed. It is in form of `namespace.name` */
  private val records: mutable.Set[String] = mutable.Set()

  final def convert(): String =
    s"""@namespace("${schema.getNamespace}")
       |protocol $protocol {${convertJustRecord()}
       |}
       |""".stripMargin

  final def convertJustRecord(): String = {
    recordsToProcessStack.push(schema)
    var recordsInString = ""
    while (recordsToProcessStack.nonEmpty) recordsInString = recordsInString + "\n" + recordToIdl()
    recordsInString
  }

  private def recordToIdl(): String = {
    val schema: Schema = recordsToProcessStack.pop()
    val recordName = s"${schema.getNamespace}.${schema.getName}"
    if (records.contains(recordName)) ""
    else {
      val fields =
        schema.getFields.asScala.sortBy(_.name().toLowerCase()).map(field => fieldToIdl(field)).mkString("\n")
      records.add(recordName)
      s"""  record ${schema.getName} {
         |$fields
         |  }
         |""".stripMargin
    }
  }

  val keywords: Seq[String] = Schema.Type.values().toIndexedSeq.map(_.getName.toLowerCase())

  private def fieldToIdl(field: Schema.Field): String = {
    val doc = if (field.doc() != null && field.doc().nonEmpty) s" // ${field.doc()}" else ""
    val defaultValue = if (field.hasDefaultValue) " = " + extractDefaultValue(field.defaultVal()) else ""
    val fieldName = if (keywords.contains(field.name().toLowerCase())) s"`${field.name()}`" else field.name()
    s"    ${schemaTypeInIdl(field.schema())} $fieldName$defaultValue;$doc"
  }

  private def extractDefaultValue(field: AnyRef): String = field match {
    case _: JsonProperties.Null => "null"
    case _                      => field.toString
  }

  private def schemaTypeInIdl(field: Schema): String = field match {
    case _ if field.isUnion && field.isNullable =>
      s"union { ${field.getTypes.asScala.map(schemaTypeInIdl).mkString(", ")} }"
    case _ if field.isRecord =>
      recordsToProcessStack.push(field)
      field.getName
    case _ if field.isArray => s"array<${schemaTypeInIdl(field.getElementType)}>"
    case _ if field.isMap   => s"map<${schemaTypeInIdl(field.getValueType)}>"
    case _                  => field.getType.toString.toLowerCase
  }
}
