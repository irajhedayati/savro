package ca.dataedu.savro

import ca.dataedu.savro.AvroSchemaError.UnsupportedTypeError
import org.apache.avro.{ Schema, SchemaBuilder }
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters._

class HiveSchemaTest extends AnyFlatSpec with Matchers {

  private val recordSchema =
    SchemaBuilder.record("VersionedRule").fields.requiredString("ruleName").requiredInt("ruleVersion").endRecord

  behavior.of("avroFieldToHiveField")

  it should "convert 'string' to 'string'" in {
    val field = new Schema.Field("dateInserted", SchemaBuilder.builder.stringType)
    HiveSchema.avroFieldToHiveField(field, 0) shouldBe Right("  `dateInserted`                string")
  }

  it should "convert 'int' to 'string'" in {
    val field = new Schema.Field("ruleVersion", SchemaBuilder.builder.intType)
    HiveSchema.avroFieldToHiveField(field, 0) shouldBe Right("  `ruleVersion`                 int")
  }

  it should "convert 'long' to 'bigint'" in {
    val field = new Schema.Field("longField", SchemaBuilder.builder.longType)
    HiveSchema.avroFieldToHiveField(field, 0) shouldBe Right("  `longField`                   bigint")
  }

  it should "convert 'boolean' to 'boolean'" in {
    val field = new Schema.Field("timeout", SchemaBuilder.builder.booleanType)
    HiveSchema.avroFieldToHiveField(field, 0) shouldBe Right("  `timeout`                     boolean")
  }

  it should "convert 'float' to 'float'" in {
    val field = new Schema.Field("floatField", SchemaBuilder.builder.floatType)
    HiveSchema.avroFieldToHiveField(field, 0) shouldBe Right("  `floatField`                  float")
  }

  it should "convert 'double' to 'double'" in {
    val field = new Schema.Field("doubleField", SchemaBuilder.builder.doubleType)
    HiveSchema.avroFieldToHiveField(field, 0) shouldBe Right("  `doubleField`                 float")
  }

  it should "convert 'optional string' to 'string'" in {
    val field = new Schema.Field("scriptError", SchemaBuilder.nullable.stringType)
    HiveSchema.avroFieldToHiveField(field, 0) shouldBe Right("  `scriptError`                 string")
  }

  it should "convert 'array of optional literal' to 'array'" in {
    val field = new Schema.Field("values", SchemaBuilder.array.items.nullable.stringType)
    HiveSchema.avroFieldToHiveField(field, 0) shouldBe Right("  `values`                      array<string>")
  }

  it should "convert 'array of literal' to 'array'" in {
    val field = new Schema.Field("values", SchemaBuilder.array.items.stringType)
    HiveSchema.avroFieldToHiveField(field, 0) shouldBe Right("  `values`                      array<string>")
  }

  it should "convert 'nested array of literal' to 'array'" in {
    val field = new Schema.Field("values", SchemaBuilder.array.items.stringType)
    val expected = Right("    `values`                      : array<string>")
    HiveSchema.avroFieldToHiveField(field, 1) shouldBe expected
  }

  it should "convert 'map of literal' to 'map'" in {
    val field = new Schema.Field("configData", SchemaBuilder.map.values.stringType)
    val expected = Right("  `configData`                  map<string,string>")
    HiveSchema.avroFieldToHiveField(field, 0) shouldBe expected
  }

  it should "convert 'map of optional literal' to 'map'" in {
    val field = new Schema.Field("configData", SchemaBuilder.map.values.nullable.stringType)
    val expected = Right("  `configData`                  map<string,string>")
    HiveSchema.avroFieldToHiveField(field, 0) shouldBe expected
  }

  it should "avroFieldToHiveField2" in {
    val expected = Right("""  `ruleName`                    string,
                           |  `ruleVersion`                 int""".stripMargin)
    HiveSchema.avroFieldToHiveField(recordSchema.getFields.asScala.toList, 0) shouldBe expected
  }

  "avroTypeToHiveType" should "return proper format of record" in {
    val expectedRecordType =
      """struct<
        |      `ruleName`                    : string,
        |      `ruleVersion`                 : int
        |  >""".stripMargin
    HiveSchema.avroTypeToHiveType(recordSchema, 1) shouldBe Right(expectedRecordType)
  }

  it should "return error if the Avro type is not supported by Hive" in {
    HiveSchema.avroTypeToHiveType(SchemaBuilder.builder.nullType, 0).left.get shouldBe a[UnsupportedTypeError]
  }

  "HiveSchema" should "generate the HiveQL compatible of create table query" in {
    val schema = new Schema.Parser().parse(
      """{
        |  "type": "record",
        |  "name": "NewPerson",
        |  "namespace": "ca.dataedu.avro",
        |  "fields": [
        |   {
        |    "name": "phone",
        |    "type": "long",
        |    "default": 0
        |  }, {
        |    "name": "lastName",
        |    "type": "string"
        |  }, {
        |    "name": "name",
        |    "type": ["null", "string"],
        |    "default": null
        |  }, {
        |    "name": "addresses",
        |    "type": {
        |      "type": "map",
        |      "values": {
        |        "type": "record",
        |        "name": "Address",
        |        "namespace": "ca.dataedu.avro",
        |        "fields": [{
        |          "name": "street",
        |          "type": "string"
        |        }, {
        |          "name": "city",
        |          "type": "string"
        |        }]
        |      }
        |    }
        |  }]
        |}""".stripMargin
    )

    HiveSchema(schema) shouldBe
    Right("""CREATE TABLE new_person (
            |  `phone`                       bigint,
            |  `lastName`                    string,
            |  `name`                        string,
            |  `addresses`                   map<string,struct<
            |    `street`                      : string,
            |    `city`                        : string
            |>>
            |)""".stripMargin)

  }
}
