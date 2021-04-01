package ca.dataedu.savro

import io.circe.Json
import io.circe.parser._
import org.apache.avro.{ Schema, SchemaBuilder }
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers._

class AvroSchemaTest extends AnyFlatSpec {

  import AvroImplicits._

  "Avro schema" should "Make the input schema nullable" in {
    val result = SchemaBuilder.builder().stringType().makeNullable
    result.isUnion mustBe true
    result.isNullable mustBe true
  }

  "Infer field" should "support floating point number" in {
    val doubleValue = Json.fromDoubleOrNull(79.13796)
    AvroSchema.inferSchema(doubleValue, "latitude", None, isArrayItem = false) mustBe Right(
      SchemaBuilder.builder().doubleType().makeNullable
    )
  }

  it should "support integer number" in {
    val intValue = Json.fromDoubleOrNull(79)
    AvroSchema.inferSchema(intValue, "count", None, isArrayItem = false) mustBe Right(
      SchemaBuilder.builder().doubleType().makeNullable
    )
  }

  "Create record" should "If there is an empty key, ignore it" in {
    parse("""
            |{
            |  "": "",
            |  "event": "savings_account_tab_clicked",
            |  "screenName": "/bank/savings-account"
            |}
            |""".stripMargin).map(
      json =>
        AvroSchema.inferRecord(json, "Payload", None) mustBe Right(
          SchemaBuilder
            .record("Payload")
            .fields()
            .name("event")
            .`type`(SchemaBuilder.builder().stringType().makeNullable)
            .withDefault(null)
            .name("screenName")
            .`type`(SchemaBuilder.builder().stringType().makeNullable)
            .withDefault(null)
            .endRecord()
      )
    )
  }

  it should "handle illegal characters in the field name" in {
    parse("""{
            |  " AB - ( CD ) ": "value"
            |}""".stripMargin).map(
      json =>
        AvroSchema.inferRecord(json, "Payload", None) mustBe Right(
          SchemaBuilder
            .record("Payload")
            .fields()
            .name("AB_CD")
            .`type`(SchemaBuilder.builder().stringType().makeNullable)
            .withDefault(null)
            .endRecord()
      )
    )
  }

  it should "support 'null' in the value" in {
    parse("""
            |{
            |  "state" : "PENDING",
            |  "device_id" : null,
            |  "timestamp" : 1610106404838
            |}""".stripMargin).map(
      json =>
        AvroSchema.inferRecord(json, "Payload", None) mustBe Right(
          new Schema.Parser().parse(
            """{
              |    "type": "record",
              |    "name": "Payload",
              |    "fields": [{
              |        "name": "device_id",
              |        "type": "null",
              |        "default": null
              |    }, {
              |        "name": "state",
              |        "type": ["null", "string"],
              |        "default": null
              |    }, {
              |        "name": "timestamp",
              |        "type": ["null", "long"],
              |        "default": null
              |    }]
              |}""".stripMargin
          )
      )
    )
  }

  it should "support floating point numbers" in {
    parse("""{
            |     "lat":12.949954,
            |     "long":79.13796
            |}""".stripMargin).map(
      json =>
        AvroSchema.inferRecord(json, "Payload", None) mustBe Right(
          new Schema.Parser().parse(
            """{
              |    "type": "record",
              |    "name": "Payload",
              |    "fields": [{
              |        "name": "lat",
              |        "type": ["null", "double"],
              |        "default": null
              |    }, {
              |        "name": "long",
              |        "type": ["null", "double"],
              |        "default": null
              |    }]
              |}""".stripMargin
          )
      )
    )
  }

}
