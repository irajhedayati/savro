package ca.dataedu.savro

import io.circe.parser._
import org.apache.avro.SchemaBuilder
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers._

class AvroSchemaTest extends AnyFlatSpec {

  import AvroImplicits._

  "Avro schema" should "Make the input schema nullable" in {
    val result = SchemaBuilder.builder().stringType().makeNullable
    result.isUnion mustBe true
    result.isNullable mustBe true
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
          SchemaBuilder
            .record("Payload")
            .fields()
            .name("device_id")
            .`type`(SchemaBuilder.builder().nullType())
            .withDefault(null)
            .name("state")
            .`type`(SchemaBuilder.builder().stringType().makeNullable)
            .withDefault(null)
            .name("timestamp")
            .`type`(SchemaBuilder.builder().longType().makeNullable)
            .withDefault(null)
            .endRecord()
      )
    )
  }

}
