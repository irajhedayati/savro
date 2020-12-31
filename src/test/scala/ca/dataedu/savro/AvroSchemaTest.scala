package ca.dataedu.savro

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

  "Avro IDL" should "generate proper schema" in {
    val idl =
      """
        |@namespace("ca.dataedu")
        |protocol AvroSchemaTool {
        |
        |  record TestObject {
        |    TestObject2 name;
        |  }
        |  record TestObject2 {
        |    int age;
        |  }
        |
        |}
        |""".stripMargin
    AvroSchema(idl) mustBe
    """{
        |  "protocol" : "AvroSchemaTool",
        |  "namespace" : "ca.dataedu",
        |  "types" : [ {
        |    "type" : "record",
        |    "name" : "TestObject",
        |    "fields" : [ {
        |      "name" : "name",
        |      "type" : {
        |        "type" : "record",
        |        "name" : "TestObject2",
        |        "fields" : [ {
        |          "name" : "age",
        |          "type" : "int"
        |        } ]
        |      }
        |    } ]
        |  } ],
        |  "messages" : { }
        |}""".stripMargin
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
            .`type`(Schema.create(Schema.Type.STRING).makeNullable)
            .withDefault(null)
            .name("screenName")
            .`type`(Schema.create(Schema.Type.STRING).makeNullable)
            .withDefault(null)
            .endRecord()
      )
    )
  }

}
