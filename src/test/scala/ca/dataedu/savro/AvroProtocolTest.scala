package ca.dataedu.savro

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers

class AvroProtocolTest extends AnyFlatSpec with Matchers {

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
    AvroProtocol(idl).toString(true) mustBe
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

}
