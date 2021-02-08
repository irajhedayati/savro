package ca.dataedu.savro

import org.apache.avro.Schema
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class AvroSchemaToIdlTest extends AnyFlatSpec with Matchers {

  "toIdl" should "set the default values properly" in {
    val schema = new Schema.Parser().parse(
      """{
        |  "type": "record",
        |  "name": "Person",
        |  "namespace": "ca.dataedu.avro",
        |  "fields": [{
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
    new AvroSchemaToIdl(schema, "AvroSchemaTool").convert() shouldBe
    """@namespace("ca.dataedu.avro")
        |protocol AvroSchemaTool {
        |  record Person {
        |    map<Address> addresses;
        |    string lastName;
        |    union { null, string } name = null;
        |    long phone = 0;
        |  }
        |
        |  record Address {
        |    string city;
        |    string street;
        |  }
        |
        |}
        |""".stripMargin
  }

}
