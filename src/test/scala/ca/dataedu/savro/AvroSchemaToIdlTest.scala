package ca.dataedu.savro

import org.apache.avro.Schema
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers

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
        |  }]
        |}""".stripMargin
    )
    new AvroSchemaToIdl(schema, "AvroSchemaTool").convert() mustBe
    """@namespace("ca.dataedu.avro")
        |protocol AvroSchemaTool {
        |  record Person {
        |    string lastName;
        |    union { null, string } name = null;
        |    long phone = 0;
        |  }
        |
        |}
        |""".stripMargin
  }

}
