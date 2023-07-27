package ca.dataedu.savro

import org.apache.avro.generic.GenericRecord
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters._

class ToAvroTest extends AnyFlatSpecLike with Matchers {

  import AvroImplicits._

  "convertObjectToMap" should "return a map" in {
    case class DataModel(numeric: Long, string: String, optional: Option[String])
    ToAvro.convertObjectToMap(DataModel(1234, "value", None)) shouldBe Map(
      "numeric" -> 1234L,
      "string" -> "value",
      "optional" -> null
    )
    ToAvro.convertObjectToMap(DataModel(1234, "value", Option("567"))) shouldBe Map(
      "numeric" -> 1234L,
      "string" -> "value",
      "optional" -> "567"
    )
  }

  it should "support nested objects" in {
    case class Outer(inner: Inner, field1: String)
    case class Inner(field1: String, field2: String)

    ToAvro.convertObjectToMap(Outer(Inner("a", "b"), "c")) shouldBe Map(
      "inner" -> Map("field1" -> "a", "field2" -> "b"),
      "field1" -> "c"
    )
  }

  it should "support array of objects" in {
    case class Outer(inner: List[Inner], field1: String)
    case class Inner(field1: String, field2: String)
    val output = ToAvro.convertObjectToMap(Outer(List(Inner("a", "b"), Inner("c", "d")), "e"))
    output shouldBe Map(
      "inner" -> List(Map("field1" -> "a", "field2" -> "b"), Map("field1" -> "c", "field2" -> "d")),
      "field1" -> "e"
    )

  }

  case class NestedRecord(field1: Option[String], field2: Boolean)
  case class AvroGeneratorTest(
      stringField: String,
      recordField: NestedRecord,
      arrayField: List[String],
      arrayOfRecords: List[NestedRecord],
      mapField: Map[String, Int]
  )

  "from" should "convert a case class to GenericRecord" in {
    val input = AvroGeneratorTest(
      "v1",
      NestedRecord(field1 = Option("v2"), field2 = true),
      List("a1", "a2"),
      List(NestedRecord(field1 = Option("b1"), field2 = false), NestedRecord(field1 = None, field2 = true)),
      Map("w1" -> 1, "w2" -> 2)
    )
    val schema = AvroSchema(
      """@namespace("ca.dataedu.avro")
        |protocol AvroSchemaConverter {
        |
        |  record AvroGeneratorTest {
        |    string stringField;
        |    NestedRecord recordField;
        |    array<string> arrayField;
        |    array<NestedRecord> arrayOfRecords;
        |    map<int> mapField;
        |    union { null, string } nullable_string = null;
        |  }
        |
        |  record NestedRecord {
        |    union { null, string } field1;
        |    boolean field2;
        |  }
        |
        |}""".stripMargin,
      "ca.dataedu.avro",
      "AvroGeneratorTest"
    )
    val output = ToAvro.from(input, schema)

    output.asString("stringField") shouldBe Option("v1")
    output.get("recordField").asInstanceOf[GenericRecord].get("field1") shouldBe "v2"
    output.get("recordField").asInstanceOf[GenericRecord].get("field2") shouldBe true
    output.get("arrayField") shouldBe List("a1", "a2").asJava
    val arrayOfRecords = output.get("arrayOfRecords").asInstanceOf[java.util.List[GenericRecord]]
    arrayOfRecords.get(0).get("field1") shouldBe "b1"
    arrayOfRecords.get(0).get("field2") shouldBe false
    arrayOfRecords.get(1).get("field1") shouldBe null
    arrayOfRecords.get(1).get("field2") shouldBe true
    output.get("mapField").asInstanceOf[java.util.Map[String, Integer]] shouldBe Map("w1" -> 1, "w2" -> 2).asJava
    output.asString("nullable_string") shouldBe None

  }

}
