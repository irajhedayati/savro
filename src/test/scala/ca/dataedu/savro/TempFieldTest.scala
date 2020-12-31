package ca.dataedu.savro

import org.apache.avro.{ JsonProperties, SchemaBuilder }
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers

class TempFieldTest extends AnyFlatSpec with Matchers {

  import AvroImplicits._

  behavior.of("flatten")

  /* ~~~~~~~~~~~~~ Primitive test cases */

  it should "Return the field if string" in {
    val in = TempField("StringField", SchemaBuilder.builder().stringType(), "", "")
    in.flatten mustBe Right(List(in))
  }

  it should "Return the field if integer" in {
    val in = TempField("IntField", SchemaBuilder.builder().intType(), "", 0)
    in.flatten mustBe Right(List(in))
  }

  it should "Return the field if long" in {
    val in = TempField("LongField", SchemaBuilder.builder().longType(), "", 0)
    in.flatten mustBe Right(List(in))
  }

  it should "Return the field if boolean" in {
    val in = TempField("BooleanField", SchemaBuilder.builder().booleanType(), "", true)
    in.flatten mustBe Right(List(in))
  }

  it should "Return the field if double" in {
    val in = TempField("DoubleField", SchemaBuilder.builder().doubleType(), "", 0)
    in.flatten mustBe Right(List(in))
  }

  it should "Return the field if float" in {
    val in = TempField("FloatField", SchemaBuilder.builder().floatType(), "", 0)
    in.flatten mustBe Right(List(in))
  }

  it should "Return the same union field for primitive nullable" in {
    val in = TempField("StringField", SchemaBuilder.unionOf().nullType().and().stringType().endUnion(), "", null)
    in.flatten mustBe Right(List(in))
  }

  /* ~~~~~~~~~~~~~ Array test cases */
  private val NullValue = JsonProperties.NULL_VALUE
  it should "Return the nullable element type of array of non-optional primitive with default 'null'" in {
    val fieldName = "ArrayOfNonOptionalStringField"
    val in = TempField(fieldName, SchemaBuilder.array().items().stringType(), "", "")
    val expected = TempField(fieldName, SchemaBuilder.builder().stringType().makeNullable, "", NullValue)
    in.flatten mustBe Right(List(expected))
  }

  it should "Return the nullable element type of optional array of non-optional primitive with default 'null'" in {
    val fieldName = "OptionalArrayOfNonOptionalStringField"
    val in = TempField(fieldName, SchemaBuilder.array().items().stringType().makeNullable, "", "")
    val expected = TempField(fieldName, SchemaBuilder.builder().stringType().makeNullable, "", NullValue)
    in.flatten mustBe Right(List(expected))
  }

  it should "Return the nullable element type of array of optional primitive with default 'null'" in {
    val fieldName = "ArrayOfOptionalStringField"
    val in = TempField(fieldName, SchemaBuilder.array().items().nullable().stringType(), "", NullValue)
    val expected = TempField(fieldName, SchemaBuilder.builder().stringType().makeNullable, "", NullValue)
    in.flatten mustBe Right(List(expected))
  }

  it should "Return the nullable element type of optional array of optional primitive with default 'null'" in {
    val fieldName = "OptionalArrayOfOptionalStringField"
    val in = TempField(fieldName, SchemaBuilder.array().items().nullable().stringType().makeNullable, "", NullValue)
    val expected = TempField(fieldName, SchemaBuilder.builder().stringType().makeNullable, "", NullValue)
    in.flatten mustBe Right(List(expected))
  }

  /* ~~~~~~~~~~~~~ Record test cases */

  it should "Return the list of primitive fields with the parent field name as a prefix" in {
    val idl =
      """
        |@namespace("ca.dataedu")
        |protocol AvroSchemaTool {
        |  record Person {
        |    string name;
        |    int age;
        |  }
        |}
        |""".stripMargin
    val inSchema = AvroProtocol(idl).getType("ca.dataedu.Person")
    val in = TempField("FlatRecord", inSchema, "", null)
    val expected = List(
      TempField("FlatRecord_name", SchemaBuilder.builder().stringType(), null, null),
      TempField("FlatRecord_age", SchemaBuilder.builder().intType(), null, null)
    )
    in.flatten mustBe Right(expected)
  }

  it should "Return the list of nested record fields with the parent field name as a prefix" in {
    val idl =
      """
        |@namespace("ca.dataedu")
        |protocol AvroSchemaTool {
        |  record Person {
        |    string name;
        |    int age;
        |    Address address;
        |  }
        |  record Address {
        |    string phone;
        |    int streetNumber;
        |  }
        |}
        |""".stripMargin
    val inSchema = AvroProtocol(idl).getType("ca.dataedu.Person")
    val in = TempField("FlatRecord", inSchema, "", null)
    val expected = List(
      TempField("FlatRecord_name", SchemaBuilder.builder().stringType(), null, null),
      TempField("FlatRecord_age", SchemaBuilder.builder().intType(), null, null),
      TempField("FlatRecord_address_phone", SchemaBuilder.builder().stringType(), null, null),
      TempField("FlatRecord_address_streetNumber", SchemaBuilder.builder().intType(), null, null)
    )
    in.flatten mustBe Right(expected)
  }

  it should "Return fields of array of record that has array of record that has array of record" in {
    val idl =
      """
        |@namespace("ca.dataedu")
        |protocol AvroSchemaTool {
        |  record Message {
        |    array<Action> actions;
        |  }
        |  record Action {
        |    array<union { ActionMessage, null }> actionMessagesList;
        |  }
        |  record ActionMessage {
        |    array<union { IndividualActionMessage, null }> actionMessages;
        |  }
        |  record IndividualActionMessage {
        |    union { null, string } action = null;
        |    union { null, string } message = null;
        |  }
        |}
        |""".stripMargin
    val inSchema = AvroProtocol(idl).getType("ca.dataedu.Action")
    val in = TempField("actions", SchemaBuilder.array().items(inSchema), "", null)
    val nullableString = SchemaBuilder.builder().stringType().makeNullable
    val expected = List(
      TempField("actions_actionMessagesList_actionMessages_action", nullableString, null, NullValue),
      TempField("actions_actionMessagesList_actionMessages_message", nullableString, null, NullValue)
    )
    in.flatten mustBe Right(expected)
  }
}
