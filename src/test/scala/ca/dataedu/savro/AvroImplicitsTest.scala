package ca.dataedu.savro

import ca.dataedu.savro.AvroSchemaError.NonNullableUnionTypeError
import org.apache.avro.{ Schema, SchemaBuilder }
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers

import scala.collection.JavaConverters._

class AvroImplicitsTest extends AnyFlatSpec with Matchers {

  import AvroImplicits._

  "AvroSchema.getNullableType" should "return the actual type from a nullable type" in {
    SchemaBuilder.builder.stringType.getTypeWithoutNull mustBe Right(SchemaBuilder.builder.stringType)
    SchemaBuilder.nullable.stringType.getTypeWithoutNull mustBe Right(SchemaBuilder.builder.stringType)
    Schema.createUnion(SchemaBuilder.builder.nullType).getTypeWithoutNull.left.toOption.get mustBe
    a[NonNullableUnionTypeError]
  }

  behavior.of("flat")

  it should "Return flatten version of array of record that has array of record that has array of record" in {
    val inIdl =
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
    val inSchema = AvroProtocol(inIdl).getType("ca.dataedu.ActionMessage")
    val expected = AvroProtocol {
      """
        |@namespace("ca.dataedu")
        |protocol AvroSchemaTool {
        |  record ActionMessageFlatten {
        |    union { null, string } actionMessages_action = null;
        |    union { null, string } actionMessages_message = null;
        |  }
        |}
        |""".stripMargin
    }.getType("ca.dataedu.ActionMessageFlatten")
    inSchema.flat mustBe Right(expected)
  }

  it should "Flatten the input Avro schema properly" in {
    val namespace = "ca.dataedu.avro"
    val inSchema = ResourceUtil.getResourceIdlAsSchema("AvroSchema.flat.input.avdl", namespace, "Message")
    val expected = ResourceUtil.getResourceIdlAsSchema("AvroSchema.flat.expected.avdl", namespace, "MessageFlatten")
    inSchema.flat mustBe Right(expected)
  }

  "Union of union and non-Union" should "If non-Union is a record and a record of same type is in the list, should merge them" in {
    val unionType = SchemaBuilder
      .unionOf()
      .stringType()
      .and()
      .intType()
      .and()
      .record("Record")
      .fields()
      .endRecord()
      .endUnion()
    val nonUnionType = SchemaBuilder
      .record("Record")
      .fields()
      .name("filters")
      .`type`(Schema.create(Schema.Type.STRING).makeNullable)
      .withDefault(null)
      .name("type")
      .`type`(Schema.create(Schema.Type.STRING).makeNullable)
      .withDefault(null)
      .endRecord()
    unionType.unionWithNonUnion(nonUnionType) mustBe
    SchemaBuilder
      .unionOf()
      .stringType()
      .and()
      .intType()
      .and()
      .`type`(nonUnionType)
      .endUnion()
  }

  behavior.of("Avro schema extract types not null")
  it should "return the main schema of a nullable type" in {
    val nullableString = SchemaBuilder.unionOf().nullType().and().stringType().endUnion()
    nullableString.getTypesWithoutNull mustBe SchemaBuilder.builder().stringType()
  }
  it should "return the schema if it's not nullable" in {
    val notNullSchema = SchemaBuilder.builder().stringType()
    notNullSchema.getTypesWithoutNull mustBe notNullSchema
  }

  behavior.of("Avro schema merger")

  it should "return same schema if both are equivalent" in {
    val a = SchemaBuilder.builder().stringType()
    val b = SchemaBuilder.builder().stringType()
    a.mergeWith(b) mustBe a
  }

  it should "Two records when one has no fields" in {
    val a = SchemaBuilder.record("ConfigData").fields().endRecord()
    val b = SchemaBuilder
      .record("ConfigData")
      .fields()
      .name("daily_count")
      .`type`(SchemaBuilder.builder().stringType().makeNullable)
      .withDefault(null)
      .endRecord()

    a.mergeWith(b) mustBe b
    b.mergeWith(a) mustBe b
  }

  it should "Two nullable records when one has no fields" in {
    val a = SchemaBuilder.record("ConfigData").fields().endRecord().makeNullable
    val b = SchemaBuilder
      .record("ConfigData")
      .fields()
      .name("daily_count")
      .`type`(SchemaBuilder.builder().stringType().makeNullable)
      .withDefault(null)
      .endRecord()
      .makeNullable

    a.mergeWith(b) mustBe b
    b.mergeWith(a) mustBe b
  }

  it should "Two records with no matching fields" in {
    val a = SchemaBuilder
      .record("ConfigData")
      .fields()
      .name("monthly_count")
      .`type`(SchemaBuilder.builder().stringType().makeNullable)
      .withDefault(null)
      .endRecord()
    val b = SchemaBuilder
      .record("ConfigData")
      .fields()
      .name("daily_count")
      .`type`(SchemaBuilder.builder().stringType().makeNullable)
      .withDefault(null)
      .endRecord()

    val merged = SchemaBuilder
      .record("ConfigData")
      .fields()
      .name("daily_count")
      .`type`(SchemaBuilder.builder().stringType().makeNullable)
      .withDefault(null)
      .name("monthly_count")
      .`type`(SchemaBuilder.builder().stringType().makeNullable)
      .withDefault(null)
      .endRecord()

    a.mergeWith(b) mustBe merged
    b.mergeWith(a) mustBe merged
  }

  it should "two records of same fields without complex types" in {
    val a = SchemaBuilder
      .record("Actions")
      .fields()
      .name("ruleName")
      .`type`(SchemaBuilder.builder().stringType().makeNullable)
      .withDefault(null)
      .name("ruleStatus")
      .`type`(SchemaBuilder.builder().stringType().makeNullable)
      .withDefault(null)
      .name("extraOptions")
      .`type`(SchemaBuilder.builder().stringType().makeNullable)
      .withDefault(null)
      .name("children")
      .`type`(SchemaBuilder.builder().array().items(SchemaBuilder.builder().nullType()).makeNullable)
      .withDefault(null)
      .name("timeout")
      .`type`(SchemaBuilder.builder().booleanType().makeNullable)
      .withDefault(null)
      .name("ruleResult")
      .`type`(SchemaBuilder.builder().stringType().makeNullable)
      .withDefault(null)
      .name("tags")
      .`type`(SchemaBuilder.builder().array().items(SchemaBuilder.builder().nullType()).makeNullable)
      .withDefault(null)
      .endRecord()
    val b = SchemaBuilder
      .record("Actions")
      .fields()
      .name("ruleName")
      .`type`(SchemaBuilder.builder().stringType().makeNullable)
      .withDefault(null)
      .name("ruleStatus")
      .`type`(SchemaBuilder.builder().stringType().makeNullable)
      .withDefault(null)
      .name("extraOptions")
      .`type`(SchemaBuilder.builder().stringType().makeNullable)
      .withDefault(null)
      .name("children")
      .`type`(SchemaBuilder.builder().array().items(SchemaBuilder.builder().nullType()).makeNullable)
      .withDefault(null)
      .name("timeout")
      .`type`(SchemaBuilder.builder().booleanType().makeNullable)
      .withDefault(null)
      .name("ruleResult")
      .`type`(SchemaBuilder.builder().stringType().makeNullable)
      .withDefault(null)
      .name("tags")
      .`type`(SchemaBuilder.builder().array().items(SchemaBuilder.builder().nullType()).makeNullable)
      .withDefault(null)
      .endRecord()

    a.mergeWith(b) mustBe a
    b.mergeWith(a) mustBe b
  }

  it should "A record with a union where union has a record of same type" in {
    val recordA = SchemaBuilder
      .record("Product")
      .fields()
      .name("Category_ID_Tree")
      .`type`(SchemaBuilder.builder().stringType().makeNullable)
      .noDefault()
      .endRecord()
    val unionWithRecordA = SchemaBuilder
      .unionOf()
      .record("Product")
      .fields()
      .name("AB_Experiment__Product_")
      .`type`(SchemaBuilder.builder().stringType().makeNullable)
      .noDefault()
      .endRecord()
      .and()
      .array()
      .items()
      .nullType()
      .endUnion()

    val expected = SchemaBuilder
      .unionOf()
      .record("Product")
      .fields()
      .name("AB_Experiment__Product_")
      .`type`(SchemaBuilder.builder().stringType().makeNullable)
      .withDefault(null)
      .name("Category_ID_Tree")
      .`type`(SchemaBuilder.builder().stringType().makeNullable)
      .withDefault(null)
      .endRecord()
      .and()
      .array()
      .items()
      .nullType()
      .endUnion()

    recordA.mergeWith(unionWithRecordA) mustBe expected
    unionWithRecordA.mergeWith(recordA) mustBe expected
  }

  it should "Array of null and array of nullable" in {
    val a = SchemaBuilder.array().items(SchemaBuilder.builder().nullType())
    val b = SchemaBuilder.array().items(SchemaBuilder.builder().stringType().makeNullable)

    a.mergeWith(b) mustBe b
    b.mergeWith(a) mustBe b
  }

  it should "Array with nonArray and not nullable and not union" in {
    val a = SchemaBuilder.array().items(SchemaBuilder.builder().stringType().makeNullable)
    val b = SchemaBuilder.builder().stringType()

    a.mergeWith(b) mustBe SchemaBuilder.unionOf().`type`(a).and().`type`(b).endUnion()
    b.mergeWith(a) mustBe SchemaBuilder.unionOf().`type`(b).and().`type`(a).endUnion()
  }

  it should "Array with nonArray and     nullable and not union" in {
    val a = SchemaBuilder.array().items(SchemaBuilder.builder().stringType().makeNullable)
    val b = SchemaBuilder.builder().nullType()

    a.mergeWith(b) mustBe a.makeNullable
    b.mergeWith(a) mustBe a.makeNullable
  }

  it should "Array with nonArray and     nullable and     union" in {
    val a = SchemaBuilder.array().items(SchemaBuilder.builder().stringType().makeNullable)
    val b = SchemaBuilder.unionOf().stringType().and().intType().endUnion().makeNullable
    val expected = Schema.createUnion((Seq(a) ++ b.getTypesWithoutNull.getTypes.asScala).asJava).makeNullable
    val expectedReverted = Schema.createUnion((b.getTypesWithoutNull.getTypes.asScala ++ Seq(a)).asJava).makeNullable

    a.mergeWith(b) mustBe expected
    b.mergeWith(a) mustBe expectedReverted
  }

  it should "Array with nonArray and not nullable and     union" in {
    val a = SchemaBuilder.array().items(SchemaBuilder.builder().stringType().makeNullable)
    val b = SchemaBuilder.unionOf().stringType().and().intType().endUnion()
    val expected = Schema.createUnion((Seq(a) ++ b.getTypes.asScala).asJava)
    val expectedReverted = Schema.createUnion((b.getTypes.asScala ++ Seq(a)).asJava)

    a.mergeWith(b) mustBe expected
    b.mergeWith(a) mustBe expectedReverted
  }
}
