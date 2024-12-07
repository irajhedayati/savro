package ca.dataedu.savro

import ca.dataedu.savro.AvroError.ToNumberError
import ca.dataedu.savro.AvroSchemaError.NonNullableUnionTypeError
import org.apache.avro.generic.GenericRecordBuilder
import org.apache.avro.{ Schema, SchemaBuilder }
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters._

class AvroImplicitsTest extends AnyFlatSpec with Matchers {

  import AvroImplicits._

  "AvroSchema.getNullableType" should "return the actual type from a nullable type" in {
    SchemaBuilder.builder.stringType.getTypeWithoutNull shouldBe Right(SchemaBuilder.builder.stringType)
    SchemaBuilder.nullable.stringType.getTypeWithoutNull shouldBe Right(SchemaBuilder.builder.stringType)
    Schema.createUnion(SchemaBuilder.builder.nullType).getTypeWithoutNull.left.toOption.get shouldBe
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
    inSchema.flat shouldBe Right(expected)
  }

  it should "Flatten the input Avro schema properly" in {
    val namespace = "ca.dataedu.avro"
    val inSchema = ResourceUtil.getResourceIdlAsSchema("AvroSchema.flat.input.avdl", namespace, "Message")
    val expected = ResourceUtil.getResourceIdlAsSchema("AvroSchema.flat.expected.avdl", namespace, "MessageFlatten")
    inSchema.flat shouldBe Right(expected)
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
    unionType.unionWithNonUnion(nonUnionType) shouldBe
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
    nullableString.getTypesWithoutNull shouldBe SchemaBuilder.builder().stringType()
  }

  it should "return the schema if it's not nullable" in {
    val notNullSchema = SchemaBuilder.builder().stringType()
    notNullSchema.getTypesWithoutNull shouldBe notNullSchema
  }

  behavior.of("Avro schema merger")

  it should "return same schema if both are equivalent" in {
    val a = SchemaBuilder.builder().stringType()
    val b = SchemaBuilder.builder().stringType()
    a.mergeWith(b) shouldBe a
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

    a.mergeWith(b) shouldBe b
    b.mergeWith(a) shouldBe b
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

    a.mergeWith(b) shouldBe b
    b.mergeWith(a) shouldBe b
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

    a.mergeWith(b) shouldBe merged
    b.mergeWith(a) shouldBe merged
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

    a.mergeWith(b) shouldBe a
    b.mergeWith(a) shouldBe b
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
      .array()
      .items()
      .nullType()
      .and()
      .record("Product")
      .fields()
      .name("AB_Experiment__Product_")
      .`type`(SchemaBuilder.builder().stringType().makeNullable)
      .withDefault(null)
      .name("Category_ID_Tree")
      .`type`(SchemaBuilder.builder().stringType().makeNullable)
      .withDefault(null)
      .endRecord()
      .endUnion()

    recordA.mergeWith(unionWithRecordA) shouldBe expected
    unionWithRecordA.mergeWith(recordA) shouldBe expected
  }

  it should "Array of null and array of nullable" in {
    val a = SchemaBuilder.array().items(SchemaBuilder.builder().nullType())
    val b = SchemaBuilder.array().items(SchemaBuilder.builder().stringType().makeNullable)

    a.mergeWith(b) shouldBe b
    b.mergeWith(a) shouldBe b
  }

  it should "Array with nonArray and not nullable and not union" in {
    val a = SchemaBuilder.array().items(SchemaBuilder.builder().stringType().makeNullable)
    val b = SchemaBuilder.builder().stringType()

    a.mergeWith(b) shouldBe SchemaBuilder.unionOf().`type`(a).and().`type`(b).endUnion()
    b.mergeWith(a) shouldBe SchemaBuilder.unionOf().`type`(b).and().`type`(a).endUnion()
  }

  it should "Array with nonArray and     nullable and not union" in {
    val a = SchemaBuilder.array().items(SchemaBuilder.builder().stringType().makeNullable)
    val b = SchemaBuilder.builder().nullType()

    a.mergeWith(b) shouldBe a.makeNullable
    b.mergeWith(a) shouldBe a.makeNullable
  }

  it should "Array with nonArray and     nullable and     union" in {
    val a = SchemaBuilder.array().items(SchemaBuilder.builder().stringType().makeNullable)
    val b = SchemaBuilder.unionOf().stringType().and().intType().endUnion().makeNullable
    val expected = Schema.createUnion((b.getTypesWithoutNull.getTypes.asScala :+ a).asJava).makeNullable

    a.mergeWith(b) shouldBe expected
  }

  it should "Array with nonArray and not nullable and     union" in {
    val a = SchemaBuilder.array().items(SchemaBuilder.builder().stringType().makeNullable)
    val b = SchemaBuilder.unionOf().stringType().and().intType().endUnion()
    val expected = Schema.createUnion((b.getTypes.asScala :+ a).asJava)

    a.mergeWith(b) shouldBe expected
  }

  behavior.of("addField")

  it should "add the field with default value" in {
    val schema = new Schema.Parser().parse(
      """{
        |  "type": "record",
        |  "name": "Person",
        |  "namespace": "ca.dataedu.avro",
        |  "fields": []
        |}""".stripMargin
    )
    val expected = new Schema.Parser().parse(
      """{
        |  "type": "record",
        |  "name": "Person",
        |  "namespace": "ca.dataedu.avro",
        |  "fields": [{
        |    "name": "phone",
        |    "type": "long",
        |    "default": 0
        |  }]
        |}""".stripMargin
    )
    schema.addField("phone", SchemaBuilder.builder().longType(), None, Option(0L)).toString() shouldBe expected
      .toString()

  }

  "updateSchema" should "update the schema by adding a new field" in {
    val schema = new Schema.Parser().parse(
      """{
        |  "type": "record",
        |  "name": "Person",
        |  "namespace": "ca.dataedu.avro",
        |  "fields": [{
        |    "name": "phone",
        |    "type": "long",
        |    "default": 0
        |  }]
        |}""".stripMargin
    )
    val newSchema = schema.addField("age", SchemaBuilder.builder.longType(), None, Option(0L))

    val input = new GenericRecordBuilder(schema).set("phone", 5141112222L).build()
    val expected = new GenericRecordBuilder(newSchema).set("phone", 5141112222L).set("age", 0L).build()

    input.updateSchema(newSchema) shouldBe Right(expected)
  }

  "copy" should "return a copy of the object respecting immutability" in {
    val schema = new Schema.Parser().parse(
      """{
        |  "type": "record",
        |  "name": "Person",
        |  "namespace": "ca.dataedu.avro",
        |  "fields": [
        |    { "name": "phone", "type": "long", "default": 0 },
        |    { "name": "address", "type": ["null","string"], "default": null }
        |  ]
        |}""".stripMargin
    )
    val original = new GenericRecordBuilder(schema).set("phone", 5141112222L).build()
    val copy = original.copy()
    copy.put("phone", 4381112222L)
    copy.get("phone") shouldBe 4381112222L // the copy object should be updated
    copy.get("address") shouldBe null
    original.get("phone") shouldBe 5141112222L // the original object should not be updated
  }

  "copy and set" should "return a copy of the object respecting immutability" in {
    val schema = new Schema.Parser().parse(
      """{
        |  "type": "record",
        |  "name": "Person",
        |  "namespace": "ca.dataedu.avro",
        |  "fields": [{
        |    "name": "phone",
        |    "type": "long",
        |    "default": 0
        |  }]
        |}""".stripMargin
    )
    val original = new GenericRecordBuilder(schema).set("phone", 5141112222L).build()
    val copy = original.copy("phone", 4381112222L)
    copy.get("phone") shouldBe 4381112222L // the copy object should be updated
    original.get("phone") shouldBe 5141112222L // the original object should not be updated
  }

  "asLong" should "return long value; value is provided and schema is long" in {
    val schema = new Schema.Parser().parse(
      """{
        |  "type": "record",
        |  "name": "Person",
        |  "namespace": "ca.dataedu.avro",
        |  "fields": [{
        |    "name": "phone",
        |    "type": "long",
        |    "default": 0
        |  }]
        |}""".stripMargin
    )
    val record = new GenericRecordBuilder(schema).set("phone", 5141112222L).build()
    record.asLong("phone") shouldBe Right(Option(5141112222L))
  }

  it should "return long value; value is provided and schema is int" in {
    val schema = new Schema.Parser().parse(
      """{
        |  "type": "record",
        |  "name": "Person",
        |  "namespace": "ca.dataedu.avro",
        |  "fields": [{
        |    "name": "age",
        |    "type": "int",
        |    "default": 0
        |  }]
        |}""".stripMargin
    )
    val record = new GenericRecordBuilder(schema).set("age", 40).build()
    record.asLong("age") shouldBe Right(Option(40L))
  }

  it should "return long value; value is provided and schema is string" in {
    val schema = new Schema.Parser().parse(
      """{
        |  "type": "record",
        |  "name": "Person",
        |  "namespace": "ca.dataedu.avro",
        |  "fields": [{
        |    "name": "phone",
        |    "type": "string",
        |    "default": "0"
        |  }]
        |}""".stripMargin
    )
    val record = new GenericRecordBuilder(schema).set("phone", "5141112222").build()
    record.asLong("phone") shouldBe Right(Option(5141112222L))
  }

  it should "return None if value is not provided" in {
    val schema = new Schema.Parser().parse(
      """{
        |  "type": "record",
        |  "name": "Person",
        |  "namespace": "ca.dataedu.avro",
        |  "fields": [{
        |    "name": "phone",
        |    "type": ["null", "string"],
        |    "default": null
        |  }]
        |}""".stripMargin
    )
    val record = new GenericRecordBuilder(schema).build()
    record.asLong("phone") shouldBe Right(None)
  }

  it should "return None if the field doesn't exist" in {
    val schema = new Schema.Parser().parse(
      """{
        |  "type": "record",
        |  "name": "Person",
        |  "namespace": "ca.dataedu.avro",
        |  "fields": [{
        |    "name": "phone",
        |    "type": ["null", "string"],
        |    "default": null
        |  }]
        |}""".stripMargin
    )
    val record = new GenericRecordBuilder(schema).set("phone", "5141112222").build()
    record.asLong("age") shouldBe Right(None)
  }

  it should "return error if schema is not long, int, and string" in {
    val schema = new Schema.Parser().parse(
      """{
        |  "type": "record",
        |  "name": "Person",
        |  "namespace": "ca.dataedu.avro",
        |  "fields": [{
        |    "name": "phone",
        |    "type": ["null", "boolean"],
        |    "default": null
        |  }]
        |}""".stripMargin
    )
    val record = new GenericRecordBuilder(schema).set("phone", true).build()
    record.asLong("phone") shouldBe Left(ToNumberError("true", "Field is not a supported type", None))
  }

  "Drop field from Avro schema" should "remove the field from schema" in {
    val inputFields = List(
      new Schema.Field("field1", SchemaBuilder.builder().stringType()),
      new Schema.Field("field2", SchemaBuilder.builder().stringType()),
      new Schema.Field("field3", SchemaBuilder.builder().stringType())
    ).asJava
    val expectedFields = List(
      new Schema.Field("field1", SchemaBuilder.builder().stringType()),
      new Schema.Field("field3", SchemaBuilder.builder().stringType())
    ).asJava
    val input = Schema.createRecord("Record1", "doc", "ca.dataedu", false, inputFields)
    val expected = Schema.createRecord("Record1", "doc", "ca.dataedu", false, expectedFields)

    (input - "field2") shouldBe expected
  }

  "Drop field from Avro record" should "remove the field from record" in {
    val inputFields = List(
      new Schema.Field("field1", SchemaBuilder.builder().stringType()),
      new Schema.Field("field2", SchemaBuilder.builder().stringType()),
      new Schema.Field("field3", SchemaBuilder.builder().stringType())
    ).asJava
    val expectedFields = List(
      new Schema.Field("field1", SchemaBuilder.builder().stringType()),
      new Schema.Field("field3", SchemaBuilder.builder().stringType())
    ).asJava

    val inputSchema = Schema.createRecord("Record1", "doc", "ca.dataedu", false, inputFields)
    val input = new GenericRecordBuilder(inputSchema)
      .set("field1", "value1")
      .set("field2", "value2")
      .set("field3", "value3")
      .build()

    val expectedSchema = Schema.createRecord("Record1", "doc", "ca.dataedu", false, expectedFields)
    val expected = new GenericRecordBuilder(expectedSchema)
      .set("field1", "value1")
      .set("field3", "value3")
      .build()

    input.drop("field2") shouldBe expected
  }

}
