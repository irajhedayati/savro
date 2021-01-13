**SAvro** is a set of tools to work with [Apache Avro](https://avro.apache.org) 
in a Scala project. The library is compiled with Scala `2.12` and `2.13` 
binaries.

In order to add the library to your projects,

```sbt
libraryDependencies += "ca.dataedu" %% "savro" % "0.1.0"
```

## Infer Avro schema

It was the first functionality that led to development of this library.

In order to try this function, visit http://www.dataedu.ca/avro.

It is common that you need to design an Avro schema that matches with your 
existing data of the data that you will ingest. This tool accepts a single JSON
object, or an array of objects and infers the most compatible Avro schema. By
"the most compatible Avro schema" it means that all the fields are going to
be optional. The reason is in JSON object, a field could be missing, and you
don't have it in your sample data, or the field could be present in your sample
data and not in the actual data.

```scala
import ca.dataedu.savro._
import org.apache.avro.Schema

val jsonValue: String = ???
val schema: Either[SAvroError, Schema] = AvroSchema(jsonValue, "", Option(""))
```

or alternatively

```scala
import ca.dataedu.savro._
import io.circe.Json
import org.apache.avro.Schema

val jsonValue: Json = ???
val schema: Either[SAvroError, Schema] = AvroSchema(jsonValue, "", Option(""))
```

### Infer the schema from single object

For example, if the input is

```json
{
    "firstname": "John",
    "lastname": "Doe"
}
```

the schema is

```json
{
    "type": "record",
    "name": "TestObject",
    "namespace": "ca.dataedu",
    "fields": [{
        "name": "firstname",
        "type": ["null", "string"],
        "doc": "",
        "default": null
    }, {
        "name": "lastname",
        "type": ["null", "string"],
        "doc": "",
        "default": null
    }]
}
```

### Infer the schema from an array of objects

A more accurate schema is generated if you provide a series of JSON values. It 
will create a schema that accepts all the input records. If a field appears in 
different types, it will use union.

For example, if the input is

```json
[
  {
    "firstname": "John"
  },
  {
    "lastname": "Doe",
    "age": 20    
  },
  {
    "firstname": "Joe",
    "age": "20" 
  }
]
```

the schema is

```json
{
    "type": "record",
    "name": "TestObject",
    "namespace": "ca.dataedu",
    "fields": [{
        "name": "age",
        "type": ["null", "int", "string"],
        "default": null
    }, {
        "name": "firstname",
        "type": ["null", "string"],
        "default": null
    }, {
        "name": "lastname",
        "type": ["null", "string"],
        "default": null
    }]
}
```

## Avro IDL

[Avro IDL](https://avro.apache.org/docs/1.9.2/idl.html) is a higher level
language for authoring Avro schema. It is indeed easier to generate or modify an
schema. Hence, there are methods to alternatively convert Avro schema to Avro
IDL and vice versa.

### Avro IDL to Avro Schema

Having Avro IDL in string format,

```scala
import ca.dataedu.savro._
import org.apache.avro.Schema
  
val idl: String = ???
val namespace: String = "ca.dataedu"
val recordName: String = "Person"
val schema: Schema = AvroSchema(idl, namespace, recordName)
```

### Avro Schema to Avro IDL

You also can convert an Avro schema to an Avro IDL. 

```scala
import ca.dataedu.savro._
import org.apache.avro.Schema

val schema: Schema = ???
val idl: String = new AvroSchemaToIdl(schema, "ProtocolName").convert()
```

Alternatively, you can use the implicits

```scala
import ca.dataedu.savro.AvroImplicits._
import ca.dataedu.savro.AvroSchemaError.IllegalOperationError
import org.apache.avro.Schema

val schema: Schema = ???
val idl: Either[IllegalOperationError, String] = schema.toIdl("ProtocolName")
```

## Implicits

In order to improve the experience of working with Avro library, a set of
implicits is added in the library.

```scala
import ca.dataedu.savro.AvroImplicits._
```

### Schema Field

**Schema comparison**
In order to check if two fields have the same schema. The comparison is done
without considering the `NULL` type.

```scala
import ca.dataedu.savro.AvroImplicits._
import org.apache.avro.Schema.Field

val aField: Field = ???
val anotherField: Field = ???
aField.hasSameSchema(anotherField)
```

| `aField` | `anotherField` | result|
|:---:|:---:|:---|
| String | String | `true` |
| String | Optional String | `true` |
| Optional String | String | `true` |
| Optional String | Optional String | `true` |
| String | Integer | `false` |
| String | Optional Integer | `false` |
| Optional String | Integer | `false` |
| Optional String | Optional Integer | `false` |


### Avro schema

**Flatten**
The Avro schema supports complex types such as record and array. This function
will return the flatten version. For more information, check the Scala doc of 
the function.

For example, having
```avroidl
@namespace("ca.dataedu")
protocol AvroSchemaTool {
  record Person {
    Name name;
    array<Phone> phones;
  }
  record Phone {
    string number;
    string type;
  }
  record Name {
    string first;
    string last;
  }
}
```

the flatten version would be

```avroidl
@namespace("ca.dataedu")
protocol AvroSchemaTool {
  record PersonFlatten {
    string name_first;
    string name_last;
    string phone_number;
    string phone_type;
  }
}
```

Note that in terms of array, we expect that the records explode.

**Strip NULL type**
The optional (nullable) data type in Avro is represented by a union of `NULL`
and the actual type. In order to get the actual type from a schema, this method 
could be useful.

```scala
import ca.dataedu.savro.AvroImplicits._
import org.apache.avro.Schema

val nullableSchema: Schema = ???
val schemaWithoutNull: Schema = nullableSchema.getTypesWithoutNull
```

Note that this function supports an actual union of multiple types. For example,
- if the schema is union of `NULL`, `STRING` and `INT`, it will return a
 union of `STRING` and `INT`.
- if the schema is union of `NULL` and `INT`, it will return `INT`.
- if the schema is union of `STRING` and `INT`, it will return a
 union of `STRING` and `INT`.
- if the schema is just `INT`, it will return `INT`.
 
Another version of this function can be used when we expect one type expect
 `NULL` which is a more common case.

```scala
import ca.dataedu.savro.AvroImplicits._
import ca.dataedu.savro.AvroSchemaError._
import org.apache.avro.Schema

val nullableSchema: Schema = ???
val schemaWithoutNull: Either[NonNullableUnionTypeError, Schema] = 
  nullableSchema.getTypeWithoutNull
```

This function works the same except if the schema is union of `NULL` and more
than one type, it will return an error.

**Make nullable**
If you have an Avro schema, and you'd like to make sure that it is nullable, you
can use this method. Normally making a schema nullable needs a verbose code
such as,

```scala
import org.apache.avro.SchemaBuilder
import org.apache.avro.Schema

val schema: Schema = ???
SchemaBuilder.builder().unionOf().nullType().and().`type`(schema).endUnion()
```

Moreover, you need to make sure that it is not nullable because above code will
throw an error having `NULL` type repeated more than once. Also, if your schema
is already a union, above code will fail as well.

A simple solution is to use the implicit:

```scala
import ca.dataedu.savro.AvroImplicits._
import org.apache.avro.Schema

val schema: Schema = ???
schema.makeNullable
```

**Add a field**

```scala
import ca.dataedu.savro.AvroImplicits._
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder

val schema: Schema = ???
schema.addField("newField1", SchemaBuilder.builder().stringType())
schema.addField(
 "newField2", 
 SchemaBuilder.builder().stringType(),
 Option("Ths implicit function used to add new field to an schema")
)
schema.addField(
 "newField3", 
 SchemaBuilder.builder().stringType(),
 Option("Ths implicit function used to add new field to an schema"),
 Option("Default Value")
)
```

**Some tests**

```scala
import ca.dataedu.savro.AvroImplicits._
import org.apache.avro.Schema

val schema: Schema = ???
schema.isRecord
schema.isArray
```

**Make union of two schemas**
Creating unions from two schemas is not easy with Avro API. With this
 implicits, all the complexities is taken away.
 
```scala
import ca.dataedu.savro.AvroImplicits._
import org.apache.avro.Schema

val schema1: Schema = ???
val schema2: Schema = ???
schema1.union(schema2)
```

There are two other functions related to making union of two schema but both of
them are variants of this one. It is recommended to simply use the above
-mentioned function. 

### Avro (GenericRecord)

Working with Java objects in Scala are sometimes verbose and frustrating. These
implicits will make it easier.

```scala
import ca.dataedu.savro.AvroImplicits._
import ca.dataedu.savro._
import org.apache.avro.generic.GenericRecord

val avroMessage: GenericRecord = ???
// To get the value of a field as string
val stringValue: Option[String] = avroMessage.asString("fieldName")
// In order to get the value of a field as a number
val longValue: Either[SAvroError, Option[Long]] = 
  avroMessage.asLong("fieldName")
val intValue: Either[SAvroError, Option[Int]] = avroMessage.asInt("fieldName")
val doubleValue: Either[SAvroError, Option[Double]] = 
  avroMessage.asDouble("fieldName")
// To get the value as boolean
val booleanValue: Either[SAvroError, Option[Boolean]] = 
  avroMessage.asBoolean("fieldName")
// OR implement a custom field extractor using `as[T]` function
```

Another useful function is `set` which helps you to set the value of a field.
