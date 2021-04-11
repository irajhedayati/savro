package ca.dataedu.savro

import com.fasterxml.jackson.databind.{ JsonNode, MapperFeature, ObjectMapper }
import org.apache.avro.Schema
import org.apache.avro.compiler.idl.Idl

import scala.io.Source

object ResourceUtil {

  private val objectMapper = new ObjectMapper().enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS)

  def getResourceAsSchema(path: String): Schema = new Schema.Parser().parse(getResourceAsString(path))

  def getResourceIdlAsSchema(path: String, namespace: String, objectName: String): Schema =
    new Idl(Source.fromResource(path).bufferedReader()).CompilationUnit.getType(s"$namespace.$objectName")

  def getResourceAsJson(path: String): JsonNode = objectMapper.readTree(getResourceAsString(path))

  def getResourceAsString(path: String): String = Source.fromResource(path).getLines().mkString(System.lineSeparator())

}
