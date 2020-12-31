package ca.dataedu.savro

import java.io.ByteArrayInputStream

import org.apache.avro.Protocol
import org.apache.avro.compiler.idl.Idl

import scala.collection.JavaConverters._

object AvroProtocol {

  def apply(idl: String): Protocol = new Idl(new ByteArrayInputStream(idl.getBytes)).CompilationUnit()

  def toIdl(protocol: Protocol): String = {
    val recordsInString = protocol.getTypes.asScala
      .map(schema => new AvroSchemaToIdl(schema, "").convertJustRecord())
      .mkString("\n")
    s"""@namespace("${protocol.getNamespace}")
       |protocol ${protocol.getName} {$recordsInString
       |}
       |""".stripMargin
  }

}
