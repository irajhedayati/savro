lazy val scala212 = "2.12.12"
lazy val scala213 = "2.13.0"
lazy val supportedScalaVersions = List(scala212, scala213)

organization := "ca.dataedu"
name := "savro"
scalaVersion := scala212
crossScalaVersions := supportedScalaVersions

val AvroVersion = "1.9.0"
val CirceVersion = "0.13.0"

lazy val avro = Seq(
  ("org.apache.avro" % "avro"       % AvroVersion).exclude("com.fasterxml.jackson.core", "jackson-core"),
  ("org.apache.avro" % "avro-tools" % AvroVersion).exclude("com.fasterxml.jackson.core", "jackson-core")
)

val circe = Seq(
  "io.circe" %% "circe-core",
  "io.circe" %% "circe-generic",
  "io.circe" %% "circe-generic-extras",
  "io.circe" %% "circe-parser"
).map(_ % CirceVersion)

libraryDependencies ++= avro ++ circe
libraryDependencies += "org.scalatest" %% "scalatest" % "3.1.1" % Test
