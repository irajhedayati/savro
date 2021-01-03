import sbtrelease.ReleasePlugin.autoImport._
import sbtrelease.ReleasePlugin.autoImport.ReleaseTransformations._
import sbtrelease.Version

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

/** Release related settings */
commands ++= Seq(tagReleaseVersion, bumpVersion)
def releaseCommand(cmdName: String, settings: Seq[Def.Setting[_]]): Command =
  Command.command(cmdName)((state: State) => {
    val extracted = Project.extract(state)
    val customState = extracted.appendWithSession(settings, state)
    Command.process("release with-defaults", customState)
  })
lazy val tagReleaseSettings = Seq(
  releaseProcess := Seq(inquireVersions, setReleaseVersion, commitReleaseVersion, tagRelease, pushChanges)
)
lazy val bumpVersionSettings = Seq(
  releaseVersionBump := Version.Bump.Minor,
  releaseProcess := Seq(inquireVersions, setNextVersion, commitNextVersion)
)
lazy val tagReleaseVersion = releaseCommand("tagReleaseVersion", tagReleaseSettings)
lazy val bumpVersion = releaseCommand("bumpVersion", bumpVersionSettings)
