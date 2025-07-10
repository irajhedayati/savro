import sbtrelease.ReleasePlugin.autoImport.ReleaseTransformations._
import sbtrelease.ReleasePlugin.autoImport._
import sbtrelease.Version

lazy val scala212 = "2.12.18"
lazy val scala213 = "2.13.11"
lazy val supportedScalaVersions = List(scala212, scala213)

organization := "ca.dataedu"
name := "savro"
scalaVersion := scala212
crossScalaVersions := supportedScalaVersions

val AvroVersion = "1.11.3"
val CirceVersion = "0.14.4"

lazy val avro = Seq(
  "org.apache.avro" % "avro",
  "org.apache.avro" % "avro-tools",
  "org.apache.avro" % "avro-compiler"
).map(_ % AvroVersion)

val circe = Seq(
  "io.circe" %% "circe-core",
  "io.circe" %% "circe-generic",
  "io.circe" %% "circe-generic-extras",
  "io.circe" %% "circe-parser"
).map(_ % CirceVersion)

libraryDependencies ++= avro ++ circe
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.19" % Test

/** Release related settings */
releaseCrossBuild := true
releaseVersionBump := Version.Bump.Minor
releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies, // check that there are no SNAPSHOT dependencies
  inquireVersions, // ask user to enter the current and next version
  runClean, // clean
  runTest, // run tests
  setReleaseVersion, // set release version in version.sbt
  commitReleaseVersion, // commit the release version
  tagRelease, // create git tag
  releaseStepCommandAndRemaining("+publishSigned"), // run +publishSigned command to sonatype stage release
  setNextVersion, // set next version in version.sbt
  commitNextVersion, // commit next version
  releaseStepCommand("sonatypeRelease"), // run sonatypeRelease and publish to maven central
  pushChanges // push changes to git
)
publishConfiguration := publishConfiguration.value.withOverwrite(true)
publishLocalConfiguration := publishLocalConfiguration.value.withOverwrite(true)

/** Sonatype release configuration */
inThisBuild(
  List(
    organization := "ca.dataedu",
    homepage := Some(url("https://savro.dataedu.ca")),
    licenses += ("Apache-2.0", url("https://www.apache.org/licenses/LICENSE-2.0")),
    developers := List(
      Developer(
        "irajhedayati",
        "Iraj Hedayati",
        "iraj.hedayati@gmail.com",
        url("https://www.dataedu.ca")
      )
    ),
    scmInfo := Some(ScmInfo(url("https://github.com/irajhedayati/savro"), "git@github.com:irajhedayati/savro.git"))
  )
)
