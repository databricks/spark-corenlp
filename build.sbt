// Your sbt build file. Guides on how to write one can be found at
// http://www.scala-sbt.org/0.13/docs/index.html

organization := "databricks"

name := "spark-corenlp"

version := "0.3.0-SNAPSHOT"

scalaVersion := "2.11.8"

scalacOptions ++= Seq(
  "-encoding", "UTF-8",
  "-unchecked",
  "-deprecation",
  "-Xfuture",
  "-Yno-adapted-args",
  "-Ywarn-dead-code",
  "-Ywarn-numeric-widen",
  "-Ywarn-value-discard",
  "-Ywarn-dead-code",
  "-feature")

initialize := {
  val _ = initialize.value
  val required = VersionNumber("1.8")
  val current = VersionNumber(sys.props("java.specification.version"))
  assert(VersionNumber.Strict.isCompatible(current, required), s"Java $required required.")
}

lazy val nlpVersion = "3.7.0"
sparkVersion := "2.3.1"

// change the value below to change the directory where your zip artifact will be created
spDistDirectory := target.value

spAppendScalaVersion := true

credentials += Credentials(Path.userHome / ".ivy2" / ".sbtcredentials")

sparkComponents += "sql"

// add any sparkPackageDependencies using sparkPackageDependencies.
// e.g. sparkPackageDependencies += "databricks/spark-avro:0.1"
spName := "databricks/spark-corenlp"

licenses := Seq("GPL-3.0" -> url("http://opensource.org/licenses/GPL-3.0"))

resolvers += Resolver.mavenLocal

libraryDependencies ++= Seq(
  "edu.stanford.nlp" % "stanford-corenlp" % nlpVersion,
  "com.google.protobuf" % "protobuf-java" % "3.2.0",
  "edu.stanford.nlp" % "stanford-corenlp" % nlpVersion % "test" classifier "models",
  "org.scalatest" %% "scalatest" % "3.0.1" % "test"
)
