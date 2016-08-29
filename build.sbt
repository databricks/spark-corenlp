// Your sbt build file. Guides on how to write one can be found at
// http://www.scala-sbt.org/0.13/docs/index.html

organization := "databricks"

name := "spark-corenlp"

version := "0.3-SNAPSHOT"

scalaVersion := "2.11.8"

initialize := {
  val _ = initialize.value
  val required = VersionNumber("1.8")
  val current = VersionNumber(sys.props("java.specification.version"))
  assert(VersionNumber.Strict.isCompatible(current, required), s"Java $required required.")
}

sparkVersion := "2.0.0"

// change the value below to change the directory where your zip artifact will be created
spDistDirectory := target.value

sparkComponents += "mllib"

// add any sparkPackageDependencies using sparkPackageDependencies.
// e.g. sparkPackageDependencies += "databricks/spark-avro:0.1"
spName := "databricks/spark-corenlp"

licenses := Seq("GPL-3.0" -> url("http://opensource.org/licenses/GPL-3.0"))

resolvers += Resolver.mavenLocal

libraryDependencies ++= Seq(
  "edu.stanford.nlp" % "stanford-corenlp" % "3.6.0",
  "com.google.protobuf" % "protobuf-java" % "2.6.1",
  "edu.stanford.nlp" % "stanford-corenlp" % "3.6.0" % "test" classifier "models",
  "org.scalatest" %% "scalatest" % "2.2.6" % "test"
)
