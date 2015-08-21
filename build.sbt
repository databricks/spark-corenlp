// Your sbt build file. Guides on how to write one can be found at
// http://www.scala-sbt.org/0.13/docs/index.html

scalaVersion := "2.10.4"

sparkVersion := "1.4.1"

// change the value below to change the directory where your zip artifact will be created
spDistDirectory := target.value

sparkComponents += "mllib"

// add any sparkPackageDependencies using sparkPackageDependencies.
// e.g. sparkPackageDependencies += "databricks/spark-avro:0.1"
sparkPackageName := "databricks/spark-corenlp"

licenses := Seq("GPL-3.0" -> url("http://opensource.org/licenses/GPL-3.0"))

libraryDependencies += "edu.stanford.nlp" % "stanford-corenlp" % "3.5.2"
