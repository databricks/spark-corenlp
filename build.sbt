import ReleaseTransformations._

lazy val commonSettings = Seq(
  organization := "databricks",
  name := "spark-corenlp",
  spName := "databricks/spark-corenlp",
  licenses := Seq("GPL-3.0" -> url("http://opensource.org/licenses/GPL-3.0")),
  // dependency settings //
  scalaVersion := "2.11.8",
  sparkVersion := "2.3.1",
  initialize := {
    val _ = initialize.value
    // require Java 8+
    val required = VersionNumber("1.8")
    val current = VersionNumber(sys.props("java.specification.version"))
    assert(VersionNumber.Strict.isCompatible(current, required), s"Java $required required.")
  },
  sparkComponents += "sql",
  resolvers += Resolver.mavenLocal,
  // test settings //
  fork in Test := true,
  javaOptions in Test ++= Seq("-Xmx6g"),
  // release settings //
  spAppendScalaVersion := true,
  // We only use sbt-release to update version numbers for now.
  releaseProcess := Seq[ReleaseStep](
    inquireVersions,
    setReleaseVersion,
    commitReleaseVersion,
    tagRelease,
    setNextVersion,
    commitNextVersion
  ),
  credentials += Credentials(Path.userHome / ".ivy2" / ".sbtcredentials")
)

lazy val nlpVersion = "3.9.1"
lazy val dependenciesToShade = Seq(
  ("edu.stanford.nlp" % "stanford-corenlp" % nlpVersion)
    .exclude("joda-time", "joda-time") // provided by Spark
    .exclude("org.apache.commons", "commons-lang3") // provided by Spark
    .exclude("javax.servlet", "javax.servlet-api") // provided by Spark
    .exclude("org.slf4j", "slf4j-api") // provided by Spark
)
lazy val testDependencies = Seq(
  "edu.stanford.nlp" % "stanford-corenlp" % nlpVersion % "test" classifier "models",
  "org.scalatest" %% "scalatest" % "3.0.1" % "test"
)

// the default subproject with shading
lazy val root = project.in(file(".")).settings(
  commonSettings,
  libraryDependencies ++= (dependenciesToShade ++ testDependencies),
  assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false),
  assemblyShadeRules in assembly := Seq(
    // We only shade protobuf-java here.
    // Unfortunately, we cannot shade stanford-corenlp because it seems using reflection.
    ShadeRule.rename("com.google.protobuf.**" -> "com.databricks.spark.corenlp.shaded.@0").inAll
  ),
  test in assembly := {},
  spDist := sys.error("Use 'sbt distribution/*' instead.")
)

// a subproject for release, where we use the assembly jar and declare no dependencies
lazy val distribution = project.settings(
  commonSettings,
  target := target.value / "distribution",
  libraryDependencies ++= testDependencies,
  spShade := true,
  assembly in spPackage := (assembly in root).value
)
