name := "graphene-spark"

version := "0.1"

scalaVersion := "2.11.12"

retrieveManaged := true

val SPARK_VERSION = "2.3.2"
val JACKSON_VERSION = "2.8.8"

resolvers += "Local Maven" at Path.userHome.asFile.toURI.toURL + ".m2/repository"

mainClass in assembly := Some("Main")

artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
  artifact.name + "_" + sv.binary + "-" + SPARK_VERSION + "_" + module.revision + "." + artifact.extension
}


dependencyOverrides ++= Seq(
  "com.fasterxml.jackson.core" % "jackson-core" % JACKSON_VERSION,
  "com.fasterxml.jackson.core" % "jackson-databind" % JACKSON_VERSION,
  "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % JACKSON_VERSION
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % SPARK_VERSION,
  "org.apache.spark" %% "spark-sql" % SPARK_VERSION,
  "org.lambda3.graphene" % "graphene-core" % "3.0.0-SNAPSHOT",
  "org.lambda3.graphene" % "graphene" % "3.0.0-SNAPSHOT"
)