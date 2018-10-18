name := "graphene-spark"

version := "0.1"

scalaVersion := "2.11.12"

// resolvers += "The Graphene Extraction Tool" at "https://github.com/Lambda-3/Graphene"
resolvers += "Local Maven" at Path.userHome.asFile.toURI.toURL + ".m2/repository"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.3.2" % "provided",
  "org.lambda3.graphene" % "graphene-core" % "3.0.0-SNAPSHOT" % "provided",
  "org.lambda3.graphene" % "graphene" % "3.0.0-SNAPSHOT" % "provided",
  "org.lambda3.text.simplification" % "discourse-simplification" % "8.1.0" % "provided"
)