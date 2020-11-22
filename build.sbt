name := "streaming-lib"

version := "0.1"

scalaVersion := "2.12.12"

artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
  artifact.name + "_" + sv.binary + "-2.4.6" + "_" + module.revision + "." + artifact.extension
}

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.6" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.4.6" % "provided"
libraryDependencies += "com.github.scopt" %% "scopt" % "3.7.0"
libraryDependencies += "org.apache.spark" %% "spark-avro" % "2.4.6" % "provided"
libraryDependencies += "io.circe" %% "circe-core" % "0.11.2"
libraryDependencies += "io.circe" %% "circe-generic" % "0.11.2"
libraryDependencies += "io.circe" %% "circe-parser" % "0.11.2"
libraryDependencies += "io.circe" %% "circe-optics" % "0.11.0"
libraryDependencies += "org.scala-lang" % "scala-reflect" % "2.12.12"

// Testing
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.1" % Test
libraryDependencies += "org.scalactic" %% "scalactic" % "3.2.1"
libraryDependencies += "org.scalatestplus" %% "mockito-3-4" % "3.2.2.0" % "test"