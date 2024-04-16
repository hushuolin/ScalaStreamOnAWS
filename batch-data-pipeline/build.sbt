ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.7"

lazy val root = (project in file("."))
  .settings(
    name := "batch-data-pipeline"
  )

libraryDependencies ++= Seq(
  "com.softwaremill.sttp.client3" %% "core" % "3.5.2",  // sttp core library
  "com.softwaremill.sttp.client3" %% "circe" % "3.5.2", // if you use circe for JSON
  "com.lihaoyi" %% "ujson" % "1.4.0",  // ujson for JSON parsing
  "org.apache.spark" %% "spark-core" % "3.5.0", // Use the Spark version installed on your device
  "org.apache.spark" %% "spark-sql" % "3.5.0"
)

