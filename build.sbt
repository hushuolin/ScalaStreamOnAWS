ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.7"

lazy val root = (project in file("."))
  .settings(
    name := "batch-data-pipeline"
  )

libraryDependencies ++= Seq(
  "com.softwaremill.sttp.client3" %% "core" % "3.9.5",  // sttp core library
  "com.softwaremill.sttp.client3" %% "circe" % "3.9.5", // if you use circe for JSON
  "com.lihaoyi" %% "ujson" % "1.4.0",  // ujson for JSON parsing
  "org.apache.spark" %% "spark-core" % "3.5.1", // Use the Spark version installed on your device
  "org.apache.spark" %% "spark-sql" % "3.5.1",
  "com.typesafe" % "config" % "1.4.3",
  "software.amazon.awssdk" % "s3" % "2.25.27",
  "org.scalatest" %% "scalatest" % "3.2.18" % Test,
  "org.mockito" %% "mockito-scala-scalatest" % "1.17.31" % Test,
  "org.scalatestplus" %% "scalatestplus-mockito" % "1.0.0-M2" % Test,
)

