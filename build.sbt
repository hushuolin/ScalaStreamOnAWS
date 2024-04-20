ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.7"

lazy val root = (project in file("."))
  .settings(
    name := "batch-data-pipeline"
  )

libraryDependencies ++= Seq(
  "com.softwaremill.sttp.client3" %% "core" % "3.9.5",
  "com.softwaremill.sttp.client3" %% "circe" % "3.9.5",
  "com.lihaoyi" %% "ujson" % "1.4.0",
  "org.apache.spark" %% "spark-core" % "3.5.1",
  "org.apache.spark" %% "spark-sql" % "3.5.1",
  "org.apache.spark" %% "spark-mllib" % "3.5.1",
  "com.typesafe" % "config" % "1.4.3",
  "software.amazon.awssdk" % "s3" % "2.25.27",
  "org.scalatest" %% "scalatest" % "3.2.18" % Test,
  "org.mockito" %% "mockito-scala-scalatest" % "1.17.31" % Test,
  "org.scalatestplus" %% "scalatestplus-mockito" % "1.0.0-M2" % Test,
  "org.deeplearning4j" % "deeplearning4j-core" % "1.0.0-M1.1",
  "org.deeplearning4j" % "deeplearning4j-nn" % "1.0.0-M1.1",
  "org.nd4j" % "nd4j-native-platform" % "1.0.0-M1.1",
  "org.elasticsearch.client" % "elasticsearch-rest-high-level-client" % "7.17.20",
  "org.elasticsearch" % "elasticsearch" % "8.13.2",
  "org.elasticsearch.client" % "elasticsearch-rest-client" % "8.13.2"
)

