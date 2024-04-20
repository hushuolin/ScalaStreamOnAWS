package batchDataPipeline

import org.apache.spark.sql.SparkSession
import sttp.client3.HttpURLConnectionBackend
import AppConfig._

object Main extends App {
  val spark = SparkSession.builder()
    .appName(AppConfig.sparkAppName)
    .master(AppConfig.sparkMaster)
    .getOrCreate()

  import spark.implicits._

  val backend = HttpURLConnectionBackend()
  val startDate = AppConfig.startDate
  val endDate = AppConfig.endDate

  val result = ETL.extractComplaints(startDate, endDate, backend, spark)

  // Process the result and print only the first 10 complaints
  result match {
    case Right(complaintsDataset) =>
      println(s"Extracted ${complaintsDataset.count()} complaints. Displaying the first 10:")
      complaintsDataset.show(10, truncate = false)
    case Left(error) =>
      println(s"Failed to extract data: $error")
  }
  spark.stop()
}
