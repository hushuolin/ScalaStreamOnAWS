package batchDataPipeline

import org.apache.spark.sql.SparkSession
import sttp.client3.HttpURLConnectionBackend
import AppConfig._

object Main extends App {
  val spark = SparkSession.builder()
    .appName(AppConfig.sparkAppName)
    .master(AppConfig.sparkMaster)
    .getOrCreate()

  try {
    val backend = HttpURLConnectionBackend()
    val startDate = AppConfig.startDate
    val endDate = AppConfig.endDate

    val result = ETL.extractComplaints(startDate, endDate, backend)

    // Process the result and print only the first 10 complaints
    result match {
      case Right(complaints) =>
        println(s"Extracted ${complaints.size} complaints. Displaying the first 10:")
        complaints.take(10).foreach(complaint => println(complaint.toString))
      case Left(error) =>
        println(s"Failed to extract data: $error")
    }
  } finally {
    spark.stop()
  }
}
