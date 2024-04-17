package batchDataPipeline

import org.apache.spark.sql.SparkSession
import sttp.client3.HttpURLConnectionBackend

object Main extends App {
  val spark = SparkSession.builder()
    .appName(AppConfig.sparkAppName)
    .master(AppConfig.sparkMaster)
    .getOrCreate()

  try {
    val backend = HttpURLConnectionBackend()
    ETL.extractData(spark, backend)
  } finally {
    spark.stop()
  }
}
