package batchDataPipeline

import com.typesafe.config.ConfigFactory

object AppConfig {
  private val config = ConfigFactory.load()

  val apiEndpoint = config.getString("api.endpoint")
  val productName = config.getString("api.productName")
  val startDate = config.getString("api.startDate")
  val endDate = config.getString("api.endDate")
  val pageSize = config.getInt("api.pageSize")
  val sparkMaster = config.getString("spark.master")
  val sparkAppName = config.getString("spark.appName")
}
