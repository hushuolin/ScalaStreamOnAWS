package utils

import org.apache.spark.sql.SparkSession
import sttp.client3._

object DataTypeChecker {
  val apiEndpoint = "https://www.consumerfinance.gov/data-research/consumer-complaints/search/api/v1/"
  val backend = HttpURLConnectionBackend()
  val frm = 0
  val size = 5 // Only fetch 5 records for type checking
  val start_date = "2023-09-01"
  val end_date = "2023-09-02"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Data Type Checker")
      .master("local[*]") // Use local in a non-clustered environment
      .getOrCreate()

    try {
      checkDataTypes()
    } finally {
      spark.stop()
    }
  }

  def checkDataTypes(): Unit = {
    val response = basicRequest
      .get(uri"$apiEndpoint?frm=$frm&size=$size&date_received_min=$start_date&date_received_max=$end_date&product=Credit%20card")
      .send(backend)

    response.body match {
      case Right(content) =>
        val data = ujson.read(content)
        val hits = data("hits")("hits").arr.map(_.obj("_source"))
        hits.take(5).foreach { source =>
          println("Data types in _source:")
          source.obj.foreach { case (key, value) =>
            val valueType = value match {
              case _: ujson.Str => "String"
              case _: ujson.Num => "Number"
              case _: ujson.Bool => "Boolean"
              case _: ujson.Arr => "Array"
              case _: ujson.Obj => "Object"
              case ujson.Null => "Null"
              case _ => "Unknown"
            }
            println(s"Key: $key, Type: $valueType")
          }
          println("----------------------")
        }

      case Left(error) =>
        println(s"API request failed: $error")
    }
  }
}
