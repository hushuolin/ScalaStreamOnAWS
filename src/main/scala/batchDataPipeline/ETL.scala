package batchDataPipeline

import batchDataPipeline.Complaint
import org.apache.spark.sql.{SparkSession, Dataset, Encoders}
import sttp.client3._
import scala.util.{Try, Success, Failure}
import AppConfig._
import ujson.{Obj, Value}

object ETL {

  // Method to fetch data from the API
  def fetchApiData(frm: Int, size: Int, backend: SttpBackend[Identity, Any]): Either[String, List[Complaint]] = {
    Try(basicRequest
      .get(uri"$apiEndpoint?frm=$frm&size=$size&date_received_min=$startDate&date_received_max=$endDate&product=$productName")
      .send(backend)) match {
      case Success(response) =>
        response.body match {
          case Right(content) => parseData(content)
          case Left(error) => Left(s"API request failed: $error")
        }
      case Failure(exception) => Left(s"Network request failed: ${exception.getMessage}")
    }
  }

  // Parse JSON data into Complaint objects
  def parseData(content: String): Either[String, List[Complaint]] = {
    Try {
      val data = ujson.read(content)
      val hits = data("hits")("hits").arr
      val complaints = hits.map(hit => parseComplaint(hit("_source").obj))
      complaints.toList
    } match {
      case Success(result) => Right(result)
      case Failure(exception) => Left(s"Failed to parse JSON: ${exception.getMessage}")
    }
  }

  // Convert JSON objects to Complaint case class instances
  def parseComplaint(json: Obj): Complaint = Complaint(
    company = json.obj.get("company").flatMap(_.strOpt),
    company_public_response = json.obj.get("company_public_response").flatMap(_.strOpt),
    company_response = json.obj.get("company_response").flatMap(_.strOpt),
    complaint_id = json.obj.get("complaint_id").flatMap(_.strOpt),
    complaint_what_happened = json.obj.get("complaint_what_happened").flatMap(_.strOpt),
    consumer_consent_provided = json.obj.get("consumer_consent_provided").flatMap(_.strOpt),
    consumer_disputed = json.obj.get("consumer_disputed").flatMap(_.strOpt),
    date_received = json.obj.get("date_received").flatMap(_.strOpt),
    date_sent_to_company = json.obj.get("date_sent_to_company").flatMap(_.strOpt),
    has_narrative = json.obj.get("has_narrative").flatMap(_.boolOpt),
    issue = json.obj.get("issue").flatMap(_.strOpt),
    product = json.obj.get("product").flatMap(_.strOpt),
    state = json.obj.get("state").flatMap(_.strOpt),
    sub_issue = json.obj.get("sub_issue").flatMap(_.strOpt),
    sub_product = json.obj.get("sub_product").flatMap(_.strOpt),
    submitted_via = json.obj.get("submitted_via").flatMap(_.strOpt),
    tags = json.obj.get("tags").flatMap(_.strOpt),
    timely = json.obj.get("timely").flatMap(_.strOpt),
    zip_code = json.obj.get("zip_code").flatMap(_.strOpt)
  )

  // Extract, transform, and load data
  def extractData(spark: SparkSession, backend: SttpBackend[Identity, Any]): Unit = {
    var hasMoreData = true
    var frm = 0

    while (hasMoreData) {
      fetchApiData(frm, size, backend) match {
        case Right(complaints) =>
          val complaintsDS = spark.createDataset(complaints)(Encoders.product[Complaint])
          val transformed = transformData(complaintsDS)
          loadData(transformed, frm)
          frm += complaints.size
          hasMoreData = complaints.size == size
        case Left(error) =>
          println(error)
          hasMoreData = false
      }
    }
  }

  // Simple transformation function (as an example)
  def transformData(dataset: Dataset[Complaint]): Dataset[Complaint] = {
    // To be implemented to do more transformation
    dataset.filter(_.consumer_disputed.isDefined)
  }

  // Load data to Parquet and then upload to S3
  def loadData(dataset: Dataset[Complaint], frm: Int): Unit = {
    val tempPath = s"/tmp/complaints_data_$frm.parquet"
    dataset.coalesce(1).write.mode("overwrite").parquet(tempPath)
    val dir = new java.io.File(tempPath)
    val actualFile = dir.listFiles().find(_.getName.endsWith(".parquet")).get
    S3Uploader.uploadFileToS3("credit-card-complaints-raw-data", s"complaints/data_$frm.parquet", actualFile)
  }
}
